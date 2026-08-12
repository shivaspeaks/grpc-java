/*
 * Copyright 2026 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.slicer;

import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.cloud.autosharding.v1main.PerSliceEndpointState;
import com.google.cloud.autosharding.v1main.SliceAssignment;
import io.grpc.Attributes;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.slicer.SliceMap.SliceEntry;
import io.grpc.slicer.SlicerLoadBalancerProvider.SlicerConfig;
import io.grpc.util.ForwardingLoadBalancerHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SlicerLoadBalancer extends LoadBalancer {
  private static final Logger logger = Logger.getLogger(SlicerLoadBalancer.class.getName());

  public interface ChannelFactory {
    final class ChannelHolder implements AutoCloseable {
      private final Channel channel;
      private final Runnable releaseCallback;

      public ChannelHolder(Channel channel, Runnable releaseCallback) {
        this.channel = channel;
        this.releaseCallback = releaseCallback;
      }

      public Channel getChannel() {
        return channel;
      }

      @Override
      public void close() {
        if (releaseCallback != null) {
          releaseCallback.run();
        }
      }
    }

    ChannelHolder createChannel(String target);
  }

  public static final Attributes.Key<ChannelFactory> CHANNEL_FACTORY_KEY =
      Attributes.Key.create("slicer-channel-factory");

  public static final Attributes.Key<String> LOCALITY_KEY =
      Attributes.Key.create("io.grpc.slicer.SlicerLoadBalancer.LOCALITY");

  private final Helper helper;
  private final SynchronizationContext syncContext;
  private final LoadBalancerProvider pickFirstProvider;

  private ChannelFactory.ChannelHolder shardingChannelHolder;
  private Channel shardingChannel;
  private ShardingClient shardingClient;
  private String currentChannelFactoryKey;
  private String currentSlicingTarget;

  private boolean fallbackEnabled = false;
  private String sliceKeyHeaderName = "";
  private long initialAssignmentTimeoutNanos = TimeUnit.SECONDS.toNanos(60);

  // Endpoint map: hostname -> EndpointHolder
  private final Map<String, EndpointHolder> endpointMap = new LinkedHashMap<>();

  private SliceMap currentSliceMap;
  private List<SliceAssignment> latestSliceAssignments;
  private List<com.google.cloud.autosharding.v1main.EndpointState> latestEndpointsProto;
  private long latestGeneration = 0;

  private SynchronizationContext.ScheduledHandle fallbackTimer;
  private boolean fallbackTimerFired = false;

  public SlicerLoadBalancer(Helper helper) {
    this.helper = helper;
    this.syncContext = helper.getSynchronizationContext();
    this.pickFirstProvider =
        LoadBalancerRegistry.getDefaultRegistry().getProvider("pick_first");
  }

  @Override
  public Status acceptResolvedAddresses(ResolvedAddresses resolvedAddresses) {
    SlicerConfig config = (SlicerConfig) resolvedAddresses.getLoadBalancingPolicyConfig();
    if (config == null) {
      return Status.INVALID_ARGUMENT.withDescription("Missing SlicerConfig");
    }

    this.fallbackEnabled = config.enableFallback;
    this.sliceKeyHeaderName = config.sliceKeyHeaderName;
    if (config.initialAssignmentTimeoutNanos != null) {
      this.initialAssignmentTimeoutNanos = config.initialAssignmentTimeoutNanos;
    }

    // Connect to sharding service if channelFactoryKey or slicingTarget changed
    if (shardingClient == null
        || !config.channelFactoryKey.equals(currentChannelFactoryKey)
        || !config.slicingTarget.equals(currentSlicingTarget)) {
      initShardingClient(
          resolvedAddresses.getAttributes(),
          config.channelFactoryKey,
          config.slicingTarget);
    }

    // Process endpoints from Name Resolver
    List<EquivalentAddressGroup> addresses = resolvedAddresses.getAddresses();
    if (addresses.isEmpty()) {
      for (EndpointHolder holder : endpointMap.values()) {
        holder.shutdown();
      }
      endpointMap.clear();
      currentSliceMap = null;
      helper.updateBalancingState(
          TRANSIENT_FAILURE,
          new FixedResultPicker(PickResult.withError(
              Status.UNAVAILABLE.withDescription(
                  "NameResolver returned empty list of endpoints"))));
      return Status.OK;
    }

    Set<String> newHostnames = new HashSet<>();
    for (int i = 0; i < addresses.size(); i++) {
      EquivalentAddressGroup eag = addresses.get(i);
      String hostname = getHostname(eag);
      newHostnames.add(hostname);

      EndpointHolder holder = endpointMap.get(hostname);
      if (holder == null) {
        holder = new EndpointHolder(i);
        endpointMap.put(hostname, holder);
      } else {
        holder.index = i;
      }
      holder.updateAddress(eag, resolvedAddresses.getAttributes());
    }

    // Remove obsolete endpoints
    List<String> toRemove = new ArrayList<>();
    for (String oldHost : endpointMap.keySet()) {
      if (!newHostnames.contains(oldHost)) {
        toRemove.add(oldHost);
      }
    }
    for (String host : toRemove) {
      EndpointHolder removed = endpointMap.remove(host);
      if (removed != null) {
        removed.shutdown();
      }
    }

    // Re-index remaining endpoints so indices form contiguous 0..N-1
    int nextIdx = 0;
    for (EndpointHolder holder : endpointMap.values()) {
      holder.index = nextIdx++;
    }

    // Build slice map if assignment received or if timer has fired
    if (latestSliceAssignments != null || fallbackTimerFired) {
      rebuildSliceMap();
    }
    updateAggregatedState();
    return Status.OK;
  }

  private void closeShardingChannel() {
    if (shardingClient != null) {
      shardingClient.stop();
      shardingClient = null;
    }
    if (shardingChannelHolder != null) {
      try {
        shardingChannelHolder.close();
      } catch (Exception e) {
        logger.log(Level.WARNING, "Error closing sharding channel", e);
      }
      shardingChannelHolder = null;
    }
    shardingChannel = null;
  }

  private void initShardingClient(
      Attributes attributes, String channelFactoryKey, String slicingTarget) {
    closeShardingChannel();
    if (fallbackTimer != null) {
      fallbackTimer.cancel();
      fallbackTimer = null;
    }

    currentChannelFactoryKey = channelFactoryKey;
    currentSlicingTarget = slicingTarget;

    ChannelFactory factory = attributes.get(CHANNEL_FACTORY_KEY);
    if (factory == null) {
      logger.log(Level.WARNING, "No ChannelFactory attribute provided to SlicerLoadBalancer");
      return;
    }

    String locality = attributes.get(LOCALITY_KEY);
    if (locality == null) {
      locality = "";
    }
    String actualTarget = slicingTarget.replace("%s", locality);

    shardingChannelHolder = factory.createChannel(channelFactoryKey);
    if (shardingChannelHolder == null || shardingChannelHolder.getChannel() == null) {
      logger.log(
          Level.WARNING,
          "ChannelFactory returned null channel for target {0}",
          channelFactoryKey);
      return;
    }
    shardingChannel = shardingChannelHolder.getChannel();
    shardingClient =
        new ShardingClient(
            shardingChannel,
            actualTarget,
            latestGeneration,
            syncContext,
            helper.getScheduledExecutorService(),
            new ShardingCallback());
    shardingClient.start();

    // Start fallback-at-startup timer
    fallbackTimerFired = false;
    fallbackTimer = syncContext.schedule(
        this::onFallbackTimerExpired,
        initialAssignmentTimeoutNanos,
        TimeUnit.NANOSECONDS,
        helper.getScheduledExecutorService());
  }

  private void onFallbackTimerExpired() {
    fallbackTimerFired = true;
    fallbackTimer = null;
    logger.log(Level.WARNING, "Initial assignment timeout expired. Entering fallback mode.");
    latestSliceAssignments = null;
    latestEndpointsProto = null;
    latestGeneration = 0;
    rebuildSliceMap();
    updateAggregatedState();
  }

  private void rebuildSliceMap() {
    if (endpointMap.isEmpty()) {
      currentSliceMap = null;
      return;
    }

    // Populate fallback_pool deterministically sorted by endpoint index
    List<Integer> fallbackPool = new ArrayList<>();
    for (int i = 0; i < endpointMap.size(); i++) {
      fallbackPool.add(i);
    }

    // If no assignment received yet (startup case), return early with empty slices
    if (latestSliceAssignments == null || latestEndpointsProto == null) {
      currentSliceMap = new SliceMap(Collections.emptyList(), fallbackPool, 0);
      return;
    }

    List<SliceEntry> sliceEntries = new ArrayList<>();
    for (SliceAssignment protoSlice : latestSliceAssignments) {
      List<Integer> sliceEndpoints = new ArrayList<>();
      for (PerSliceEndpointState perSliceEp : protoSlice.getEndpointsList()) {
        int protoEpIdx = perSliceEp.getEndpointIndex();
        if (protoEpIdx >= 0 && protoEpIdx < latestEndpointsProto.size()) {
          String hostname = latestEndpointsProto.get(protoEpIdx).getEndpoint();
          EndpointHolder holder = endpointMap.get(hostname);
          if (holder != null) {
            sliceEndpoints.add(holder.index);
          }
        }
      }
      sliceEntries.add(
          new SliceEntry(
              protoSlice.getSlice().getStartKeyInclusive().toByteArray(), sliceEndpoints));
    }

    currentSliceMap = new SliceMap(sliceEntries, fallbackPool, latestGeneration);
  }

  private void updateAggregatedState() {
    if (endpointMap.isEmpty()) {
      helper.updateBalancingState(
          TRANSIENT_FAILURE,
          new FixedResultPicker(PickResult.withError(
              Status.UNAVAILABLE.withDescription(
                  "NameResolver returned empty list of endpoints"))));
      return;
    }

    int readyCount = 0;
    int tfCount = 0;
    int connectingCount = 0;
    int idleCount = 0;

    EndpointHolder firstIdle = null;

    for (EndpointHolder holder : endpointMap.values()) {
      ConnectivityState state = holder.state;
      if (state == READY) {
        readyCount++;
      } else if (state == TRANSIENT_FAILURE) {
        tfCount++;
      } else if (state == CONNECTING) {
        connectingCount++;
      } else if (state == IDLE) {
        idleCount++;
        if (firstIdle == null) {
          firstIdle = holder;
        }
      }
    }

    ConnectivityState aggregated;
    int total = endpointMap.size();

    if (readyCount > 0) {
      aggregated = READY;
    } else if (tfCount >= 2) {
      aggregated = TRANSIENT_FAILURE;
    } else if (connectingCount > 0) {
      aggregated = CONNECTING;
    } else if (tfCount == 1 && total > 1) {
      aggregated = CONNECTING;
    } else if (idleCount > 0) {
      aggregated = IDLE;
    } else {
      aggregated = TRANSIENT_FAILURE;
    }

    // gRFC A119 heuristic: ensure at least one IDLE endpoint starts connecting
    // if aggregated state is CONNECTING or TRANSIENT_FAILURE and none are CONNECTING
    if ((aggregated == CONNECTING || aggregated == TRANSIENT_FAILURE)
        && connectingCount == 0 && firstIdle != null) {
      firstIdle.requestConnection();
    }

    SubchannelPicker picker;
    if (currentSliceMap != null) {
      List<PickerEndpoint> pickerEndpoints =
          new ArrayList<>(Collections.nCopies(endpointMap.size(), null));
      for (EndpointHolder holder : endpointMap.values()) {
        pickerEndpoints.set(
            holder.index,
            new PickerEndpoint(
                holder.state,
                holder.picker,
                () -> syncContext.execute(holder::requestConnection)));
      }
      picker = new SlicerPicker(
          currentSliceMap, pickerEndpoints, fallbackEnabled, sliceKeyHeaderName);
    } else {
      picker = new FixedResultPicker(
          PickResult.withNoResult(
              "autosharding_assignment_pending", "Waiting for initial sharding assignment"));
    }

    helper.updateBalancingState(aggregated, picker);
  }

  @Override
  public void handleNameResolutionError(Status error) {
    helper.updateBalancingState(
        TRANSIENT_FAILURE,
        new FixedResultPicker(PickResult.withError(error)));
  }

  @Override
  public void shutdown() {
    closeShardingChannel();
    if (fallbackTimer != null) {
      fallbackTimer.cancel();
      fallbackTimer = null;
    }
    for (EndpointHolder holder : endpointMap.values()) {
      holder.shutdown();
    }
    endpointMap.clear();
  }

  private static String getHostname(EquivalentAddressGroup eag) {
    String hostname = eag.getAttributes().get(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE);
    if (hostname != null && !hostname.isEmpty()) {
      return hostname;
    }
    return eag.getAddresses().get(0).toString();
  }

  private final class ShardingCallback implements ShardingClient.Callback {
    @Override
    public void onAssignmentReceived(
        List<SliceAssignment> sliceAssignments,
        List<com.google.cloud.autosharding.v1main.EndpointState> endpoints,
        long generation) {
      if (fallbackTimer != null) {
        fallbackTimer.cancel();
        fallbackTimer = null;
      }
      latestSliceAssignments = sliceAssignments;
      latestEndpointsProto = endpoints;
      latestGeneration = generation;
      rebuildSliceMap();
      updateAggregatedState();
    }

    @Override
    public void onError(Throwable t) {
      logger.log(Level.WARNING, "ShardingClient stream error", t);
      // Keep using existing slice map if we have one.
      // If we don't have one and timer already expired, rebuild fallback.
      if (currentSliceMap == null && fallbackTimerFired) {
        rebuildSliceMap();
        updateAggregatedState();
      }
    }
  }

  private final class EndpointHolder {
    int index;
    final LazyChildLoadBalancer childLb;
    ConnectivityState state = IDLE;
    SubchannelPicker picker = new FixedResultPicker(PickResult.withNoResult());

    EndpointHolder(int index) {
      this.index = index;
      this.childLb = new LazyChildLoadBalancer(new ChildHelper(), pickFirstProvider);
    }

    void updateAddress(EquivalentAddressGroup eag, Attributes attributes) {
      ResolvedAddresses childAddresses = ResolvedAddresses.newBuilder()
          .setAddresses(Collections.singletonList(eag))
          .setAttributes(attributes)
          .build();
      childLb.acceptResolvedAddresses(childAddresses);
    }

    void requestConnection() {
      childLb.requestConnection();
    }

    void shutdown() {
      childLb.shutdown();
    }

    private final class ChildHelper extends ForwardingLoadBalancerHelper {
      @Override
      protected Helper delegate() {
        return helper;
      }

      @Override
      public void updateBalancingState(ConnectivityState newState, SubchannelPicker newPicker) {
        state = newState;
        picker = newPicker;
        updateAggregatedState();
      }
    }
  }
}
