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

package io.grpc.autosharding;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.SynchronizationContext.ScheduledHandle;
import io.grpc.internal.BackoffPolicy;
import io.grpc.internal.ExponentialBackoffPolicy;
import io.grpc.internal.GrpcUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * The {@code autosharding_experimental} load balancing policy.
 *
 * <p>This policy shards RPCs across endpoints by an application-defined key carried in a request
 * header. The mapping from key ranges to endpoints comes from an external sharding service, which
 * an {@link AutoshardingClient} streams assignments from. See gRFC A119.
 *
 * <h3>Moving parts</h3>
 *
 * <ul>
 *   <li>{@link EndpointMap} owns one lazily-created {@code pick_first} child per resolved
 *       endpoint and assigns each a dense index.
 *   <li>{@link AutoshardingClient} produces validated {@link Assignment}s, which name endpoints
 *       by hostname.
 *   <li>{@link SliceMap} is the join of the two: the assignment's key ranges with hostnames
 *       translated into endpoint indices. It is rebuilt whenever either input changes.
 *   <li>{@link AutoShardingPicker} performs the per-RPC lookup against a {@link SliceMap} and a
 *       snapshot of endpoint states. It is rebuilt on every child state update too, reusing the
 *       existing {@link SliceMap} because the endpoint indices did not move.
 * </ul>
 *
 * <h3>Startup</h3>
 *
 * <p>Creating an {@link AutoshardingClient} starts the initial assignment timer. Until the first
 * assignment arrives or that timer fires, RPCs are queued. Once the timer fires without an
 * assignment, RPCs either spread across every resolved endpoint or fail outright, depending on
 * {@code enable_fallback}. A new client is created whenever the channel factory key or the
 * sharding target changes; an assignment carried over from the previous client keeps being used
 * while the timer runs, so a change of sharding service does not interrupt traffic.
 *
 * <h3>Threading model</h3>
 *
 * <p>All state lives on the {@link SynchronizationContext}.
 */
final class AutoShardingLoadBalancer extends LoadBalancer {
  private static final Logger logger =
      Logger.getLogger(AutoShardingLoadBalancer.class.getName());

  /**
   * Published while waiting for the first assignment. The delay type is consumed by the
   * name-resolution delay tracking in gRFC A121.
   */
  private static final SubchannelPicker ASSIGNMENT_PENDING_PICKER =
      new FixedResultPicker(
          PickResult.withNoResult(
              "autosharding_assignment_pending", "Waiting for initial sharding assignment"));

  private final Helper helper;
  private final SynchronizationContext syncContext;
  private final ScheduledExecutorService timeService;
  private final LoadBalancerProvider childProvider;
  private final BackoffPolicy.Provider backoffPolicyProvider;
  private final Supplier<Stopwatch> stopwatchSupplier;

  /** Identifies this client to the sharding service; stable across stream restarts. */
  private final String clientUuid;

  private final EndpointMap endpointMap;

  @Nullable private AutoShardingLoadBalancerConfig config;
  @Nullable private Metadata.Key<byte[]> keyHeader;

  /** The factory last seen in the resolver attributes. */
  @Nullable private ChannelFactory channelFactory;

  /** Channel borrowed from {@link #channelFactory}; must be given back when we are done. */
  @Nullable private Channel shardingChannel;

  /**
   * The {@code autosharding_target} the current {@link #client} was created with, after {@code %s}
   * substitution. Tracked separately from the config because the substitution depends on the
   * resolved endpoints, so the target can change while the config does not.
   */
  @Nullable private String shardingTarget;

  @Nullable private AutoshardingClient client;

  /** Most recent assignment accepted from the sharding service, retained across reconnects. */
  @Nullable private Assignment assignment;

  /** Join of {@link #assignment} and {@link #endpointMap}; null only before the first update. */
  @Nullable private SliceMap sliceMap;

  @Nullable private ScheduledHandle initialAssignmentTimer;

  /**
   * True from the moment an {@link AutoshardingClient} is created until either an assignment
   * arrives from it or {@link #initialAssignmentTimer} fires. Combined with a null
   * {@link #assignment} it means RPCs must be queued rather than failed.
   */
  private boolean awaitingInitialAssignment;

  private boolean shutdown;

  AutoShardingLoadBalancer(Helper helper) {
    this(
        helper,
        LoadBalancerRegistry.getDefaultRegistry().getProvider("pick_first"),
        new ExponentialBackoffPolicy.Provider(),
        GrpcUtil.STOPWATCH_SUPPLIER,
        UUID.randomUUID().toString());
  }

  /**
   * Constructs a load balancer with injectable collaborators.
   *
   * @param childProvider provides the per-endpoint child load balancer, {@code pick_first} in
   *     production. {@link EndpointMap} takes care of deferring its instantiation, so this must
   *     not be wrapped in a {@link io.grpc.util.LazyLoadBalancer.Factory} by the caller
   */
  @VisibleForTesting
  AutoShardingLoadBalancer(
      Helper helper,
      LoadBalancerProvider childProvider,
      BackoffPolicy.Provider backoffPolicyProvider,
      Supplier<Stopwatch> stopwatchSupplier,
      String clientUuid) {
    this.helper = checkNotNull(helper, "helper");
    this.syncContext = helper.getSynchronizationContext();
    this.timeService = helper.getScheduledExecutorService();
    this.childProvider = checkNotNull(childProvider, "childProvider");
    this.backoffPolicyProvider = checkNotNull(backoffPolicyProvider, "backoffPolicyProvider");
    this.stopwatchSupplier = checkNotNull(stopwatchSupplier, "stopwatchSupplier");
    this.clientUuid = checkNotNull(clientUuid, "clientUuid");
    this.endpointMap = new EndpointMap(helper, this.childProvider, this::onChildStateUpdate);
  }

  @Override
  public Status acceptResolvedAddresses(ResolvedAddresses resolvedAddresses) {
    if (shutdown) {
      return Status.OK;
    }
    Object rawConfig = resolvedAddresses.getLoadBalancingPolicyConfig();
    if (!(rawConfig instanceof AutoShardingLoadBalancerConfig)) {
      return failPermanently("autosharding: missing or malformed load balancing configuration");
    }
    AutoShardingLoadBalancerConfig newConfig = (AutoShardingLoadBalancerConfig) rawConfig;

    ChannelFactory factory =
        resolvedAddresses.getAttributes().get(AutoShardingAttributes.ATTR_CHANNEL_FACTORY);
    if (factory == null) {
      return failPermanently("autosharding: no channel factory supplied to the LB policy");
    }

    List<EquivalentAddressGroup> endpoints = resolvedAddresses.getAddresses();
    if (endpoints.isEmpty()) {
      // Tear the children down so that in-flight picks stop resolving to endpoints the resolver
      // has retracted. The assignment is kept: it stays valid if the endpoints come back.
      endpointMap.updateEndpoints(ImmutableList.of(), resolvedAddresses.getAttributes());
      config = newConfig;
      return failPermanently("autosharding: name resolver returned no endpoints");
    }

    Channel previousChannel = shardingChannel;
    Status channelStatus = updateShardingServiceChannel(factory, newConfig);
    if (!channelStatus.isOk()) {
      return channelStatus;
    }

    if (config == null || !config.keyHeaderName.equals(newConfig.keyHeaderName)) {
      keyHeader = AutoShardingPicker.createKeyHeader(newConfig.keyHeaderName);
    }
    config = newConfig;

    endpointMap.updateEndpoints(endpoints, resolvedAddresses.getAttributes());

    // The target is resolved against the endpoints, so it can change even when the config did not.
    maybeRecreateClient(
        shardingChannel != previousChannel,
        resolveTarget(newConfig, endpoints),
        newConfig.initialAssignmentTimeoutNanos);

    rebuildSliceMapAndPublish();
    return Status.OK;
  }

  @Override
  public void handleNameResolutionError(Status error) {
    if (shutdown) {
      return;
    }
    // Endpoints we already have remain usable; only report the failure if we have nothing.
    if (endpointMap.size() > 0) {
      logger.log(Level.FINE, "Ignoring name resolution error, endpoints still known: {0}", error);
      return;
    }
    helper.updateBalancingState(
        TRANSIENT_FAILURE,
        new FixedResultPicker(
            PickResult.withError(
                error.getCode() == Status.Code.OK
                    ? Status.UNAVAILABLE.withDescription("autosharding: name resolution failed")
                    : error)));
  }

  @Override
  public void requestConnection() {
    endpointMap.maybeWakeUpIdleEndpoint();
  }

  @Override
  public void shutdown() {
    if (shutdown) {
      return;
    }
    shutdown = true;
    cancelInitialAssignmentTimer();
    if (client != null) {
      client.shutdown();
      client = null;
    }
    shardingTarget = null;
    if (shardingChannel != null) {
      channelFactory.releaseChannel(shardingChannel);
      shardingChannel = null;
    }

    endpointMap.shutdown();
  }

  /**
   * Creates a channel to the sharding service if this is the first configuration update, or if
   * the {@code channel_factory_key} or the factory itself changed. Leaves {@link #shardingChannel}
   * untouched when nothing changed, which is how the caller detects that no new channel was
   * needed.
   */
  private Status updateShardingServiceChannel(
      ChannelFactory factory, AutoShardingLoadBalancerConfig newConfig) {
    boolean keyChanged =
        config == null || !config.channelFactoryKey.equals(newConfig.channelFactoryKey);
    if (shardingChannel != null && factory == channelFactory && !keyChanged) {
      return Status.OK;
    }

    Channel newChannel;
    try {
      newChannel = factory.createChannel(newConfig.channelFactoryKey);
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Failed to create a channel to the sharding service", e);
      return failPermanently(
          "autosharding: channel factory rejected key '"
              + newConfig.channelFactoryKey
              + "': "
              + e.getMessage());
    }

    // Release through the factory that produced it, which is not necessarily the new one.
    if (shardingChannel != null) {
      channelFactory.releaseChannel(shardingChannel);
    }
    shardingChannel = newChannel;
    channelFactory = factory;
    return Status.OK;
  }

  /**
   * Replaces the {@link AutoshardingClient} when there is none yet, or when the channel to the
   * sharding service or the resolved target changed.
   *
   * <p>gRFC A119 calls for a new client rather than an in-place update because the client's
   * accepted-generation watermark is only meaningful against the server and the resource it was
   * learned from; carrying it over could make a different server withhold assignments
   * indefinitely.
   *
   * <p>Creating a client also restarts the initial assignment timer, since the new one has to
   * start from scratch. Any assignment carried over from the previous client keeps being served
   * while that timer runs.
   */
  private void maybeRecreateClient(boolean channelChanged, String newTarget, long timeoutNanos) {
    if (client != null && !channelChanged && newTarget.equals(shardingTarget)) {
      return;
    }
    if (client != null) {
      client.shutdown();
    }
    shardingTarget = newTarget;
    client =
        new AutoshardingClient(
            clientUuid,
            syncContext,
            timeService,
            backoffPolicyProvider,
            stopwatchSupplier,
            shardingChannel,
            newTarget,
            new AssignmentWatcherImpl());
    // Armed before the stream opens so that an assignment delivered right away cancels it.
    startInitialAssignmentTimer(timeoutNanos);
    client.start();
  }

  /**
   * Substitutes the optional {@code %s} token in the configured target with the locality of the
   * resolved endpoints, or with the empty string when no locality is available.
   *
   * <p>The locality is read from {@link EquivalentAddressGroup#ATTR_LOCALITY_NAME}, which is a
   * plain {@code io.grpc} attribute rather than an xDS-specific one. That keeps a single code path
   * for both deployments: under xDS the attribute is populated by the cluster resolver, and
   * without xDS gRFC A119 makes it the user's responsibility to have their name resolver populate
   * it if their target contains a {@code %s} token. All endpoints handed to one instance of this
   * policy belong to the same locality, so the first one is representative.
   */
  private static String resolveTarget(
      AutoShardingLoadBalancerConfig config, List<EquivalentAddressGroup> endpoints) {
    if (!config.autoshardingTarget.contains("%s")) {
      return config.autoshardingTarget;
    }
    String locality =
        endpoints.get(0).getAttributes().get(EquivalentAddressGroup.ATTR_LOCALITY_NAME);
    return config.autoshardingTarget.replace("%s", locality == null ? "" : locality);
  }

  private void startInitialAssignmentTimer(long timeoutNanos) {
    cancelInitialAssignmentTimer();
    awaitingInitialAssignment = true;
    initialAssignmentTimer =
        syncContext.schedule(
            this::onInitialAssignmentTimeout, timeoutNanos, TimeUnit.NANOSECONDS, timeService);
  }

  private void cancelInitialAssignmentTimer() {
    if (initialAssignmentTimer != null) {
      initialAssignmentTimer.cancel();
      initialAssignmentTimer = null;
    }
    awaitingInitialAssignment = false;
  }

  /**
   * Gives up on hearing from the sharding service. Any queued RPCs are retried against whatever
   * the current configuration allows: the full endpoint set if fallback is enabled, otherwise a
   * failing picker.
   */
  private void onInitialAssignmentTimeout() {
    logger.log(
        Level.WARNING,
        "Timed out waiting for the initial assignment from the sharding service; "
            + "proceeding {0} fallback",
        config != null && config.enableFallback ? "with" : "without");
    awaitingInitialAssignment = false;
    initialAssignmentTimer = null;
    rebuildSliceMapAndPublish();
  }

  /**
   * Receives assignments from the current {@link AutoshardingClient}. Both callbacks arrive on
   * the synchronization context.
   *
   * <p>A client that has been replaced cannot deliver anything, because {@link
   * AutoshardingClient#shutdown()} closes its stream, so there is no need to check which client a
   * callback came from.
   */
  private final class AssignmentWatcherImpl implements AutoshardingClient.AssignmentWatcher {
    @Override
    public void onAssignment(Assignment newAssignment) {
      if (shutdown) {
        return;
      }
      assignment = newAssignment;
      cancelInitialAssignmentTimer();
      rebuildSliceMapAndPublish();
    }

    @Override
    public void onError(Status error) {
      if (shutdown) {
        return;
      }
      if (assignment != null) {
        // An assignment we can still use is in hand; the service only failed to replace it.
        // Mirrors handleNameResolutionError: stale data beats no data.
        logger.log(Level.WARNING, "Keeping the current sharding assignment: {0}", error);
        return;
      }
      logger.log(
          Level.WARNING,
          "The sharding service sent no usable assignment; proceeding {0} fallback: {1}",
          new Object[] {config != null && config.enableFallback ? "with" : "without", error});
      // Stop queuing RPCs: there is nothing left to wait for on this generation.
      cancelInitialAssignmentTimer();
      rebuildSliceMapAndPublish();
    }
  }

  /**
   * Called by {@link EndpointMap} when a child reports a new state or picker. The endpoint set
   * and the indices into it are unchanged, so the existing {@link SliceMap} still applies and
   * only the picker needs rebuilding.
   */
  private void onChildStateUpdate() {
    if (shutdown) {
      return;
    }
    publishPicker();
  }

  private void rebuildSliceMapAndPublish() {
    sliceMap = buildSliceMap();
    publishPicker();
  }

  /**
   * Joins the current assignment with the current endpoints, translating the assignment's
   * hostnames into endpoint indices. Hostnames the resolver has not given us are dropped, which
   * can leave a slice with no endpoints; the picker treats such a slice as being in fallback.
   *
   * <p>Before any assignment has been received the result has no slices, so every lookup misses
   * and the picker routes through the fallback pool or fails, according to configuration.
   */
  private SliceMap buildSliceMap() {
    int endpointCount = endpointMap.size();
    List<Integer> fallbackPool = new ArrayList<>(endpointCount);
    for (int i = 0; i < endpointCount; i++) {
      fallbackPool.add(i);
    }
    if (assignment == null) {
      return new SliceMap(ImmutableList.of(), fallbackPool, 0);
    }

    ImmutableList<String> endpointNames = assignment.getEndpointNames();
    List<SliceMap.SliceEntry> entries = new ArrayList<>(assignment.getSlices().size());
    for (Assignment.Slice slice : assignment.getSlices()) {
      List<Integer> indices = new ArrayList<>(slice.getEndpoints().size());
      for (int nameIndex : slice.getEndpoints()) {
        int endpointIndex = endpointMap.indexOf(endpointNames.get(nameIndex));
        if (endpointIndex != -1) {
          indices.add(endpointIndex);
        }
      }
      entries.add(new SliceMap.SliceEntry(slice.getStartKey(), indices));
    }
    return new SliceMap(entries, fallbackPool, assignment.getGeneration());
  }

  private void publishPicker() {
    if (shutdown || config == null) {
      return;
    }
    if (endpointMap.size() == 0) {
      // acceptResolvedAddresses already reported TRANSIENT_FAILURE for this case.
      return;
    }
    if (awaitingInitialAssignment && assignment == null) {
      helper.updateBalancingState(CONNECTING, ASSIGNMENT_PENDING_PICKER);
      return;
    }

    ConnectivityState state = endpointMap.aggregateConnectivityState();
    helper.updateBalancingState(
        state,
        new AutoShardingPicker(
            sliceMap, endpointMap.toPickerEndpoints(), config.enableFallback, keyHeader));

    // Nothing else will drive progress: this policy only connects in response to picks, so a
    // CONNECTING or TRANSIENT_FAILURE aggregate could otherwise stick with no attempt in flight.
    // The woken endpoint reports CONNECTING synchronously, re-entering publishPicker() once to
    // publish the fresher picker; that pass finds an endpoint CONNECTING and wakes no one else.
    if (state == CONNECTING || state == TRANSIENT_FAILURE) {
      endpointMap.maybeWakeUpIdleEndpoint();
    }
  }

  @VisibleForTesting
  EndpointMap getEndpointMap() {
    return endpointMap;
  }

  /**
   * Reports TRANSIENT_FAILURE with a picker that fails every RPC, and returns the same error for
   * {@link #acceptResolvedAddresses} to hand back to the channel.
   */
  private Status failPermanently(String description) {
    Status error = Status.UNAVAILABLE.withDescription(description);
    helper.updateBalancingState(TRANSIENT_FAILURE, new FixedResultPicker(PickResult.withError(
        error)));
    return error;
  }
}
