package io.grpc.slicer;

import com.google.protobuf.ByteString;
import io.grpc.ConnectivityState;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.SubchannelPicker;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

final class SlicerPicker extends SubchannelPicker {
  private final SliceMap sliceMap;
  private final List<PickerEndpoint> endpoints;
  private final boolean[] sliceInFallback;
  private final boolean fallbackEnabled;
  private final Metadata.Key<byte[]> sliceKeyHeader;

  SlicerPicker(
      SliceMap sliceMap,
      List<PickerEndpoint> endpoints,
      boolean fallbackEnabled,
      String sliceKeyHeaderName) {
    this.sliceMap = sliceMap;
    this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
    this.fallbackEnabled = fallbackEnabled;
    this.sliceKeyHeader = Metadata.Key.of(sliceKeyHeaderName, Metadata.BINARY_BYTE_MARSHALLER);

    this.sliceInFallback = new boolean[sliceMap.getSlices().size()];
    for (int i = 0; i < sliceInFallback.length; i++) {
      this.sliceInFallback[i] = isPoolInFallback(sliceMap.getSlices().get(i).endpoints);
    }
  }

  private boolean isPoolInFallback(List<Integer> indices) {
    if (indices.isEmpty()) {
      return true;
    }
    for (int idx : indices) {
      if (endpoints.get(idx).state != ConnectivityState.TRANSIENT_FAILURE) {
        return false;
      }
    }
    return true;
  }

  @Override
  public PickResult pickSubchannel(PickSubchannelArgs args) {
    byte[] keyBytes = args.getHeaders().get(sliceKeyHeader);
    ByteString key = keyBytes != null ? ByteString.copyFrom(keyBytes) : ByteString.EMPTY;

    Integer sliceIdx = sliceMap.lookup(key);

    if (sliceIdx == null) {
      if (fallbackEnabled) {
        return pickFromEndpointIndices(sliceMap.getFallbackPool(), args);
      } else {
        return PickResult.withError(
            Status.UNAVAILABLE.withDescription("No sharding assignment available and fallback disabled"));
      }
    }

    if (sliceInFallback[sliceIdx] && fallbackEnabled) {
      return pickFromEndpointIndices(sliceMap.getFallbackPool(), args);
    }

    SliceMap.SliceEntry sliceEntry = sliceMap.getSlices().get(sliceIdx);
    return pickFromEndpointIndices(sliceEntry.endpoints, args);
  }

  private PickResult pickFromEndpointIndices(
      List<Integer> indices, PickSubchannelArgs args) {
    if (indices.isEmpty()) {
      return PickResult.withError(
          Status.UNAVAILABLE.withDescription("No valid endpoints in slice and fallback disabled"));
    }

    int size = indices.size();
    int firstIndex = ThreadLocalRandom.current().nextInt(size);
    boolean requestedConnection = false;
    boolean foundConnecting = false;

    for (int i = 0; i < size; i++) {
      int epIdx = indices.get((firstIndex + i) % size);
      PickerEndpoint endpoint = endpoints.get(epIdx);

      if (endpoint.state == ConnectivityState.READY) {
        return endpoint.picker.pickSubchannel(args);
      }

      if (endpoint.state == ConnectivityState.CONNECTING) {
        foundConnecting = true;
      } else if (!requestedConnection && endpoint.state == ConnectivityState.IDLE) {
        if (endpoint.requestConnection != null) {
          endpoint.requestConnection.run();
        }
        requestedConnection = true;
      }
    }

    if (requestedConnection || foundConnecting) {
      return PickResult.withNoResult("connecting", "Waiting for endpoint connection");
    }

    int firstEpIdx = indices.get(firstIndex);
    return endpoints.get(firstEpIdx).picker.pickSubchannel(args);
  }
}
