package io.grpc.slicer;

import io.grpc.ConnectivityState;
import io.grpc.LoadBalancer.SubchannelPicker;

/**
 * Immutable snapshot of endpoint state used by the SlicerPicker.
 */
final class PickerEndpoint {
  final ConnectivityState state;
  final SubchannelPicker picker;
  final Runnable requestConnection;

  PickerEndpoint(ConnectivityState state, SubchannelPicker picker, Runnable requestConnection) {
    this.state = state;
    this.picker = picker;
    this.requestConnection = requestConnection;
  }
}
