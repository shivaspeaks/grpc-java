package io.grpc.slicer;

import io.grpc.ConnectivityState;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.Status;

/**
 * A wrapper LoadBalancer that lazily creates and delegates to a child LoadBalancer (e.g. pick_first)
 * only when {@link #requestConnection()} is explicitly called. Until then, it remains in IDLE state.
 */
final class LazyChildLoadBalancer extends LoadBalancer {
  private final Helper helper;
  private final LoadBalancerProvider delegateProvider;
  private LoadBalancer delegate;
  private ResolvedAddresses lastResolvedAddresses;
  private boolean connectionRequested = false;

  LazyChildLoadBalancer(Helper helper, LoadBalancerProvider delegateProvider) {
    this.helper = helper;
    this.delegateProvider = delegateProvider;
  }

  @Override
  public Status acceptResolvedAddresses(ResolvedAddresses resolvedAddresses) {
    lastResolvedAddresses = resolvedAddresses;
    if (connectionRequested) {
      if (delegate == null) {
        delegate = delegateProvider.newLoadBalancer(helper);
      }
      return delegate.acceptResolvedAddresses(resolvedAddresses);
    } else {
      // Report IDLE state until connection is explicitly requested
      helper.updateBalancingState(
          ConnectivityState.IDLE,
          new FixedResultPicker(PickResult.withNoResult()));
      return Status.OK;
    }
  }

  @Override
  public void handleNameResolutionError(Status error) {
    if (delegate != null) {
      delegate.handleNameResolutionError(error);
    } else {
      helper.updateBalancingState(
          ConnectivityState.TRANSIENT_FAILURE,
          new FixedResultPicker(PickResult.withError(error)));
    }
  }

  @Override
  public void requestConnection() {
    connectionRequested = true;
    if (delegate == null && lastResolvedAddresses != null) {
      delegate = delegateProvider.newLoadBalancer(helper);
      delegate.acceptResolvedAddresses(lastResolvedAddresses);
    } else if (delegate != null) {
      delegate.requestConnection();
    }
  }

  @Override
  public void shutdown() {
    if (delegate != null) {
      delegate.shutdown();
      delegate = null;
    }
  }

  // Visible for testing
  boolean isConnectionRequested() {
    return connectionRequested;
  }
}
