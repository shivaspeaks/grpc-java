package io.grpc.slicer;

import com.google.common.base.MoreObjects;
import io.grpc.Internal;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.NameResolver.ConfigOrError;
import io.grpc.internal.JsonUtil;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

@Internal
public final class SlicerLoadBalancerProvider extends LoadBalancerProvider {
  static final String POLICY_NAME = "autosharding_experimental";

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public int getPriority() {
    return 5;
  }

  @Override
  public String getPolicyName() {
    return POLICY_NAME;
  }

  @Override
  public LoadBalancer newLoadBalancer(LoadBalancer.Helper helper) {
    return new SlicerLoadBalancer(helper);
  }

  @Override
  public ConfigOrError parseLoadBalancingPolicyConfig(Map<String, ?> rawLoadBalancingPolicyConfig) {
    try {
      return ConfigOrError.fromConfig(parseConfig(rawLoadBalancingPolicyConfig));
    } catch (RuntimeException e) {
      return ConfigOrError.fromError(
          io.grpc.Status.UNKNOWN.withDescription("Failed to parse config: " + e.getMessage()).withCause(e));
    }
  }

  private SlicerConfig parseConfig(Map<String, ?> config) {
    String channelFactoryKey = JsonUtil.getString(config, "channelFactoryKey");
    String slicingTarget = JsonUtil.getString(config, "slicingTarget");
    String sliceKeyHeaderName = JsonUtil.getString(config, "sliceKeyHeaderName");
    Boolean enableFallback = JsonUtil.getBoolean(config, "enableFallback");
    // Default is 60 seconds if not provided.
    Long initialAssignmentTimeoutNanos = JsonUtil.getStringAsDuration(config, "initialAssignmentTimeout");
    
    if (enableFallback == null) {
      enableFallback = false;
    }
    
    return new SlicerConfig(
        channelFactoryKey, 
        slicingTarget, 
        sliceKeyHeaderName, 
        enableFallback, 
        initialAssignmentTimeoutNanos);
  }

  public static final class SlicerConfig {
    final String channelFactoryKey;
    final String slicingTarget;
    final String sliceKeyHeaderName;
    final boolean enableFallback;
    final Long initialAssignmentTimeoutNanos;

    public SlicerConfig(
        String channelFactoryKey,
        String slicingTarget,
        String sliceKeyHeaderName,
        boolean enableFallback,
        Long initialAssignmentTimeoutNanos) {
      this.channelFactoryKey = channelFactoryKey;
      this.slicingTarget = slicingTarget;
      this.sliceKeyHeaderName = sliceKeyHeaderName;
      this.enableFallback = enableFallback;
      this.initialAssignmentTimeoutNanos = initialAssignmentTimeoutNanos;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SlicerConfig that = (SlicerConfig) o;
      return enableFallback == that.enableFallback
          && Objects.equals(channelFactoryKey, that.channelFactoryKey)
          && Objects.equals(slicingTarget, that.slicingTarget)
          && Objects.equals(sliceKeyHeaderName, that.sliceKeyHeaderName)
          && Objects.equals(initialAssignmentTimeoutNanos, that.initialAssignmentTimeoutNanos);
    }

    @Override
    public int hashCode() {
      return Objects.hash(channelFactoryKey, slicingTarget, sliceKeyHeaderName, enableFallback, initialAssignmentTimeoutNanos);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("channelFactoryKey", channelFactoryKey)
          .add("slicingTarget", slicingTarget)
          .add("sliceKeyHeaderName", sliceKeyHeaderName)
          .add("enableFallback", enableFallback)
          .add("initialAssignmentTimeoutNanos", initialAssignmentTimeoutNanos)
          .toString();
    }
  }
}
