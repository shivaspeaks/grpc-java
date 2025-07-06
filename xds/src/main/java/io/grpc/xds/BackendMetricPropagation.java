/*
 * Copyright 2024 The gRPC Authors
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

package io.grpc.xds;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.grpc.Internal;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents the configuration for which ORCA metrics should be propagated from backend
 * to LRS load reports, as defined in gRFC A85.
 */
@Internal
public final class BackendMetricPropagation {
  
  // Top-level field propagation flags
  private final boolean propagateCpuUtilization;
  private final boolean propagateMemUtilization;
  private final boolean propagateApplicationUtilization;
  
  // Named metrics propagation
  private final boolean propagateAllNamedMetrics;
  private final ImmutableSet<String> namedMetricKeys;
  
  private BackendMetricPropagation(
      boolean propagateCpuUtilization,
      boolean propagateMemUtilization, 
      boolean propagateApplicationUtilization,
      boolean propagateAllNamedMetrics,
      ImmutableSet<String> namedMetricKeys) {
    this.propagateCpuUtilization = propagateCpuUtilization;
    this.propagateMemUtilization = propagateMemUtilization;
    this.propagateApplicationUtilization = propagateApplicationUtilization;
    this.propagateAllNamedMetrics = propagateAllNamedMetrics;
    this.namedMetricKeys = checkNotNull(namedMetricKeys, "namedMetricKeys");
  }
  
  /**
   * Creates a BackendMetricPropagation from a list of metric specifications.
   * 
   * @param metricSpecs list of metric specification strings from CDS resource
   * @return BackendMetricPropagation instance
   */
  public static BackendMetricPropagation fromMetricSpecs(@Nullable java.util.List<String> metricSpecs) {
    if (metricSpecs == null || metricSpecs.isEmpty()) {
      // If no specs provided, don't propagate anything (per gRFC A85)
      return new BackendMetricPropagation(false, false, false, false, ImmutableSet.of());
    }
    
    boolean propagateCpuUtilization = false;
    boolean propagateMemUtilization = false;
    boolean propagateApplicationUtilization = false;
    boolean propagateAllNamedMetrics = false;
    ImmutableSet.Builder<String> namedMetricKeysBuilder = ImmutableSet.builder();
    
    for (String spec : metricSpecs) {
      if (spec == null) {
        continue;
      }
      
      switch (spec) {
        case "cpu_utilization":
          propagateCpuUtilization = true;
          break;
        case "mem_utilization":
          propagateMemUtilization = true;
          break;
        case "application_utilization":
          propagateApplicationUtilization = true;
          break;
        case "named_metrics.*":
          propagateAllNamedMetrics = true;
          break;
        default:
          // Check if it's a named metric specification
          if (spec.startsWith("named_metrics.")) {
            String metricKey = spec.substring("named_metrics.".length());
            if (!metricKey.isEmpty()) {
              namedMetricKeysBuilder.add(metricKey);
            }
          }
          // Ignore unknown specs for forward compatibility
          break;
      }
    }
    
    return new BackendMetricPropagation(
        propagateCpuUtilization,
        propagateMemUtilization,
        propagateApplicationUtilization,
        propagateAllNamedMetrics,
        namedMetricKeysBuilder.build());
  }
  
  /**
   * Returns whether CPU utilization should be propagated.
   */
  public boolean propagateCpuUtilization() {
    return propagateCpuUtilization;
  }
  
  /**
   * Returns whether memory utilization should be propagated.
   */
  public boolean propagateMemUtilization() {
    return propagateMemUtilization;
  }
  
  /**
   * Returns whether application utilization should be propagated.
   */
  public boolean propagateApplicationUtilization() {
    return propagateApplicationUtilization;
  }
  
  /**
   * Returns whether all named metrics should be propagated.
   */
  public boolean propagateAllNamedMetrics() {
    return propagateAllNamedMetrics;
  }
  
  /**
   * Returns the set of specific named metric keys to propagate.
   * Only used if propagateAllNamedMetrics() returns false.
   */
  public ImmutableSet<String> namedMetricKeys() {
    return namedMetricKeys;
  }
  
  /**
   * Returns whether the given named metric key should be propagated.
   */
  public boolean shouldPropagateNamedMetric(String metricKey) {
    return propagateAllNamedMetrics || namedMetricKeys.contains(metricKey);
  }
  
  /**
   * Returns whether any metrics should be propagated.
   */
  public boolean shouldPropagateAnyMetrics() {
    return propagateCpuUtilization || propagateMemUtilization || propagateApplicationUtilization
        || propagateAllNamedMetrics || !namedMetricKeys.isEmpty();
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BackendMetricPropagation that = (BackendMetricPropagation) o;
    return propagateCpuUtilization == that.propagateCpuUtilization
        && propagateMemUtilization == that.propagateMemUtilization
        && propagateApplicationUtilization == that.propagateApplicationUtilization
        && propagateAllNamedMetrics == that.propagateAllNamedMetrics
        && Objects.equals(namedMetricKeys, that.namedMetricKeys);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(propagateCpuUtilization, propagateMemUtilization,
        propagateApplicationUtilization, propagateAllNamedMetrics, namedMetricKeys);
  }
  
  @Override
  public String toString() {
    return "BackendMetricPropagation{"
        + "propagateCpuUtilization=" + propagateCpuUtilization
        + ", propagateMemUtilization=" + propagateMemUtilization
        + ", propagateApplicationUtilization=" + propagateApplicationUtilization
        + ", propagateAllNamedMetrics=" + propagateAllNamedMetrics
        + ", namedMetricKeys=" + namedMetricKeys
        + '}';
  }
  
  @VisibleForTesting
  static BackendMetricPropagation createForTesting(
      boolean propagateCpuUtilization,
      boolean propagateMemUtilization,
      boolean propagateApplicationUtilization,
      boolean propagateAllNamedMetrics,
      Set<String> namedMetricKeys) {
    return new BackendMetricPropagation(
        propagateCpuUtilization,
        propagateMemUtilization,
        propagateApplicationUtilization,
        propagateAllNamedMetrics,
        ImmutableSet.copyOf(namedMetricKeys));
  }
} 