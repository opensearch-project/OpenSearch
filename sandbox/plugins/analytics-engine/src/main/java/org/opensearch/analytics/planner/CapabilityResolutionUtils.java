/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Aggregate;
import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.analytics.spi.ShuffleCapability;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for resolving backend capabilities during planning.
 * Used by all marking rules to determine which backend handles each operator.
 *
 * @opensearch.internal
 */
public final class CapabilityResolutionUtils {

    private CapabilityResolutionUtils() {}

    /** True if the named backend supports the given operator capability. */
    public static boolean backendSupports(Map<String, AnalyticsSearchBackendPlugin> backends,
                                          String backendName, OperatorCapability capability) {
        AnalyticsSearchBackendPlugin plugin = backends.get(backendName);
        return plugin != null && plugin.supportedOperators().contains(capability);
    }

    /** Finds the first backend that supports the given capability, or null. */
    public static String findBackendWith(Map<String, AnalyticsSearchBackendPlugin> backends,
                                         OperatorCapability capability) {
        for (AnalyticsSearchBackendPlugin plugin : backends.values()) {
            if (plugin.supportedOperators().contains(capability)) {
                return plugin.name();
            }
        }
        return null;
    }

    /**
     * Resolves the backend for an operator: prefers childBackend if it supports
     * the capability, otherwise finds any backend that does.
     * Falls back to childBackend if none found.
     */
    public static String resolveBackend(Map<String, AnalyticsSearchBackendPlugin> backends,
                                        String childBackend, OperatorCapability capability) {
        if (backendSupports(backends, childBackend, capability)) {
            return childBackend;
        }
        String found = findBackendWith(backends, capability);
        return found != null ? found : childBackend;
    }

    /**
     * True if the named backend supports AGGREGATE and every aggregate function
     * required by the plan.
     */
    public static boolean backendSupportsAggregate(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                   String backendName, Aggregate aggregate) {
        AnalyticsSearchBackendPlugin plugin = backends.get(backendName);
        if (plugin == null || !plugin.supportedOperators().contains(OperatorCapability.AGGREGATE)) {
            return false;
        }
        return aggregate.getAggCallList().stream().allMatch(aggCall -> {
            AggregateFunction func = AggregateFunction.fromSqlKind(aggCall.getAggregation().getKind());
            if (func == null) {
                func = AggregateFunction.fromNameOrError(aggCall.getAggregation().getName());
            }
            return plugin.supportedAggregateFunctions().contains(func);
        });
    }

    /** Finds the first backend that supports AGGREGATE and all required functions. */
    public static String findBackendForAggregate(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                 Aggregate aggregate) {
        for (AnalyticsSearchBackendPlugin plugin : backends.values()) {
            if (backendSupportsAggregate(backends, plugin.name(), aggregate)) {
                return plugin.name();
            }
        }
        return null;
    }

    /**
     * Computes all backends viable for an operator capability, considering delegation.
     * A backend is viable if it supports the capability natively, OR if it can delegate
     * that type of work and at least one other backend can accept the delegation.
     */
    public static List<String> computeViableBackends(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                     OperatorCapability capability,
                                                     DelegationType delegationType) {
        boolean anyAcceptsDelegation = backends.values().stream()
            .anyMatch(b -> b.acceptedDelegations().contains(delegationType));

        List<String> viable = new ArrayList<>();
        for (AnalyticsSearchBackendPlugin plugin : backends.values()) {
            if (plugin.supportedOperators().contains(capability)) {
                viable.add(plugin.name());
            } else if (anyAcceptsDelegation && plugin.supportedDelegations().contains(delegationType)) {
                viable.add(plugin.name());
            }
        }
        return viable;
    }

    /** Computes all backends that natively support the given capability. No delegation. */
    public static List<String> computeViableBackends(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                     OperatorCapability capability) {
        List<String> viable = new ArrayList<>();
        for (AnalyticsSearchBackendPlugin plugin : backends.values()) {
            if (plugin.supportedOperators().contains(capability)) {
                viable.add(plugin.name());
            }
        }
        return viable;
    }

    /**
     * Computes all backends viable for a capability that also support the given data format.
     * Used by scan rule where the backend must match the index's primary data format.
     */
    public static List<String> computeViableBackends(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                     OperatorCapability capability, String dataFormat) {
        List<String> viable = new ArrayList<>();
        for (AnalyticsSearchBackendPlugin plugin : backends.values()) {
            if (plugin.supportedOperators().contains(capability)
                && plugin.getSupportedFormats().stream().anyMatch(f -> f.name().equals(dataFormat))) {
                viable.add(plugin.name());
            }
        }
        return viable;
    }

    /**
     * Resolves the shuffle implementation for a HASH/RANGE exchange based on the
     * backend's advertised shuffle capabilities. Prefers STREAM over FILE.
     * Only called for non-SINGLETON distributions.
     */
    public static ShuffleImpl resolveShuffleImpl(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                 String backendName, RelDistribution.Type distributionType) {
        AnalyticsSearchBackendPlugin plugin = backends.get(backendName);
        if (plugin != null) {
            if (plugin.supportedShuffleCapabilities().contains(ShuffleCapability.STREAM_WRITE)) {
                return ShuffleImpl.STREAM;
            }
            if (plugin.supportedShuffleCapabilities().contains(ShuffleCapability.FILE_WRITE)) {
                return ShuffleImpl.FILE;
            }
        }
        throw new IllegalStateException(
            "Backend [" + backendName + "] does not advertise any shuffle capability for distribution ["
                + distributionType + "]");
    }

    /**
     * Validates that the backend supports coordinator-side reduce.
     * Throws if it doesn't.
     */
    public static void validateReduceCapability(Map<String, AnalyticsSearchBackendPlugin> backends,
                                                String backendName) {
        if (!backendSupports(backends, backendName, OperatorCapability.COORDINATOR_REDUCE)) {
            throw new IllegalStateException(
                "Backend [" + backendName + "] does not support COORDINATOR_REDUCE capability");
        }
    }
}
