/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.analytics.spi.ShuffleCapability;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility logic that operates on {@link CapabilityRegistry} results.
 * Registry handles indexed lookups; this class handles filtering and resolution logic.
 *
 * @opensearch.internal
 */
public final class CapabilityResolutionUtils {

    private CapabilityResolutionUtils() {}

    /** Filters viable backends to those that support COORDINATOR_REDUCE. */
    public static List<String> filterByReduceCapability(CapabilityRegistry registry, List<String> viableBackends) {
        List<String> reduceCapable = registry.operatorBackends(OperatorCapability.COORDINATOR_REDUCE);
        List<String> result = new ArrayList<>();
        for (String name : viableBackends) {
            if (reduceCapable.contains(name)) {
                result.add(name);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException("No viable backend supports COORDINATOR_REDUCE among " + viableBackends);
        }
        return result;
    }

    /** Filters viable backends to those with any shuffle capability. */
    public static List<String> filterByShuffleCapability(CapabilityRegistry registry, List<String> viableBackends) {
        List<String> result = new ArrayList<>();
        for (String name : viableBackends) {
            if (!registry.getShuffleCapabilities(name).isEmpty()) {
                result.add(name);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException("No viable backend supports shuffle among " + viableBackends);
        }
        return result;
    }

    /** Picks the best shuffle impl across all shuffle-viable backends. Prefers STREAM over FILE. */
    public static ShuffleImpl bestShuffleImpl(CapabilityRegistry registry, List<String> shuffleViable) {
        boolean hasStream = false;
        boolean hasFile = false;
        for (String name : shuffleViable) {
            var caps = registry.getShuffleCapabilities(name);
            if (caps.contains(ShuffleCapability.STREAM_WRITE)) {
                hasStream = true;
            }
            if (caps.contains(ShuffleCapability.FILE_WRITE)) {
                hasFile = true;
            }
        }
        if (hasStream) {
            return ShuffleImpl.STREAM;
        }
        if (hasFile) {
            return ShuffleImpl.FILE;
        }
        throw new IllegalStateException("No shuffle impl available among " + shuffleViable);
    }
}
