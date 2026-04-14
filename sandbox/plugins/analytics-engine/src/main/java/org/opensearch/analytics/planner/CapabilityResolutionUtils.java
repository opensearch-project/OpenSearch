/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.EngineCapability;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility logic that operates on {@link CapabilityRegistry} results.
 *
 * @opensearch.internal
 */
public final class CapabilityResolutionUtils {

    private CapabilityResolutionUtils() {}

    /**
     * Filters viable backends to those that support COORDINATOR_REDUCE.
     *
     * <p>TODO: COORDINATOR_REDUCE will be replaced by a DataTransferCapability-based
     * declaration once the coordinator reduce model is redesigned.
     */
    public static List<String> filterByReduceCapability(CapabilityRegistry registry, List<String> viableBackends) {
        List<String> reduceCapable = registry.operatorBackends(EngineCapability.COORDINATOR_REDUCE);
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
}
