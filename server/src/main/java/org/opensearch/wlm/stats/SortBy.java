/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import java.util.Comparator;
import java.util.Locale;

/**
 * Represents the different fields by which WLM statistics can be sorted.
 */
public enum SortBy {
    WORKLOAD_GROUP {
        @Override
        public Comparator<WlmStats> getComparator() {
            return Comparator.comparing((WlmStats wlmStats) -> {
                if (wlmStats.getWorkloadGroupStats() == null
                    || wlmStats.getWorkloadGroupStats().getStats() == null
                    || wlmStats.getWorkloadGroupStats().getStats().isEmpty()) {
                    return "";
                }
                return wlmStats.getWorkloadGroupStats().getStats().keySet().iterator().next();
            }).thenComparing(wlmStats -> {
                if (wlmStats.getNode() == null || wlmStats.getNode().getId() == null) {
                    return "";
                }
                return wlmStats.getNode().getId();
            });
        }
    },
    NODE_ID {
        @Override
        public Comparator<WlmStats> getComparator() {
            return Comparator.comparing((WlmStats wlmStats) -> {
                if (wlmStats.getNode() == null || wlmStats.getNode().getId() == null) {
                    return "";
                }
                return wlmStats.getNode().getId();
            }).thenComparing(wlmStats -> {
                if (wlmStats.getWorkloadGroupStats() == null
                    || wlmStats.getWorkloadGroupStats().getStats() == null
                    || wlmStats.getWorkloadGroupStats().getStats().isEmpty()) {
                    return "";
                }
                return wlmStats.getWorkloadGroupStats().getStats().keySet().iterator().next();
            });
        }
    };

    public abstract Comparator<WlmStats> getComparator();

    public static SortBy fromString(String input) {
        try {
            return SortBy.valueOf(input.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid sort field: " + input + ". Allowed values: workload_group, node_id");
        }
    }
}
