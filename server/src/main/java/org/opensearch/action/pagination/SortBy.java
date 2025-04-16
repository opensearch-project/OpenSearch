/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.wlm.stats.WlmStats;

import java.util.Comparator;

/**
 * Represents the different fields by which WLM statistics can be sorted.
 */
public enum SortBy {
    QUERY_GROUP {
        @Override
        public Comparator<WlmStats> getComparator() {
            return Comparator.comparing(
                (WlmStats wlmStats) -> wlmStats.getWorkloadGroupStats().getStats().isEmpty()
                    ? ""
                    : wlmStats.getWorkloadGroupStats().getStats().keySet().iterator().next()
            ).thenComparing(wlmStats -> wlmStats.getNode().getId());
        }
    },
    NODE_ID {
        @Override
        public Comparator<WlmStats> getComparator() {
            return Comparator.comparing(
                (WlmStats wlmStats) -> wlmStats.getNode().getId()
            ).thenComparing(
                wlmStats -> wlmStats.getWorkloadGroupStats().getStats().isEmpty()
                    ? ""
                    : wlmStats.getWorkloadGroupStats().getStats().keySet().iterator().next()
            );
        }
    };

    public abstract Comparator<WlmStats> getComparator();

    public static SortBy fromString(String input) {
        try {
            return SortBy.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid sort field: " + input + ". Allowed values: query_group, node_id"
            );
        }
    }
}

