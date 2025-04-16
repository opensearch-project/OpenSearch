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
 * Represents the sort order for WLM statistics.
 * The sort order can be either ascending or descending.
 */
public enum SortOrder {
    ASC {
        @Override
        public Comparator<WlmStats> apply(Comparator<WlmStats> baseComparator) {
            return baseComparator;
        }
    },
    DESC {
        @Override
        public Comparator<WlmStats> apply(Comparator<WlmStats> baseComparator) {
            return baseComparator.reversed();
        }
    };

    public abstract Comparator<WlmStats> apply(Comparator<WlmStats> baseComparator);

    public static SortOrder fromString(String input) {
        try {
            return SortOrder.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid sort order: " + input + ". Allowed values: asc, desc"
            );
        }
    }
}
