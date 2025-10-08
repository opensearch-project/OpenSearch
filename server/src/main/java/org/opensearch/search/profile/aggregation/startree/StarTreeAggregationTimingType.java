/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.aggregation.startree;

import java.util.Locale;

/**
 * Timing levels for aggregations
 *
 * @opensearch.internal
 */
public enum StarTreeAggregationTimingType {
    SCAN_STAR_TREE_SEGMENTS,
    BUILD_BUCKETS_FROM_STAR_TREE;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
