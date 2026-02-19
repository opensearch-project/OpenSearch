/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Context object that encapsulates all cardinality aggregation related settings and configurations.
 * This helps keep cardinality-specific settings scoped properly and reduces SearchContext bloat.
 */
@PublicApi(since = "3.4.0")
public class CardinalityAggregationContext {
    private final boolean hybridCollectorEnabled;
    private final long memoryThreshold;

    public CardinalityAggregationContext(boolean hybridCollectorEnabled, long memoryThreshold) {
        this.hybridCollectorEnabled = hybridCollectorEnabled;
        this.memoryThreshold = memoryThreshold;
    }

    public boolean isHybridCollectorEnabled() {
        return hybridCollectorEnabled;
    }

    public long getMemoryThreshold() {
        return memoryThreshold;
    }

    /**
     * Creates a CardinalityAggregationContext from cluster settings
     */
    public static CardinalityAggregationContext from(boolean hybridCollectorEnabled, ByteSizeValue memoryThreshold) {
        return new CardinalityAggregationContext(hybridCollectorEnabled, memoryThreshold.getBytes());
    }
}
