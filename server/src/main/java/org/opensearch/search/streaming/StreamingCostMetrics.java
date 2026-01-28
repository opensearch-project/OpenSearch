/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Cost analysis metrics for streaming aggregation decisions.
 *
 * @param streamable whether this aggregation supports streaming
 * @param topNSize number of top buckets sent per partial agg result
 * @opensearch.experimental
 */
@ExperimentalApi
public record StreamingCostMetrics(boolean streamable, int topNSize) {
    public StreamingCostMetrics {
        assert topNSize >= 0 : "topNSize must be non-negative";
    }

    /**
     * Creates metrics indicating an aggregation does not support streaming.
     *
     * @return metrics with streamable=false and zero for topNSize
     */
    public static StreamingCostMetrics nonStreamable() {
        return new StreamingCostMetrics(false, 0);
    }

    /**
     * Creates metrics for aggregations that are compatible with streaming but don't
     * contribute to the streaming cost decision (e.g., metrics aggregations like max, min, avg).
     *
     * <p>These aggregations can be nested within streaming bucket aggregations without
     * blocking streaming, but their cost is negligible (single value output).
     *
     * @return metrics with streamable=true and topNSize=1
     */
    public static StreamingCostMetrics neutral() {
        return new StreamingCostMetrics(true, 1);
    }

    /**
     * Combines metrics for parent-child aggregation relationships.
     *
     * <p>Models nested aggregation scenarios where child aggregations execute once per
     * parent bucket, creating multiplicative cost effects. For example, a terms aggregation
     * with a nested avg sub-aggregation.
     *
     * @param subAggMetrics metrics from the child aggregation
     * @return combined metrics reflecting the nested relationship, or non-streamable if either input is non-streamable or overflow occurs
     */
    public StreamingCostMetrics combineWithSubAggregation(StreamingCostMetrics subAggMetrics) {
        assert subAggMetrics != null : "subAggMetrics must not be null";
        if (!this.streamable || !subAggMetrics.streamable) {
            return nonStreamable();
        }

        try {
            int combinedTopNSize = Math.multiplyExact(this.topNSize, subAggMetrics.topNSize);
            return new StreamingCostMetrics(true, combinedTopNSize);
        } catch (ArithmeticException e) {
            return nonStreamable();
        }
    }

    /**
     * Combines metrics for sibling aggregation relationships.
     *
     * <p>Models parallel aggregation scenarios where multiple aggregations execute
     * independently at the same level, creating additive cost effects. For example,
     * multiple terms aggregations in the same aggregation request.
     *
     * @param siblingMetrics metrics from the sibling aggregation
     * @return combined metrics reflecting the parallel relationship, or non-streamable if either input is non-streamable or overflow occurs
     */
    public StreamingCostMetrics combineWithSibling(StreamingCostMetrics siblingMetrics) {
        assert siblingMetrics != null : "siblingMetrics must not be null";
        if (!this.streamable || !siblingMetrics.streamable) {
            return nonStreamable();
        }

        try {
            int combinedTopNSize = Math.addExact(this.topNSize, siblingMetrics.topNSize);
            return new StreamingCostMetrics(true, combinedTopNSize);
        } catch (ArithmeticException e) {
            return nonStreamable();
        }
    }
}
