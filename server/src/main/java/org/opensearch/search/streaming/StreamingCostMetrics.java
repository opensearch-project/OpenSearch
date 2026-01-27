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
 * <p>Provides quantitative data to compare streaming per-segment processing against
 * traditional per-shard processing. The {@link FlushModeResolver} uses these metrics
 * to determine the optimal {@link FlushMode} for a given aggregation tree.
 *
 * <p>Metrics capture the fundamental trade-offs of streaming: faster response times
 * versus increased coordinator overhead and memory usage. All metrics are shard-scoped
 * unless otherwise specified.
 *
 * @param streamable whether this Streamable supports streaming - if false, other parameters are ignored
 * @param topNSize number of top buckets sent per partial agg result (multi-bucket aggregations only)
 * @param estimatedBucketCount estimated number of buckets this aggregation will produce (multi-bucket aggregations only)
 * @param estimatedDocCount estimated number of documents that have this field
 * @opensearch.experimental
 */
@ExperimentalApi
public record StreamingCostMetrics(boolean streamable, long topNSize, long estimatedBucketCount, long estimatedDocCount) {
    public StreamingCostMetrics {
        assert topNSize >= 0 : "topNSize must be non-negative";
        assert estimatedBucketCount >= 0 : "estimatedBucketCount must be non-negative";
        assert estimatedDocCount >= 0 : "estimatedDocCount must be non-negative";
    }

    /**
     * Creates metrics indicating an aggregation does not support streaming.
     *
     * @return metrics with streamable=false and zero values for all cost parameters
     */
    public static StreamingCostMetrics nonStreamable() {
        return new StreamingCostMetrics(false, 0, 0, 0);
    }

    /**
     * Creates metrics for aggregations that are compatible with streaming but don't
     * contribute to the streaming cost decision (e.g., metrics aggregations like max, min, avg).
     *
     * <p>These aggregations can be nested within streaming bucket aggregations without
     * blocking streaming, but their cost is negligible (single value output).
     *
     * @return metrics with streamable=true and minimal values (1, 1, 1)
     */
    public static StreamingCostMetrics neutral() {
        return new StreamingCostMetrics(true, 1, 1, 1);
    }

    /**
     * Combines metrics for parent-child aggregation relationships.
     *
     * <p>Models nested aggregation scenarios where child aggregations execute once per
     * parent bucket, creating multiplicative cost effects. For example, a terms aggregation
     * with a nested avg sub-aggregation.
     *
     * @param subAggMetrics metrics from the child aggregation
     * @return combined metrics reflecting the nested relationship, or non-streamable if either input is non-streamable
     */
    public StreamingCostMetrics combineWithSubAggregation(StreamingCostMetrics subAggMetrics) {
        assert subAggMetrics != null : "subAggMetrics must not be null";
        if (!this.streamable || !subAggMetrics.streamable) {
            return nonStreamable();
        }

        long combinedTopNSize;
        try {
            combinedTopNSize = Math.multiplyExact(this.topNSize, subAggMetrics.topNSize);
        } catch (ArithmeticException e) {
            return nonStreamable();
        }

        long combinedEstimatedBucketCount;
        try {
            combinedEstimatedBucketCount = Math.multiplyExact(this.estimatedBucketCount, subAggMetrics.estimatedBucketCount);
        } catch (ArithmeticException e) {
            return nonStreamable();
        }

        return new StreamingCostMetrics(
            true,
            combinedTopNSize,
            combinedEstimatedBucketCount,
            Math.max(this.estimatedDocCount, subAggMetrics.estimatedDocCount)
        );
    }

    /**
     * Combines metrics for sibling aggregation relationships.
     *
     * <p>Models parallel aggregation scenarios where multiple aggregations execute
     * independently at the same level, creating additive cost effects. For example,
     * multiple terms aggregations in the same aggregation request.
     *
     * @param siblingMetrics metrics from the sibling aggregation
     * @return combined metrics reflecting the parallel relationship, or non-streamable if either input is non-streamable
     */
    public StreamingCostMetrics combineWithSibling(StreamingCostMetrics siblingMetrics) {
        assert siblingMetrics != null : "siblingMetrics must not be null";
        if (!this.streamable || !siblingMetrics.streamable) {
            return nonStreamable();
        }

        long combinedTopNSize;
        try {
            combinedTopNSize = Math.addExact(this.topNSize, siblingMetrics.topNSize);
        } catch (ArithmeticException e) {
            return nonStreamable();
        }

        long combinedEstimatedBucketCount;
        try {
            combinedEstimatedBucketCount = Math.addExact(this.estimatedBucketCount, siblingMetrics.estimatedBucketCount);
        } catch (ArithmeticException e) {
            return nonStreamable();
        }

        return new StreamingCostMetrics(
            true,
            combinedTopNSize,
            combinedEstimatedBucketCount,
            this.estimatedDocCount + siblingMetrics.estimatedDocCount
        );
    }
}
