/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Cost analysis metrics for streaming aggregation decisions.
 *
 * <p>
 * Provides quantitative data to compare streaming per-segment processing
 * against
 * traditional per-shard processing. The {@link FlushModeResolver} uses these
 * metrics
 * to determine the optimal {@link FlushMode} for a given aggregation tree.
 *
 * <p>
 * Metrics capture the fundamental trade-offs of streaming: faster response
 * times
 * versus increased coordinator overhead and memory usage. All metrics are
 * shard-scoped
 * unless otherwise specified.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class StreamingCostMetrics {
    private final boolean streamable;
    private final long topNSize;
    private final long estimatedBucketCount;
    private final int segmentCount;
    private final long estimatedDocCount;

    /**
     * @param streamable           whether this Streamable supports streaming - if
     *                             false, other parameters are ignored
     * @param topNSize             number of top buckets sent per shard in
     *                             traditional processing (multi-bucket aggregations
     *                             only)
     * @param estimatedBucketCount estimated number of buckets this aggregation will
     *                             produce (multi-bucket aggregations only)
     * @param segmentCount         number of segments in this shard (used for
     *                             streaming volume calculations)
     * @param estimatedDocCount    estimated number of documents that have this
     *                             field
     */
    public StreamingCostMetrics(boolean streamable, long topNSize, long estimatedBucketCount, int segmentCount, long estimatedDocCount) {
        assert topNSize >= 0 : "topNSize must be non-negative";
        assert estimatedBucketCount >= 0 : "estimatedBucketCount must be non-negative";
        assert segmentCount >= 0 : "segmentCount must be non-negative";
        assert estimatedDocCount >= 0 : "estimatedDocCount must be non-negative";
        this.streamable = streamable;
        this.topNSize = topNSize;
        this.estimatedBucketCount = estimatedBucketCount;
        this.segmentCount = segmentCount;
        this.estimatedDocCount = estimatedDocCount;
    }

    public boolean streamable() {
        return streamable;
    }

    public long topNSize() {
        return topNSize;
    }

    public long estimatedBucketCount() {
        return estimatedBucketCount;
    }

    public int segmentCount() {
        return segmentCount;
    }

    public long estimatedDocCount() {
        return estimatedDocCount;
    }

    /**
     * Creates metrics indicating an aggregation does not support streaming.
     *
     * @return metrics with streamable=false and zero values for all cost parameters
     */
    public static StreamingCostMetrics nonStreamable() {
        return new StreamingCostMetrics(false, 0, 0, 0, 0);
    }

    /**
     * Combines metrics for parent-child aggregation relationships.
     *
     * <p>
     * Models nested aggregation scenarios where child aggregations execute once per
     * parent bucket, creating multiplicative cost effects. For example, a terms
     * aggregation
     * with a nested avg sub-aggregation.
     *
     * @param subAggMetrics metrics from the child aggregation
     * @return combined metrics reflecting the nested relationship, or
     *         non-streamable if either input is non-streamable
     */
    public StreamingCostMetrics combineWithSubAggregation(StreamingCostMetrics subAggMetrics) {
        if (!this.streamable || subAggMetrics == null || !subAggMetrics.streamable) {
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
            Math.max(this.segmentCount, subAggMetrics.segmentCount),
            Math.max(this.estimatedDocCount, subAggMetrics.estimatedDocCount)
        );
    }

    /**
     * Combines metrics for sibling aggregation relationships.
     *
     * <p>
     * Models parallel aggregation scenarios where multiple aggregations execute
     * independently at the same level, creating additive cost effects. For example,
     * multiple terms aggregations in the same aggregation request.
     *
     * @param siblingMetrics metrics from the sibling aggregation
     * @return combined metrics reflecting the parallel relationship, or
     *         non-streamable if either input is non-streamable
     */
    public StreamingCostMetrics combineWithSibling(StreamingCostMetrics siblingMetrics) {
        if (!this.streamable || siblingMetrics == null || !siblingMetrics.streamable) {
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
            Math.max(this.segmentCount, siblingMetrics.segmentCount),
            this.estimatedDocCount + siblingMetrics.estimatedDocCount
        );
    }

    public boolean isStreamable() {
        return streamable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamingCostMetrics that = (StreamingCostMetrics) o;
        return streamable == that.streamable
            && topNSize == that.topNSize
            && estimatedBucketCount == that.estimatedBucketCount
            && segmentCount == that.segmentCount
            && estimatedDocCount == that.estimatedDocCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamable, topNSize, estimatedBucketCount, segmentCount, estimatedDocCount);
    }

    @Override
    public String toString() {
        return "StreamingCostMetrics["
            + "streamable="
            + streamable
            + ", "
            + "topNSize="
            + topNSize
            + ", "
            + "estimatedBucketCount="
            + estimatedBucketCount
            + ", "
            + "segmentCount="
            + segmentCount
            + ", "
            + "estimatedDocCount="
            + estimatedDocCount
            + ']';
    }
}
