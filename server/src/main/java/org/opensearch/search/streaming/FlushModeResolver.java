/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MultiCollector;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.search.aggregations.AggregatorBase;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.profile.aggregation.ProfilingAggregator;

/**
 * Analyzes collector trees to determine optimal {@link FlushMode} for streaming
 * aggregations.
 *
 * <p>
 * Performs cost-benefit analysis by examining all collectors in the tree.
 * Streaming is only
 * enabled when all collectors implement {@link Streamable} and the combined
 * cost metrics
 * indicate streaming will be beneficial compared to traditional shard-level
 * processing.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class FlushModeResolver {

    private static final Logger logger = LogManager.getLogger(FlushModeResolver.class);

    /**
     * Maximum estimated bucket count allowed for streaming aggregations.
     * If an aggregation is estimated to produce more buckets than this threshold,
     * traditional shard-level processing will be used instead of streaming.
     * This prevents coordinator overload from processing too many streaming
     * buckets.
     */
    public static final Setting<Long> STREAMING_MAX_ESTIMATED_BUCKET_COUNT = Setting.longSetting(
        "search.aggregations.streaming.max_estimated_bucket_count",
        100_000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum cardinality ratio required for streaming aggregations.
     * Calculated as (estimated_buckets / documents_with_field).
     * If the ratio is below this threshold, traditional processing is used
     * to prevent performance regression on low-cardinality data.
     * Range: 0.0 to 1.0, where 0.01 means at least 1% unique values.
     */
    public static final Setting<Double> STREAMING_MIN_CARDINALITY_RATIO = Setting.doubleSetting(
        "search.aggregations.streaming.min_cardinality_ratio",
        0.01,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum estimated bucket count required for streaming aggregations.
     * If an aggregation is estimated to produce fewer buckets than this threshold,
     * traditional processing is used to avoid streaming overhead for small result
     * sets.
     */
    public static final Setting<Long> STREAMING_MIN_ESTIMATED_BUCKET_COUNT = Setting.longSetting(
        "search.aggregations.streaming.min_estimated_bucket_count",
        1000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Determines the optimal flush mode for the given collector tree.
     *
     * @param collector           the root collector to analyze
     * @param defaultMode         fallback mode if streaming is not supported
     * @param maxBucketCount      maximum bucket count threshold
     * @param minCardinalityRatio minimum cardinality ratio threshold
     * @param minBucketCount      minimum estimated bucket count threshold
     * @return {@link FlushMode#PER_SEGMENT} if streaming is beneficial, otherwise
     *         the default mode
     */
    public static FlushMode resolve(
        Collector collector,
        FlushMode defaultMode,
        long maxBucketCount,
        double minCardinalityRatio,
        long minBucketCount
    ) {
        StreamingCostMetrics metrics = collectMetrics(collector);
        FlushMode decision = decideFlushMode(metrics, defaultMode, maxBucketCount, minCardinalityRatio, minBucketCount);
        return decision;

    }

    /**
     * Collects and combines streaming metrics from the collector tree.
     *
     * @param collector the collector to analyze
     * @return combined metrics if all collectors support streaming, nonStreamable
     *         otherwise
     */
    private static StreamingCostMetrics collectMetrics(Collector collector) {
        if (!(collector instanceof Streamable
            || collector instanceof MultiBucketCollector
            || collector instanceof MultiCollector
            || collector instanceof ProfilingAggregator
            || collector instanceof AggregatorBase)) {
            return StreamingCostMetrics.nonStreamable();
        }

        StreamingCostMetrics nodeMetrics = null;
        if (collector instanceof Streamable streamable) {
            nodeMetrics = streamable.getStreamingCostMetrics();
            if (!nodeMetrics.isStreamable()) {
                return StreamingCostMetrics.nonStreamable();
            }
        }

        StreamingCostMetrics childMetrics = null;
        for (Collector child : getChildren(collector)) {
            StreamingCostMetrics childResult = collectMetrics(child);
            if (!childResult.isStreamable()) {
                return StreamingCostMetrics.nonStreamable();
            }
            childMetrics = (childMetrics == null) ? childResult : childMetrics.combineWithSibling(childResult);
        }

        if (nodeMetrics == null) {
            return childMetrics == null ? StreamingCostMetrics.nonStreamable() : childMetrics;
        }
        return childMetrics != null ? nodeMetrics.combineWithSubAggregation(childMetrics) : nodeMetrics;
    }

    private static Collector[] getChildren(Collector collector) {
        return switch (collector) {
            case AggregatorBase aggregatorBase -> aggregatorBase.subAggregators();
            case MultiCollector multiCollector -> multiCollector.getCollectors();
            case MultiBucketCollector multiBucketCollector -> multiBucketCollector.getCollectors();
            case ProfilingAggregator profilingAggregator -> getChildren(profilingAggregator.unwrapAggregator());
            default -> new Collector[0];
        };
    }

    /**
     * Evaluates cost metrics to determine if streaming is beneficial.
     *
     * @param metrics             combined cost metrics from the collector tree
     * @param defaultMode         fallback mode when streaming is not beneficial
     * @param maxBucketCount      maximum bucket count threshold
     * @param minCardinalityRatio minimum cardinality ratio threshold
     * @return {@link FlushMode#PER_SEGMENT} if streaming is beneficial, otherwise
     *         the default mode
     */
    private static FlushMode decideFlushMode(
        StreamingCostMetrics metrics,
        FlushMode defaultMode,
        long maxBucketCount,
        double minCardinalityRatio,
        long minBucketCount
    ) {
        if (!metrics.isStreamable()) {
            return defaultMode;
        }
        if (metrics.estimatedBucketCount() > maxBucketCount) {
            return defaultMode;
        }

        if (metrics.estimatedBucketCount() < minBucketCount) {
            return defaultMode;
        }

        if (metrics.estimatedDocCount() > 0) {
            double cardinalityRatio = (double) metrics.estimatedBucketCount() / metrics.estimatedDocCount();
            if (cardinalityRatio < minCardinalityRatio) {
                return defaultMode;
            }
        }

        return FlushMode.PER_SEGMENT;
    }

}
