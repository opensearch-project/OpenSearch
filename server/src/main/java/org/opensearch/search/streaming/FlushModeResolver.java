/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

import java.util.Collection;

/**
 * Determines optimal {@link FlushMode} for streaming aggregations based on cost metrics.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class FlushModeResolver {

    private FlushModeResolver() {}

    /**
     * Minimum segment-level size for streaming aggregations to ensure accuracy.
     * This applies per-segment in streaming mode to control the topN buckets collected.
     * Default is 1000. Can be adjusted based on accuracy requirements.
     */
    public static final Setting<Integer> STREAMING_AGGREGATION_MIN_SEGMENT_SIZE_SETTING = Setting.intSetting(
        "index.aggregation.streaming.min_segment_size",
        1000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    /**
     * Maximum estimated bucket count allowed for streaming aggregations.
     * If an aggregation is estimated to produce more buckets than this threshold,
     * traditional shard-level processing will be used instead of streaming.
     * This prevents coordinator overload from processing too many streaming buckets.
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
     * traditional processing is used to avoid streaming overhead for small result sets.
     */
    public static final Setting<Long> STREAMING_MIN_ESTIMATED_BUCKET_COUNT = Setting.longSetting(
        "search.aggregations.streaming.min_estimated_bucket_count",
        1000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Evaluates cost metrics to determine if streaming is beneficial.
     *
     * @param metrics combined cost metrics from the factory tree
     * @param defaultMode fallback mode when streaming is not beneficial
     * @param maxBucketCount maximum bucket count threshold
     * @return {@link FlushMode#PER_SEGMENT} if streaming is beneficial, otherwise the default mode
     */
    public static FlushMode decideFlushMode(StreamingCostMetrics metrics, FlushMode defaultMode, long maxBucketCount) {
        if (!metrics.streamable()) {
            return defaultMode;
        }
        // Prevent coordinator overload with too many buckets
        if (metrics.topNSize() <= maxBucketCount) {
            return FlushMode.PER_SEGMENT;
        }
        return defaultMode;
    }

    /**
     * Determines if an aggregation tree is eligible for streaming based on aggregation types.
     *
     * <p>Streaming aggregations support:
     * <ul>
     *   <li>Top level: terms aggregations (string or numeric)</li>
     *   <li>Sub-aggregations: numeric terms, cardinality, max, min, sum</li>
     * </ul>
     *
     * @param aggregations the aggregation factories to validate
     * @return true if all aggregations are eligible for streaming, false otherwise
     */
    public static boolean isStreamable(AggregatorFactories.Builder aggregations) {
        if (aggregations == null || aggregations.count() == 0) {
            return false;
        }

        Collection<AggregationBuilder> topLevelAggs = aggregations.getAggregatorFactories();
        for (AggregationBuilder agg : topLevelAggs) {
            if (!isTopLevelStreamable(agg)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTopLevelStreamable(AggregationBuilder agg) {
        if (!(agg instanceof TermsAggregationBuilder)) {
            return false;
        }

        // Check sub-aggregations
        Collection<AggregationBuilder> subAggs = agg.getSubAggregations();
        for (AggregationBuilder subAgg : subAggs) {
            if (!isSubAggregationStreamable(subAgg)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSubAggregationStreamable(AggregationBuilder agg) {
        if (agg instanceof TermsAggregationBuilder) {
            // Level 2 sub-aggs can only be metrics, not more terms
            for (AggregationBuilder nestedAgg : agg.getSubAggregations()) {
                if (!isMetricAggregation(nestedAgg)) {
                    return false;
                }
            }
            return true;
        }
        return isMetricAggregation(agg);
    }

    private static boolean isMetricAggregation(AggregationBuilder agg) {
        return agg instanceof CardinalityAggregationBuilder
            || agg instanceof MaxAggregationBuilder
            || agg instanceof MinAggregationBuilder
            || agg instanceof SumAggregationBuilder;
    }
}
