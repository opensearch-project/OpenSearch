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

/**
 * Determines optimal {@link FlushMode} for streaming aggregations based on cost metrics.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class FlushModeResolver {

    private FlushModeResolver() {}

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
     * @param minCardinalityRatio minimum cardinality ratio threshold
     * @param minBucketCount minimum bucket count threshold
     * @return {@link FlushMode#PER_SEGMENT} if streaming is beneficial, otherwise the default mode
     */
    public static FlushMode decideFlushMode(
        StreamingCostMetrics metrics,
        FlushMode defaultMode,
        long maxBucketCount,
        double minCardinalityRatio,
        long minBucketCount
    ) {
        if (!metrics.streamable()) {
            return defaultMode;
        }
        // Check coordinator overhead - don't stream if too many buckets
        if (metrics.estimatedBucketCount() > maxBucketCount) {
            return defaultMode;
        }

        // Prevent regression for low cardinality cases
        // Check both absolute bucket count and cardinality ratio
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
