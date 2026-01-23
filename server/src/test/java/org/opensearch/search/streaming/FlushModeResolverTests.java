/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for FlushModeResolver settings and decideFlushMode logic.
 */
public class FlushModeResolverTests extends OpenSearchTestCase {

    public void testSettingsDefaults() {
        assertEquals(100_000L, FlushModeResolver.STREAMING_MAX_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
        assertEquals(0.01, FlushModeResolver.STREAMING_MIN_CARDINALITY_RATIO.getDefault(Settings.EMPTY).doubleValue(), 0.001);
        assertEquals(1000L, FlushModeResolver.STREAMING_MIN_ESTIMATED_BUCKET_COUNT.getDefault(Settings.EMPTY).longValue());
    }

    public void testDecideFlushModeNonStreamable() {
        StreamingCostMetrics nonStreamable = StreamingCostMetrics.nonStreamable();
        FlushMode result = FlushModeResolver.decideFlushMode(nonStreamable, FlushMode.PER_SHARD, 100_000, 0.01, 1000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeStreamable() {
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 10, 5000, 5, 10000);
        FlushMode result = FlushModeResolver.decideFlushMode(streamable, FlushMode.PER_SHARD, 100_000, 0.01, 1000);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeTooManyBuckets() {
        StreamingCostMetrics highBuckets = new StreamingCostMetrics(true, 10, 200_000, 5, 10000);
        FlushMode result = FlushModeResolver.decideFlushMode(highBuckets, FlushMode.PER_SHARD, 100_000, 0.01, 1000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeTooFewBuckets() {
        StreamingCostMetrics lowBuckets = new StreamingCostMetrics(true, 10, 500, 5, 10000);
        FlushMode result = FlushModeResolver.decideFlushMode(lowBuckets, FlushMode.PER_SHARD, 100_000, 0.01, 1000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeLowCardinalityRatio() {
        // 500 buckets / 1_000_000 docs = 0.0005 ratio, below 0.01 threshold
        StreamingCostMetrics lowCardinality = new StreamingCostMetrics(true, 10, 5000, 5, 1_000_000);
        FlushMode result = FlushModeResolver.decideFlushMode(lowCardinality, FlushMode.PER_SHARD, 100_000, 0.01, 1000);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeNeutralMetrics() {
        StreamingCostMetrics neutral = StreamingCostMetrics.neutral();
        // Neutral metrics have very low values (1, 1, 1, 1) so should fall below thresholds
        FlushMode result = FlushModeResolver.decideFlushMode(neutral, FlushMode.PER_SHARD, 100_000, 0.01, 1000);
        assertEquals(FlushMode.PER_SHARD, result);
    }
}
