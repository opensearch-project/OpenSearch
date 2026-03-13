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
        assertEquals(1000, FlushModeResolver.STREAMING_AGGREGATION_MIN_SEGMENT_SIZE_SETTING.getDefault(Settings.EMPTY).intValue());
    }

    public void testDecideFlushMode_NonStreamable() {
        StreamingCostMetrics nonStreamable = StreamingCostMetrics.nonStreamable();
        FlushMode result = FlushModeResolver.decideFlushMode(nonStreamable, FlushMode.PER_SHARD, 100_000, 0.0, 0L);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushMode_StreamableWithinLimits() {
        StreamingCostMetrics neutral = StreamingCostMetrics.neutral();
        FlushMode result = FlushModeResolver.decideFlushMode(neutral, FlushMode.PER_SHARD, 100_000, 0.0, 0L);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushMode_StreamableExceedsLimits() {
        // Streamable but small topN (should still check bucket count logic if
        // implemented)
        // Here we just check basic pass-through for now or mock high estimated cost
        StreamingCostMetrics largeCost = new StreamingCostMetrics(true, 1000, 200_000, 10, 1_000_000);
        FlushMode result = FlushModeResolver.decideFlushMode(largeCost, FlushMode.PER_SHARD, 100_000, 0.0, 0L);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushMode_StreamableSmallLoad() {
        StreamingCostMetrics smallTopN = new StreamingCostMetrics(true, 100);
        FlushMode result = FlushModeResolver.decideFlushMode(smallTopN, FlushMode.PER_SHARD, 100_000, 0.0, 0L);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

    public void testDecideFlushModeTopNExceedsMax() {
        // topN=200_000 > maxBucketCount=100_000, should not stream
        StreamingCostMetrics highTopN = new StreamingCostMetrics(true, 200_000, 200_000, 10, 1000);
        FlushMode result = FlushModeResolver.decideFlushMode(highTopN, FlushMode.PER_SHARD, 100_000, 0.0, 0L);
        assertEquals(FlushMode.PER_SHARD, result);
    }

    public void testDecideFlushModeTopNExactlyAtMax() {
        // topN=100_000 == maxBucketCount=100_000, should stream (<=)
        StreamingCostMetrics exactMatch = new StreamingCostMetrics(true, 100_000);
        FlushMode result = FlushModeResolver.decideFlushMode(exactMatch, FlushMode.PER_SHARD, 100_000, 0.0, 0L);
        assertEquals(FlushMode.PER_SEGMENT, result);
    }

}
