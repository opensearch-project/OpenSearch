/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for rescore/suggest conditional presence and max-aggregation
 * in {@link SearchLatencyBreakdown}.
 * <p>
 * Verifies:
 * <ul>
 *   <li>Rescore/suggest metrics appear in the unified breakdown map when configured (recorded with positive values)</li>
 *   <li>Rescore/suggest metrics are absent from the unified breakdown map when not configured (no recording)</li>
 *   <li>Max-aggregation across multiple shards selects the maximum value for rescore/suggest</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 3.1, 3.2, 4.1, 4.2, 3.3, 4.3</b>
 */
public class SearchLatencyBreakdownRescoreSuggestTests extends OpenSearchTestCase {

    private static final long ABSOLUTE_START = 1_000_000_000L;
    private static final long REQUEST_END = ABSOLUTE_START + 500_000_000L;

    // ========== Conditional Presence Tests ==========

    /**
     * Tests that the "rescore" metric appears in the unified breakdown map
     * when rescore timing is recorded with a value that converts to positive milliseconds.
     * <p>
     * Validates: Requirement 3.1 - rescore metric present when configured
     */
    public void testRescoreAppearsInBreakdownWhenConfigured() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Record rescore timing (5ms in nanos)
        long rescoreNanos = 5_000_000L;
        breakdown.recordRescoreMax(rescoreNanos);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertTrue("rescore should be present in breakdown when recorded", map.containsKey("rescore"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(rescoreNanos), ((Long) map.get("rescore")).longValue());
    }

    /**
     * Tests that the "suggest" metric appears in the unified breakdown map
     * when suggest timing is recorded with a value that converts to positive milliseconds.
     * <p>
     * Validates: Requirement 4.1 - suggest metric present when configured
     */
    public void testSuggestAppearsInBreakdownWhenConfigured() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Record suggest timing (3ms in nanos)
        long suggestNanos = 3_000_000L;
        breakdown.recordSuggestMax(suggestNanos);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertTrue("suggest should be present in breakdown when recorded", map.containsKey("suggest"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(suggestNanos), ((Long) map.get("suggest")).longValue());
    }

    /**
     * Tests that both rescore and suggest metrics appear simultaneously when both are configured.
     * <p>
     * Validates: Requirements 3.1, 4.1
     */
    public void testBothRescoreAndSuggestAppearWhenConfigured() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long rescoreNanos = 10_000_000L; // 10ms
        long suggestNanos = 7_000_000L;  // 7ms
        breakdown.recordRescoreMax(rescoreNanos);
        breakdown.recordSuggestMax(suggestNanos);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertTrue("rescore should be present", map.containsKey("rescore"));
        assertTrue("suggest should be present", map.containsKey("suggest"));
        assertEquals(10L, ((Long) map.get("rescore")).longValue());
        assertEquals(7L, ((Long) map.get("suggest")).longValue());
    }

    // ========== Absence Tests ==========

    /**
     * Tests that the "rescore" metric is absent from the unified breakdown map
     * when no rescore timing is recorded (rescore not configured).
     * <p>
     * Validates: Requirement 3.2 - rescore metric omitted when not configured
     */
    public void testRescoreAbsentWhenNotConfigured() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Do NOT record any rescore timing - simulates rescore not configured
        // Record some other metric to ensure the map is not completely empty
        breakdown.recordQueryPhase(50_000_000L);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("rescore should be absent when not recorded", map.containsKey("rescore"));
    }

    /**
     * Tests that the "suggest" metric is absent from the unified breakdown map
     * when no suggest timing is recorded (suggest not configured).
     * <p>
     * Validates: Requirement 4.2 - suggest metric omitted when not configured
     */
    public void testSuggestAbsentWhenNotConfigured() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Do NOT record any suggest timing - simulates suggest not configured
        // Record some other metric to ensure the map is not completely empty
        breakdown.recordQueryPhase(50_000_000L);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("suggest should be absent when not recorded", map.containsKey("suggest"));
    }

    /**
     * Tests that recording zero nanoseconds for rescore does not produce an entry
     * in the unified breakdown map (since 0 nanos converts to 0 millis, omitted by putIfPositive).
     * <p>
     * Validates: Requirement 3.2 - zero values treated as "not configured"
     */
    public void testRescoreAbsentWhenRecordedAsZero() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        breakdown.recordRescoreMax(0);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("rescore should be absent when recorded as 0", map.containsKey("rescore"));
    }

    /**
     * Tests that recording zero nanoseconds for suggest does not produce an entry
     * in the unified breakdown map.
     * <p>
     * Validates: Requirement 4.2 - zero values treated as "not configured"
     */
    public void testSuggestAbsentWhenRecordedAsZero() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        breakdown.recordSuggestMax(0);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("suggest should be absent when recorded as 0", map.containsKey("suggest"));
    }

    /**
     * Tests that sub-millisecond rescore values (e.g., 500_000 nanos = 0.5ms) are omitted
     * from the unified map because putIfPositive only includes values that convert to at least 1ms.
     * <p>
     * Validates: Requirement 3.2 - sub-millisecond values treated as negligible
     */
    public void testRescoreAbsentWhenSubMillisecond() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // 500 microseconds = 500_000 nanos, which is 0ms when converted
        breakdown.recordRescoreMax(500_000L);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("rescore should be absent for sub-millisecond values", map.containsKey("rescore"));
    }

    /**
     * Tests that sub-millisecond suggest values are omitted from the unified map.
     * <p>
     * Validates: Requirement 4.2 - sub-millisecond values treated as negligible
     */
    public void testSuggestAbsentWhenSubMillisecond() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // 999_999 nanos = 0ms when converted to millis (truncated)
        breakdown.recordSuggestMax(999_999L);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("suggest should be absent for sub-millisecond values", map.containsKey("suggest"));
    }

    // ========== Max-Aggregation Tests ==========

    /**
     * Tests that recordRescoreMax uses max-aggregation: when multiple shard values
     * are recorded, only the maximum value is retained.
     * <p>
     * Validates: Requirement 3.3 - coordinator reports maximum rescore duration across all shards
     */
    public void testRescoreMaxAggregationAcrossMultipleShards() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulate 3 shards returning different rescore durations
        long shard1Rescore = 3_000_000L;  // 3ms
        long shard2Rescore = 8_000_000L;  // 8ms (max)
        long shard3Rescore = 5_000_000L;  // 5ms

        breakdown.recordRescoreMax(shard1Rescore);
        breakdown.recordRescoreMax(shard2Rescore);
        breakdown.recordRescoreMax(shard3Rescore);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertTrue("rescore should be present", map.containsKey("rescore"));
        assertEquals("rescore should report maximum across shards",
            TimeUnit.NANOSECONDS.toMillis(shard2Rescore), ((Long) map.get("rescore")).longValue());
    }

    /**
     * Tests that recordSuggestMax uses max-aggregation: when multiple shard values
     * are recorded, only the maximum value is retained.
     * <p>
     * Validates: Requirement 4.3 - coordinator reports maximum suggest duration across all shards
     */
    public void testSuggestMaxAggregationAcrossMultipleShards() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulate 4 shards returning different suggest durations
        long shard1Suggest = 2_000_000L;  // 2ms
        long shard2Suggest = 1_000_000L;  // 1ms
        long shard3Suggest = 6_000_000L;  // 6ms (max)
        long shard4Suggest = 4_000_000L;  // 4ms

        breakdown.recordSuggestMax(shard1Suggest);
        breakdown.recordSuggestMax(shard2Suggest);
        breakdown.recordSuggestMax(shard3Suggest);
        breakdown.recordSuggestMax(shard4Suggest);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertTrue("suggest should be present", map.containsKey("suggest"));
        assertEquals("suggest should report maximum across shards",
            TimeUnit.NANOSECONDS.toMillis(shard3Suggest), ((Long) map.get("suggest")).longValue());
    }

    /**
     * Tests max-aggregation when the first recorded shard value is the largest.
     * Ensures the max is correct regardless of recording order.
     * <p>
     * Validates: Requirements 3.3, 4.3
     */
    public void testMaxAggregationWithFirstShardAsMax() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // First shard is the max
        breakdown.recordRescoreMax(12_000_000L); // 12ms (max)
        breakdown.recordRescoreMax(5_000_000L);  // 5ms
        breakdown.recordRescoreMax(7_000_000L);  // 7ms

        breakdown.recordSuggestMax(9_000_000L);  // 9ms (max)
        breakdown.recordSuggestMax(3_000_000L);  // 3ms
        breakdown.recordSuggestMax(6_000_000L);  // 6ms

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertEquals(12L, ((Long) map.get("rescore")).longValue());
        assertEquals(9L, ((Long) map.get("suggest")).longValue());
    }

    /**
     * Tests max-aggregation with a single shard - the single value should be reported.
     * <p>
     * Validates: Requirements 3.3, 4.3
     */
    public void testMaxAggregationWithSingleShard() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long singleRescoreNanos = 4_000_000L; // 4ms
        long singleSuggestNanos = 2_000_000L; // 2ms

        breakdown.recordRescoreMax(singleRescoreNanos);
        breakdown.recordSuggestMax(singleSuggestNanos);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertEquals(4L, ((Long) map.get("rescore")).longValue());
        assertEquals(2L, ((Long) map.get("suggest")).longValue());
    }

    /**
     * Tests max-aggregation with many shards (simulating a large cluster).
     * <p>
     * Validates: Requirements 3.3, 4.3
     */
    public void testMaxAggregationWithManyShards() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        int shardCount = 50;
        long expectedMaxRescore = 0;
        long expectedMaxSuggest = 0;

        for (int i = 0; i < shardCount; i++) {
            // Generate random durations between 1ms and 100ms in nanos
            long rescoreNanos = randomLongBetween(1_000_000L, 100_000_000L);
            long suggestNanos = randomLongBetween(1_000_000L, 100_000_000L);

            breakdown.recordRescoreMax(rescoreNanos);
            breakdown.recordSuggestMax(suggestNanos);

            expectedMaxRescore = Math.max(expectedMaxRescore, rescoreNanos);
            expectedMaxSuggest = Math.max(expectedMaxSuggest, suggestNanos);
        }

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertEquals("rescore should be max across all shards",
            TimeUnit.NANOSECONDS.toMillis(expectedMaxRescore), ((Long) map.get("rescore")).longValue());
        assertEquals("suggest should be max across all shards",
            TimeUnit.NANOSECONDS.toMillis(expectedMaxSuggest), ((Long) map.get("suggest")).longValue());
    }

    /**
     * Tests that max-aggregation with all shards reporting sub-millisecond values
     * results in the metric being absent (since max is still less than 1ms).
     * <p>
     * Validates: Requirements 3.2, 3.3, 4.2, 4.3
     */
    public void testMaxAggregationAllSubMillisecond() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // All shards report sub-millisecond values
        breakdown.recordRescoreMax(100_000L);  // 0.1ms
        breakdown.recordRescoreMax(500_000L);  // 0.5ms
        breakdown.recordRescoreMax(900_000L);  // 0.9ms (max, still less than 1ms)

        breakdown.recordSuggestMax(200_000L);
        breakdown.recordSuggestMax(800_000L);  // max, still less than 1ms

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(ABSOLUTE_START, REQUEST_END);

        assertFalse("rescore should be absent when max across shards is sub-millisecond",
            map.containsKey("rescore"));
        assertFalse("suggest should be absent when max across shards is sub-millisecond",
            map.containsKey("suggest"));
    }
}
