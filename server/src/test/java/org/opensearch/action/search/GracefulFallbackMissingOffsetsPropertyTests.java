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
 * Property-based tests for graceful fallback when data-node breakdown responses lack
 * absolute start_offset_micros values (older node or requestStartNanos == 0).
 * <p>
 * Feature: latency-breakdown-fixes, Property 12: Graceful fallback for missing absolute offsets
 * <p>
 * For any data-node breakdown response that lacks absolute start_offset_micros values
 * (older node or requestStartNanos == 0), the coordinator SHALL produce a valid unified
 * breakdown map using duration-only values with sequential positioning, without throwing errors.
 * <p>
 * <b>Validates: Requirements 13.1, 13.2</b>
 */
public class GracefulFallbackMissingOffsetsPropertyTests extends OpenSearchTestCase {

    private static final int MIN_ITERATIONS = 200;

    /**
     * Property 12a: No exceptions thrown when generating unified map with no named events.
     * <p>
     * When a SearchLatencyBreakdown has duration metrics recorded but no named events
     * (simulating an older data node that only reports duration), toUnifiedBreakdownMap()
     * SHALL produce a valid map without throwing any exceptions.
     * <p>
     * <b>Validates: Requirements 13.1, 13.2</b>
     */
    public void testNoExceptionWithMissingNamedEvents() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long requestEndNanos = absoluteStartNanos + randomLongBetween(50_000_000L, 2_000_000_000L);

            // Record random durations for various metrics WITHOUT recording named events
            // This simulates older data nodes that only provide duration data
            if (randomBoolean()) {
                breakdown.recordQueryPhase(randomLongBetween(1_000_000L, 500_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordFetchPhase(randomLongBetween(1_000_000L, 300_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordRescoreMax(randomLongBetween(1_000_000L, 100_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordSuggestMax(randomLongBetween(1_000_000L, 100_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordNetworkRoundtripQuery(randomLongBetween(1_000_000L, 200_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordNetworkRoundtripFetch(randomLongBetween(1_000_000L, 200_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordFetchStoredFields(randomLongBetween(1_000_000L, 100_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordGlobalOrdinalsLoading(randomLongBetween(1_000_000L, 50_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordQueryPreProcess(randomLongBetween(1_000_000L, 50_000_000L));
            }

            // This should NOT throw any exception regardless of missing named events
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            // The map should be non-null and a valid map
            assertNotNull(
                "Unified map should never be null even with no named events (iteration " + i + ")",
                resultMap
            );
        }
    }

    /**
     * Property 12b: All duration values in the unified map are non-negative.
     * <p>
     * For any combination of present/missing absolute offsets, every numeric value
     * in the unified breakdown map SHALL be non-negative (no negative durations or offsets).
     * <p>
     * <b>Validates: Requirements 13.1, 13.2</b>
     */
    public void testAllDurationValuesNonNegative() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Randomly decide whether to include absolute offsets (via named events) or not
            boolean includeAbsoluteOffsets = randomBoolean();

            // Record various metrics
            long queryDuration = randomLongBetween(1_000_000L, 500_000_000L);
            breakdown.recordQueryPhase(queryDuration);

            long fetchDuration = randomLongBetween(1_000_000L, 300_000_000L);
            breakdown.recordFetchPhase(fetchDuration);

            if (randomBoolean()) {
                breakdown.recordRescoreMax(randomLongBetween(1_000_000L, 100_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordSuggestMax(randomLongBetween(1_000_000L, 100_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordNetworkRoundtripQuery(randomLongBetween(1_000_000L, 200_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordNetworkRoundtripFetch(randomLongBetween(1_000_000L, 200_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordFetchStoredFields(randomLongBetween(1_000_000L, 100_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordQueryPreProcess(randomLongBetween(1_000_000L, 50_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordGlobalOrdinalsLoading(randomLongBetween(1_000_000L, 50_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordScriptCompilation(randomLongBetween(1_000_000L, 50_000_000L));
            }

            // Only include named events (absolute offsets) sometimes
            if (includeAbsoluteOffsets) {
                long queryStart = absoluteStartNanos + randomLongBetween(1_000L, 100_000_000L);
                breakdown.recordTimedEvent("query", queryStart, queryStart + queryDuration);

                long fetchStart = queryStart + queryDuration + randomLongBetween(1_000L, 50_000_000L);
                breakdown.recordTimedEvent("fetch", fetchStart, fetchStart + fetchDuration);
            }
            // If includeAbsoluteOffsets is false, no named events → simulates missing offsets

            long requestEndNanos = absoluteStartNanos + randomLongBetween(500_000_000L, 2_000_000_000L);
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            assertNotNull("Unified map should not be null (iteration " + i + ")", resultMap);

            // Verify all numeric values are non-negative
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Long) {
                    long longValue = (Long) value;
                    assertTrue(
                        "Value for key '" + entry.getKey() + "' should be non-negative but was "
                            + longValue + " (iteration " + i + ", includeAbsoluteOffsets=" + includeAbsoluteOffsets + ")",
                        longValue >= 0
                    );
                }
            }
        }
    }

    /**
     * Property 12c: Network roundtrip entries use fallback start_offset of 0 when phase events are missing.
     * <p>
     * When network_roundtrip_query is recorded but no "query" named event exists (older data node),
     * the network roundtrip start_offset_micros SHALL fall back to 0 without error. Same for fetch.
     * <p>
     * <b>Validates: Requirements 13.1, 13.2</b>
     */
    public void testNetworkRoundtripFallbackStartOffsetWhenNoPhaseEvent() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long requestEndNanos = absoluteStartNanos + randomLongBetween(500_000_000L, 2_000_000_000L);

            // Record network roundtrip metrics WITHOUT recording any named phase events
            long networkQueryDuration = randomLongBetween(1_000_000L, 200_000_000L);
            breakdown.recordNetworkRoundtripQuery(networkQueryDuration);

            long networkFetchDuration = randomLongBetween(1_000_000L, 200_000_000L);
            breakdown.recordNetworkRoundtripFetch(networkFetchDuration);

            // Generate unified map — no "query" or "fetch" named events exist
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            assertNotNull("Unified map should not be null (iteration " + i + ")", resultMap);

            // Verify network_roundtrip_query entries are still present with fallback offsets
            if (TimeUnit.NANOSECONDS.toMillis(networkQueryDuration) > 0) {
                assertTrue(
                    "network_roundtrip_query should be present (iteration " + i + ")",
                    resultMap.containsKey("network_roundtrip_query")
                );
                assertTrue(
                    "network_roundtrip_query.start_offset_micros should be present (iteration " + i + ")",
                    resultMap.containsKey("network_roundtrip_query.start_offset_micros")
                );
                // Fallback: start_offset_micros is 0 when no "query" named event exists
                long queryStartOffset = (Long) resultMap.get("network_roundtrip_query.start_offset_micros");
                assertEquals(
                    "network_roundtrip_query.start_offset_micros should fall back to 0 "
                        + "when no query named event exists (iteration " + i + ")",
                    0L,
                    queryStartOffset
                );
                // _overlaps metadata should NOT be present (Requirement 3.6: no overlapping bar metadata)
                assertNull(
                    "network_roundtrip_query._overlaps should NOT be present (iteration " + i + ")",
                    resultMap.get("network_roundtrip_query._overlaps")
                );
            }

            // Same verification for fetch
            if (TimeUnit.NANOSECONDS.toMillis(networkFetchDuration) > 0) {
                assertTrue(
                    "network_roundtrip_fetch should be present (iteration " + i + ")",
                    resultMap.containsKey("network_roundtrip_fetch")
                );
                assertTrue(
                    "network_roundtrip_fetch.start_offset_micros should be present (iteration " + i + ")",
                    resultMap.containsKey("network_roundtrip_fetch.start_offset_micros")
                );
                long fetchStartOffset = (Long) resultMap.get("network_roundtrip_fetch.start_offset_micros");
                assertEquals(
                    "network_roundtrip_fetch.start_offset_micros should fall back to 0 "
                        + "when no fetch named event exists (iteration " + i + ")",
                    0L,
                    fetchStartOffset
                );
                // _overlaps metadata should NOT be present (Requirement 3.6: no overlapping bar metadata)
                assertNull(
                    "network_roundtrip_fetch._overlaps should NOT be present (iteration " + i + ")",
                    resultMap.get("network_roundtrip_fetch._overlaps")
                );
            }
        }
    }

    /**
     * Property 12d: Unified map is valid with mixed present/missing offsets across different metrics.
     * <p>
     * Generate random breakdown instances where SOME metrics have named events (absolute offsets)
     * and OTHERS do not. The unified map SHALL still be valid: non-null, all Long values non-negative,
     * and expected base metric keys present based on which durations were recorded.
     * <p>
     * <b>Validates: Requirements 13.1, 13.2</b>
     */
    public void testUnifiedMapValidWithMixedPresentAndMissingOffsets() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Record query phase (always)
            long queryDuration = randomLongBetween(1_000_000L, 500_000_000L);
            breakdown.recordQueryPhase(queryDuration);

            // Optionally record query named event
            boolean hasQueryOffset = randomBoolean();
            long queryStartNanos = absoluteStartNanos + randomLongBetween(1_000L, 50_000_000L);
            if (hasQueryOffset) {
                breakdown.recordTimedEvent("query", queryStartNanos, queryStartNanos + queryDuration);
            }

            // Record fetch phase (always)
            long fetchDuration = randomLongBetween(1_000_000L, 300_000_000L);
            breakdown.recordFetchPhase(fetchDuration);

            // Optionally record fetch named event
            boolean hasFetchOffset = randomBoolean();
            long fetchStartNanos = queryStartNanos + queryDuration + randomLongBetween(1_000L, 50_000_000L);
            if (hasFetchOffset) {
                breakdown.recordTimedEvent("fetch", fetchStartNanos, fetchStartNanos + fetchDuration);
            }

            // Record network roundtrips (both present)
            long netQueryDuration = randomLongBetween(1_000_000L, 200_000_000L);
            breakdown.recordNetworkRoundtripQuery(netQueryDuration);
            long netFetchDuration = randomLongBetween(1_000_000L, 200_000_000L);
            breakdown.recordNetworkRoundtripFetch(netFetchDuration);

            // Optionally record query_pre_process with partial sub-component offsets
            if (randomBoolean()) {
                long ppDuration = randomLongBetween(1_000_000L, 50_000_000L);
                breakdown.recordQueryPreProcess(ppDuration);
                if (randomBoolean()) {
                    long ppStart = queryStartNanos + randomLongBetween(0, 10_000_000L);
                    breakdown.recordTimedEvent("query_pre_process", ppStart, ppStart + ppDuration);
                }
                if (randomBoolean()) {
                    breakdown.recordGlobalOrdinalsLoading(randomLongBetween(1_000_000L, ppDuration));
                }
            }

            long requestEndNanos = absoluteStartNanos + randomLongBetween(1_000_000_000L, 3_000_000_000L);

            // This should NEVER throw an exception regardless of offset data availability
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            // Basic validity checks
            assertNotNull("Unified map should not be null (iteration " + i + ")", resultMap);
            assertFalse("Unified map should not be empty (iteration " + i + ")", resultMap.isEmpty());

            // Query and fetch base keys should be present (durations are > 1ms)
            assertTrue(
                "query key should be present (iteration " + i + ", queryDuration=" + queryDuration + ")",
                resultMap.containsKey("query")
            );
            assertTrue(
                "fetch key should be present (iteration " + i + ", fetchDuration=" + fetchDuration + ")",
                resultMap.containsKey("fetch")
            );

            // All Long values must be non-negative
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                if (entry.getValue() instanceof Long) {
                    assertTrue(
                        "Key '" + entry.getKey() + "' has negative value " + entry.getValue()
                            + " (iteration " + i + ", hasQueryOffset=" + hasQueryOffset
                            + ", hasFetchOffset=" + hasFetchOffset + ")",
                        (Long) entry.getValue() >= 0
                    );
                }
            }
        }
    }

    /**
     * Property 12e: Completely empty breakdown produces a valid (possibly empty) unified map.
     * <p>
     * A SearchLatencyBreakdown with no metrics recorded at all (simulating a response from
     * a very old node or a completely failed shard) SHALL produce a valid non-null map
     * without throwing errors.
     * <p>
     * <b>Validates: Requirements 13.1, 13.2</b>
     */
    public void testCompletelyEmptyBreakdownProducesValidMap() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Don't record ANY metrics — simulating completely missing data
            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long requestEndNanos = absoluteStartNanos + randomLongBetween(50_000_000L, 2_000_000_000L);

            // Should not throw
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            assertNotNull(
                "Unified map should not be null for empty breakdown (iteration " + i + ")",
                resultMap
            );

            // All values (if any) must be non-negative
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                if (entry.getValue() instanceof Long) {
                    assertTrue(
                        "Key '" + entry.getKey() + "' should be non-negative in empty breakdown (iteration " + i + ")",
                        (Long) entry.getValue() >= 0
                    );
                }
            }
        }
    }

    /**
     * Property 12f: Fetch internals produce duration-only output when no named events exist.
     * <p>
     * When fetch phase internals (stored_fields, source_loading, etc.) are recorded but
     * no named event provides their absolute offset, the unified map SHALL still contain
     * the duration values without error. The metrics appear as duration-in-millis only.
     * <p>
     * <b>Validates: Requirements 13.1, 13.2</b>
     */
    public void testFetchInternalsDurationOnlyWithoutNamedEvents() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Record fetch phase with duration but NO named event for absolute offset
            long fetchDuration = randomLongBetween(5_000_000L, 300_000_000L);
            breakdown.recordFetchPhase(fetchDuration);

            // Record fetch internals — these are duration-only (no start_offset)
            long storedFieldsDuration = randomLongBetween(1_000_000L, fetchDuration);
            breakdown.recordFetchStoredFields(storedFieldsDuration);

            if (randomBoolean()) {
                breakdown.recordFetchSourceLoading(randomLongBetween(1_000_000L, fetchDuration));
            }
            if (randomBoolean()) {
                breakdown.recordFetchHighlighting(randomLongBetween(1_000_000L, fetchDuration));
            }
            if (randomBoolean()) {
                breakdown.recordFetchScriptFields(randomLongBetween(1_000_000L, fetchDuration));
            }
            if (randomBoolean()) {
                breakdown.recordFetchInnerHits(randomLongBetween(1_000_000L, fetchDuration));
            }

            long requestEndNanos = absoluteStartNanos + randomLongBetween(500_000_000L, 2_000_000_000L);

            // Should not throw
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            assertNotNull("Unified map should not be null (iteration " + i + ")", resultMap);

            // fetch key should be present
            assertTrue(
                "fetch key should be present (iteration " + i + ")",
                resultMap.containsKey("fetch")
            );

            // fetch_stored_fields should be present as a duration-only value
            long expectedStoredFieldsMillis = TimeUnit.NANOSECONDS.toMillis(storedFieldsDuration);
            if (expectedStoredFieldsMillis > 0) {
                assertTrue(
                    "fetch_stored_fields should be present (iteration " + i + ")",
                    resultMap.containsKey("fetch_stored_fields")
                );
                long actualMillis = (Long) resultMap.get("fetch_stored_fields");
                assertTrue(
                    "fetch_stored_fields should be non-negative (iteration " + i + ")",
                    actualMillis >= 0
                );
            }

            // fetch.start_offset_micros should NOT be present (no named event for "fetch")
            assertFalse(
                "fetch.start_offset_micros should NOT be present without named event (iteration " + i + ")",
                resultMap.containsKey("fetch.start_offset_micros")
            );
        }
    }
}
