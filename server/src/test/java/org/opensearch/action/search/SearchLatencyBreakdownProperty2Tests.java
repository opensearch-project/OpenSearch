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
 * Property-based tests for network roundtrip start_offset alignment in
 * {@link SearchLatencyBreakdown}.
 *
 * <p><b>Property 2: Network roundtrip start_offset aligns with corresponding phase</b></p>
 * <p>For any unified breakdown map containing a {@code network_roundtrip_query} entry,
 * its {@code start_offset_micros} SHALL equal {@code query.start_offset_micros}, and the map
 * SHALL contain {@code network_roundtrip_query._overlaps} with value {@code "query"}.
 * The same SHALL hold for {@code network_roundtrip_fetch} relative to {@code fetch}.</p>
 *
 * <p><b>Validates: Requirements 2.1, 2.2, 2.3</b></p>
 *
 * Feature: latency-breakdown-fixes, Property 2: Network roundtrip start_offset alignment
 */
public class SearchLatencyBreakdownProperty2Tests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    /**
     * Property 2: network_roundtrip_query.start_offset_micros aligns with query phase start_offset.
     * <p>
     * Generate random phase start offsets and network durations, record them into
     * SearchLatencyBreakdown, and verify that toUnifiedBreakdownMap produces aligned offsets
     * and correct _overlaps metadata.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    public void testNetworkRoundtripQueryStartOffsetAlignsWithQueryPhase() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random absolute start nanos
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            // Generate random query phase start offset relative to absoluteStart
            long queryStartOffset = randomLongBetween(1_000L, 200_000_000L);
            long queryStartNanos = absoluteStart + queryStartOffset;
            long queryDurationNanos = randomLongBetween(1_000_000L, 500_000_000L);
            long queryEndNanos = queryStartNanos + queryDurationNanos;

            // Record query phase timing
            breakdown.recordQueryPhase(queryDurationNanos);
            breakdown.recordTimedEvent("query", queryStartNanos, queryEndNanos);

            // Record network roundtrip for query - must be >= query duration (it wraps query execution)
            long networkQueryDurationNanos = queryDurationNanos + randomLongBetween(1_000_000L, 100_000_000L);
            breakdown.recordNetworkRoundtripQuery(networkQueryDurationNanos);

            // Request end must be after all events
            long requestEnd = queryEndNanos + randomLongBetween(1_000_000L, 100_000_000L);

            // Generate the unified breakdown map
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // Compute expected query start_offset_micros
            long expectedQueryStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(queryStartNanos - absoluteStart);

            // PROPERTY ASSERTION 1: network_roundtrip_query.start_offset_micros == query.start_offset_micros
            assertTrue(
                "network_roundtrip_query.start_offset_micros should be present in map (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query.start_offset_micros")
            );
            long actualNetworkQueryStartOffset = (Long) resultMap.get("network_roundtrip_query.start_offset_micros");
            assertEquals(
                "network_roundtrip_query.start_offset_micros should equal query start_offset_micros (iteration " + i + ")",
                expectedQueryStartOffsetMicros,
                actualNetworkQueryStartOffset
            );

            // PROPERTY ASSERTION 2: _overlaps metadata is present and equals "query"
            assertTrue(
                "network_roundtrip_query._overlaps should be present in map (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query._overlaps")
            );
            assertEquals(
                "network_roundtrip_query._overlaps should equal 'query' (iteration " + i + ")",
                "query",
                resultMap.get("network_roundtrip_query._overlaps")
            );

            // PROPERTY ASSERTION 3: duration_micros is present and correct
            assertTrue(
                "network_roundtrip_query.duration_micros should be present in map (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query.duration_micros")
            );
            long expectedDurationMicros = TimeUnit.NANOSECONDS.toMicros(networkQueryDurationNanos);
            long actualDurationMicros = (Long) resultMap.get("network_roundtrip_query.duration_micros");
            assertEquals(
                "network_roundtrip_query.duration_micros should match recorded duration (iteration " + i + ")",
                expectedDurationMicros,
                actualDurationMicros
            );
        }
    }

    /**
     * Property 2: network_roundtrip_fetch.start_offset_micros aligns with fetch phase start_offset.
     * <p>
     * Generate random fetch phase start offsets and network durations, record them into
     * SearchLatencyBreakdown, and verify alignment and _overlaps metadata for fetch.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    public void testNetworkRoundtripFetchStartOffsetAlignsWithFetchPhase() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random absolute start nanos
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            // Generate random fetch phase start offset relative to absoluteStart
            long fetchStartOffset = randomLongBetween(100_000_000L, 400_000_000L);
            long fetchStartNanos = absoluteStart + fetchStartOffset;
            long fetchDurationNanos = randomLongBetween(1_000_000L, 200_000_000L);
            long fetchEndNanos = fetchStartNanos + fetchDurationNanos;

            // Record fetch phase timing
            breakdown.recordFetchPhase(fetchDurationNanos);
            breakdown.recordTimedEvent("fetch", fetchStartNanos, fetchEndNanos);

            // Record network roundtrip for fetch - must be >= fetch duration
            long networkFetchDurationNanos = fetchDurationNanos + randomLongBetween(1_000_000L, 50_000_000L);
            breakdown.recordNetworkRoundtripFetch(networkFetchDurationNanos);

            // Request end must be after all events
            long requestEnd = fetchEndNanos + randomLongBetween(1_000_000L, 100_000_000L);

            // Generate the unified breakdown map
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // Compute expected fetch start_offset_micros
            long expectedFetchStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(fetchStartNanos - absoluteStart);

            // PROPERTY ASSERTION 1: network_roundtrip_fetch.start_offset_micros == fetch.start_offset_micros
            assertTrue(
                "network_roundtrip_fetch.start_offset_micros should be present in map (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_fetch.start_offset_micros")
            );
            long actualNetworkFetchStartOffset = (Long) resultMap.get("network_roundtrip_fetch.start_offset_micros");
            assertEquals(
                "network_roundtrip_fetch.start_offset_micros should equal fetch start_offset_micros (iteration " + i + ")",
                expectedFetchStartOffsetMicros,
                actualNetworkFetchStartOffset
            );

            // PROPERTY ASSERTION 2: _overlaps metadata is present and equals "fetch"
            assertTrue(
                "network_roundtrip_fetch._overlaps should be present in map (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_fetch._overlaps")
            );
            assertEquals(
                "network_roundtrip_fetch._overlaps should equal 'fetch' (iteration " + i + ")",
                "fetch",
                resultMap.get("network_roundtrip_fetch._overlaps")
            );

            // PROPERTY ASSERTION 3: duration_micros is present and correct
            assertTrue(
                "network_roundtrip_fetch.duration_micros should be present in map (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_fetch.duration_micros")
            );
            long expectedDurationMicros = TimeUnit.NANOSECONDS.toMicros(networkFetchDurationNanos);
            long actualDurationMicros = (Long) resultMap.get("network_roundtrip_fetch.duration_micros");
            assertEquals(
                "network_roundtrip_fetch.duration_micros should match recorded duration (iteration " + i + ")",
                expectedDurationMicros,
                actualDurationMicros
            );
        }
    }

    /**
     * Property 2: Both query and fetch network roundtrips align simultaneously.
     * <p>
     * When both query and fetch phases are recorded with network roundtrips,
     * both should independently align with their respective phase start_offsets.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    public void testBothNetworkRoundtripsAlignSimultaneously() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            // Query phase
            long queryStartOffset = randomLongBetween(1_000L, 100_000_000L);
            long queryStartNanos = absoluteStart + queryStartOffset;
            long queryDurationNanos = randomLongBetween(1_000_000L, 300_000_000L);
            long queryEndNanos = queryStartNanos + queryDurationNanos;

            // Fetch phase starts after query
            long fetchStartOffset = queryStartOffset + queryDurationNanos + randomLongBetween(1_000L, 50_000_000L);
            long fetchStartNanos = absoluteStart + fetchStartOffset;
            long fetchDurationNanos = randomLongBetween(1_000_000L, 200_000_000L);
            long fetchEndNanos = fetchStartNanos + fetchDurationNanos;

            // Record both phases
            breakdown.recordQueryPhase(queryDurationNanos);
            breakdown.recordTimedEvent("query", queryStartNanos, queryEndNanos);
            breakdown.recordFetchPhase(fetchDurationNanos);
            breakdown.recordTimedEvent("fetch", fetchStartNanos, fetchEndNanos);

            // Record network roundtrips for both
            long networkQueryNanos = queryDurationNanos + randomLongBetween(1_000_000L, 50_000_000L);
            long networkFetchNanos = fetchDurationNanos + randomLongBetween(1_000_000L, 30_000_000L);
            breakdown.recordNetworkRoundtripQuery(networkQueryNanos);
            breakdown.recordNetworkRoundtripFetch(networkFetchNanos);

            long requestEnd = fetchEndNanos + randomLongBetween(1_000_000L, 100_000_000L);

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // Compute expected offsets
            long expectedQueryStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(queryStartNanos - absoluteStart);
            long expectedFetchStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(fetchStartNanos - absoluteStart);

            // Verify query network alignment
            assertEquals(
                "network_roundtrip_query.start_offset_micros should equal query start_offset_micros (iteration " + i + ")",
                expectedQueryStartOffsetMicros,
                (long) (Long) resultMap.get("network_roundtrip_query.start_offset_micros")
            );
            assertEquals(
                "network_roundtrip_query._overlaps should equal 'query' (iteration " + i + ")",
                "query",
                resultMap.get("network_roundtrip_query._overlaps")
            );

            // Verify fetch network alignment
            assertEquals(
                "network_roundtrip_fetch.start_offset_micros should equal fetch start_offset_micros (iteration " + i + ")",
                expectedFetchStartOffsetMicros,
                (long) (Long) resultMap.get("network_roundtrip_fetch.start_offset_micros")
            );
            assertEquals(
                "network_roundtrip_fetch._overlaps should equal 'fetch' (iteration " + i + ")",
                "fetch",
                resultMap.get("network_roundtrip_fetch._overlaps")
            );

            // Verify the two start offsets are different (query comes before fetch)
            long networkQueryOffset = (Long) resultMap.get("network_roundtrip_query.start_offset_micros");
            long networkFetchOffset = (Long) resultMap.get("network_roundtrip_fetch.start_offset_micros");
            assertTrue(
                "network_roundtrip_query start_offset should be before network_roundtrip_fetch start_offset (iteration " + i + ")",
                networkQueryOffset < networkFetchOffset
            );
        }
    }

    /**
     * Property 2 edge case: When network roundtrip is recorded but no corresponding phase
     * named event exists, the start_offset defaults to 0.
     * <p>
     * This tests the fallback when namedEvents does not contain the "query" or "fetch" key.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    public void testNetworkRoundtripDefaultsToZeroOffsetWhenPhaseEventMissing() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(100_000_000L, 500_000_000L);

            // Record network roundtrip WITHOUT recording the corresponding phase timed event
            long networkQueryNanos = randomLongBetween(1_000_000L, 100_000_000L);
            breakdown.recordNetworkRoundtripQuery(networkQueryNanos);

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // start_offset_micros should default to 0 when "query" named event is missing
            assertTrue(
                "network_roundtrip_query.start_offset_micros should be present (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query.start_offset_micros")
            );
            assertEquals(
                "network_roundtrip_query.start_offset_micros should be 0 when phase event is missing (iteration " + i + ")",
                0L,
                (long) (Long) resultMap.get("network_roundtrip_query.start_offset_micros")
            );

            // _overlaps metadata should still be present
            assertEquals(
                "network_roundtrip_query._overlaps should still equal 'query' (iteration " + i + ")",
                "query",
                resultMap.get("network_roundtrip_query._overlaps")
            );
        }
    }

    /**
     * Property 2 edge case: When network roundtrip nanos is 0, no _overlaps entry should be
     * generated in the unified breakdown map.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    public void testNoOverlapsMetadataWhenNetworkRoundtripIsZero() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(100_000_000L, 500_000_000L);

            // Record query phase but DO NOT record any network roundtrip
            long queryStartNanos = absoluteStart + randomLongBetween(1_000L, 100_000_000L);
            long queryDurationNanos = randomLongBetween(1_000_000L, 200_000_000L);
            breakdown.recordQueryPhase(queryDurationNanos);
            breakdown.recordTimedEvent("query", queryStartNanos, queryStartNanos + queryDurationNanos);

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // When network roundtrip is 0, no _overlaps metadata should be generated
            assertFalse(
                "network_roundtrip_query._overlaps should NOT be present when roundtrip is 0 (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query._overlaps")
            );
            assertFalse(
                "network_roundtrip_query.start_offset_micros should NOT be present when roundtrip is 0 (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query.start_offset_micros")
            );
            assertFalse(
                "network_roundtrip_query.duration_micros should NOT be present when roundtrip is 0 (iteration " + i + ")",
                resultMap.containsKey("network_roundtrip_query.duration_micros")
            );
        }
    }
}
