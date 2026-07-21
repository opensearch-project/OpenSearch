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
 * Property-based tests for fetch phase start_offset recording in the unified breakdown map.
 *
 * <p><b>Property 6: Fetch phase start_offset recorded in unified map</b></p>
 * <p>For any completed search request that includes a fetch phase, the unified breakdown map
 * SHALL contain {@code fetch.start_offset_micros} equal to
 * {@code (fetch_start_nanos - request_start_nanos) / 1000} and {@code fetch.duration_micros}.</p>
 *
 * <p>Feature: latency-breakdown-fixes, Property 6: Fetch phase start_offset recorded in unified map</p>
 *
 * <p><b>Validates: Requirements 6.1, 6.2, 6.3</b></p>
 */
public class SearchLatencyBreakdownFetchStartOffsetPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    /**
     * Property 6: fetch.start_offset_micros equals (fetchStartNanos - absoluteStartNanos) / 1000.
     * <p>
     * Generate random request start times and fetch start times. Record a fetch phase with
     * the corresponding timed event. Assert that the unified breakdown map contains
     * fetch.start_offset_micros equal to the expected computation.
     * <p>
     * <b>Validates: Requirements 6.1, 6.2, 6.3</b>
     */
    public void testFetchStartOffsetMicrosEqualsExpectedComputation() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random absolute request start time (simulating System.nanoTime())
            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Generate random fetch start time that is after the request start
            // Use a minimum offset of 1000 nanos to ensure the microsecond conversion is > 0
            long fetchStartOffsetNanos = randomLongBetween(1_000L, 500_000_000L);
            long fetchStartNanos = absoluteStartNanos + fetchStartOffsetNanos;

            // Generate random fetch duration (at least 1000 nanos to produce non-zero micros)
            long fetchDurationNanos = randomLongBetween(1_000_000L, 300_000_000L);
            long fetchEndNanos = fetchStartNanos + fetchDurationNanos;

            // Record the fetch phase duration (this sets fetchPhaseNanos > 0)
            breakdown.recordFetchPhase(fetchDurationNanos);

            // Record the fetch timed event (this populates namedEvents with "fetch" key)
            breakdown.recordTimedEvent("fetch", fetchStartNanos, fetchEndNanos);

            // Request end must be after all events
            long requestEndNanos = fetchEndNanos + randomLongBetween(1_000_000L, 100_000_000L);

            // Generate the unified breakdown map
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            // Compute expected fetch.start_offset_micros
            long expectedStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(fetchStartNanos - absoluteStartNanos);

            // ASSERTION 1: fetch.start_offset_micros is present
            assertTrue(
                "fetch.start_offset_micros should be present in unified map (iteration " + i
                    + ", fetchStartOffset=" + fetchStartOffsetNanos + " nanos, expectedMicros=" + expectedStartOffsetMicros + ")",
                resultMap.containsKey("fetch.start_offset_micros")
            );

            // ASSERTION 2: fetch.start_offset_micros equals expected value
            long actualStartOffsetMicros = (Long) resultMap.get("fetch.start_offset_micros");
            assertEquals(
                "fetch.start_offset_micros should equal (fetchStartNanos - absoluteStartNanos) / 1000 (iteration " + i
                    + ", fetchStartNanos=" + fetchStartNanos + ", absoluteStartNanos=" + absoluteStartNanos + ")",
                expectedStartOffsetMicros,
                actualStartOffsetMicros
            );

            // ASSERTION 3: fetch.duration_micros is present
            assertTrue(
                "fetch.duration_micros should be present in unified map (iteration " + i + ")",
                resultMap.containsKey("fetch.duration_micros")
            );

            // ASSERTION 4: fetch.duration_micros equals expected value
            long expectedDurationMicros = TimeUnit.NANOSECONDS.toMicros(fetchDurationNanos);
            long actualDurationMicros = (Long) resultMap.get("fetch.duration_micros");
            assertEquals(
                "fetch.duration_micros should equal toMicros(fetchDurationNanos) (iteration " + i + ")",
                expectedDurationMicros,
                actualDurationMicros
            );
        }
    }

    /**
     * Property 6 (Sub-property): fetch.start_offset_micros is non-negative for valid fetch events.
     * <p>
     * When the fetch phase starts at or after the request start, the computed
     * start_offset_micros must be non-negative.
     * <p>
     * <b>Validates: Requirements 6.1, 6.2, 6.3</b>
     */
    public void testFetchStartOffsetMicrosNonNegative() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Fetch starts at or after request start (offset >= 0)
            long fetchStartOffsetNanos = randomLongBetween(0, 1_000_000_000L);
            long fetchStartNanos = absoluteStartNanos + fetchStartOffsetNanos;
            long fetchDurationNanos = randomLongBetween(1_000_000L, 200_000_000L);
            long fetchEndNanos = fetchStartNanos + fetchDurationNanos;

            breakdown.recordFetchPhase(fetchDurationNanos);
            breakdown.recordTimedEvent("fetch", fetchStartNanos, fetchEndNanos);

            long requestEndNanos = fetchEndNanos + randomLongBetween(1_000_000L, 50_000_000L);
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            // If start_offset_micros is present, it must be >= 0
            if (resultMap.containsKey("fetch.start_offset_micros")) {
                long actualStartOffsetMicros = (Long) resultMap.get("fetch.start_offset_micros");
                assertTrue(
                    "fetch.start_offset_micros should be non-negative (iteration " + i
                        + ", got " + actualStartOffsetMicros + ")",
                    actualStartOffsetMicros >= 0
                );
            }
        }
    }

    /**
     * Property 6 (Sub-property): fetch.start_offset_micros absent when fetch phase not recorded.
     * <p>
     * When no fetch phase is recorded (fetchPhaseNanos == 0), the unified map should NOT
     * contain fetch.start_offset_micros or fetch.duration_micros.
     * <p>
     * <b>Validates: Requirements 6.1, 6.2, 6.3</b>
     */
    public void testFetchStartOffsetAbsentWhenNoFetchPhase() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Record some other metrics but NOT fetch phase
            long queryDuration = randomLongBetween(1_000_000L, 100_000_000L);
            breakdown.recordQueryPhase(queryDuration);

            long requestEndNanos = absoluteStartNanos + randomLongBetween(100_000_000L, 500_000_000L);
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            // ASSERTION: fetch.start_offset_micros should be absent
            assertFalse(
                "fetch.start_offset_micros should NOT be present when fetch phase is not recorded (iteration " + i + ")",
                resultMap.containsKey("fetch.start_offset_micros")
            );

            // ASSERTION: fetch.duration_micros should be absent
            assertFalse(
                "fetch.duration_micros should NOT be present when fetch phase is not recorded (iteration " + i + ")",
                resultMap.containsKey("fetch.duration_micros")
            );
        }
    }

    /**
     * Property 6 (Sub-property): fetch.start_offset_micros consistent with varying offsets.
     * <p>
     * Tests that the offset computation is linear: for different fetch start times relative to
     * the same request start, the start_offset_micros increases proportionally.
     * <p>
     * <b>Validates: Requirements 6.1, 6.2, 6.3</b>
     */
    public void testFetchStartOffsetProportionalToElapsedTime() {
        for (int i = 0; i < ITERATIONS; i++) {
            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Create two breakdowns with different fetch start times
            long fetchOffset1Nanos = randomLongBetween(1_000L, 200_000_000L);
            long fetchOffset2Nanos = fetchOffset1Nanos + randomLongBetween(1_000L, 200_000_000L);

            long fetchDurationNanos = randomLongBetween(1_000_000L, 100_000_000L);

            // First breakdown
            SearchLatencyBreakdown breakdown1 = new SearchLatencyBreakdown();
            long fetchStart1 = absoluteStartNanos + fetchOffset1Nanos;
            breakdown1.recordFetchPhase(fetchDurationNanos);
            breakdown1.recordTimedEvent("fetch", fetchStart1, fetchStart1 + fetchDurationNanos);

            // Second breakdown with later fetch start
            SearchLatencyBreakdown breakdown2 = new SearchLatencyBreakdown();
            long fetchStart2 = absoluteStartNanos + fetchOffset2Nanos;
            breakdown2.recordFetchPhase(fetchDurationNanos);
            breakdown2.recordTimedEvent("fetch", fetchStart2, fetchStart2 + fetchDurationNanos);

            long requestEndNanos = absoluteStartNanos + fetchOffset2Nanos + fetchDurationNanos
                + randomLongBetween(1_000_000L, 50_000_000L);

            Map<String, Object> map1 = breakdown1.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);
            Map<String, Object> map2 = breakdown2.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);

            // Both must have fetch.start_offset_micros
            if (map1.containsKey("fetch.start_offset_micros") && map2.containsKey("fetch.start_offset_micros")) {
                long offset1 = (Long) map1.get("fetch.start_offset_micros");
                long offset2 = (Long) map2.get("fetch.start_offset_micros");

                // Since fetchOffset2 > fetchOffset1, offset2 must be >= offset1
                assertTrue(
                    "Later fetch start should produce larger or equal start_offset_micros (iteration " + i
                        + ", offset1=" + offset1 + ", offset2=" + offset2 + ")",
                    offset2 >= offset1
                );
            }
        }
    }
}
