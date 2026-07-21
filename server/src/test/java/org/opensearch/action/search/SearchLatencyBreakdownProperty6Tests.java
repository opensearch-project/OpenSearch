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
 * Property-based test for SearchLatencyBreakdown.
 * <p>
 * Feature: search-latency-breakdown, Property 6: Timed breakdown map start_offset computation
 * <p>
 * For any random named events with valid startNanos and endNanos, the timed breakdown map SHALL contain:
 * <ul>
 *   <li>start_offset_micros = TimeUnit.NANOSECONDS.toMicros(startNanos - absoluteStartNanos)</li>
 *   <li>duration_micros = TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos)</li>
 * </ul>
 * Events with zero duration_micros SHALL be omitted.
 * <p>
 * <b>Validates: Requirements 1.5, 2.2, 3.4, 11.3</b>
 */
public class SearchLatencyBreakdownProperty6Tests extends OpenSearchTestCase {

    /**
     * Property 6: Timed breakdown map computes start_offset_micros and duration_micros correctly.
     * <p>
     * For each iteration, generate random named events with valid startNanos and endNanos,
     * record them via recordTimedEvent, then verify toTimedBreakdownMap produces the correct
     * start_offset_micros and duration_micros for events whose duration converts to > 0 micros.
     */
    public void testTimedBreakdownMapStartOffsetAndDurationComputation() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            // Generate a random number of named events (1 to 10)
            int eventCount = randomIntBetween(1, 10);
            String[] names = new String[eventCount];
            long[] startNanosArr = new long[eventCount];
            long[] endNanosArr = new long[eventCount];

            for (int e = 0; e < eventCount; e++) {
                names[e] = "event_" + e + "_" + randomAlphaOfLength(5);
                // startNanos must be >= absoluteStart (event starts after request begins)
                startNanosArr[e] = absoluteStart + randomLongBetween(0, 500_000_000L);
                // endNanos must be >= startNanos (valid duration)
                endNanosArr[e] = startNanosArr[e] + randomLongBetween(0, 200_000_000L);

                breakdown.recordTimedEvent(names[e], startNanosArr[e], endNanosArr[e]);
            }

            // Generate the timed breakdown map
            Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

            // Verify each event
            for (int e = 0; e < eventCount; e++) {
                long expectedStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(startNanosArr[e] - absoluteStart);
                long durationNanos = endNanosArr[e] - startNanosArr[e];
                long expectedDurationMicros = TimeUnit.NANOSECONDS.toMicros(durationNanos);

                if (expectedDurationMicros == 0) {
                    // Zero-duration entries must be omitted
                    assertFalse(
                        "Event '" + names[e] + "' with zero duration_micros should be omitted from timed map, "
                            + "iteration " + i + ", durationNanos=" + durationNanos,
                        timedMap.containsKey(names[e])
                    );
                } else {
                    // Non-zero duration entries must be present with correct values
                    assertTrue(
                        "Event '" + names[e] + "' with non-zero duration_micros=" + expectedDurationMicros
                            + " should be present in timed map, iteration " + i,
                        timedMap.containsKey(names[e])
                    );

                    Map<String, Long> timing = timedMap.get(names[e]);

                    assertEquals(
                        "start_offset_micros mismatch for event '" + names[e] + "' in iteration " + i
                            + ". Expected toMicros(" + startNanosArr[e] + " - " + absoluteStart + ")",
                        expectedStartOffsetMicros,
                        timing.get("start_offset_micros").longValue()
                    );

                    assertEquals(
                        "duration_micros mismatch for event '" + names[e] + "' in iteration " + i
                            + ". Expected toMicros(" + endNanosArr[e] + " - " + startNanosArr[e] + ")",
                        expectedDurationMicros,
                        timing.get("duration_micros").longValue()
                    );
                }
            }
        }
    }

    /**
     * Property 6 (Sub-property): Events with sub-microsecond duration are omitted.
     * <p>
     * When the duration in nanoseconds is less than 1000 (below 1 microsecond),
     * TimeUnit.NANOSECONDS.toMicros() truncates to 0, so the event must be excluded.
     */
    public void testSubMicrosecondDurationEventsOmitted() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            // Record an event with sub-microsecond duration (1 to 999 nanos)
            String eventName = "sub_micro_event_" + randomAlphaOfLength(4);
            long startNanos = absoluteStart + randomLongBetween(1_000L, 100_000_000L);
            long durationNanos = randomLongBetween(1, 999);
            long endNanos = startNanos + durationNanos;

            breakdown.recordTimedEvent(eventName, startNanos, endNanos);

            Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

            // Sub-microsecond duration must be omitted
            assertFalse(
                "Event '" + eventName + "' with sub-microsecond duration (" + durationNanos
                    + " nanos) should be omitted from timed map, iteration " + i,
                timedMap.containsKey(eventName)
            );
        }
    }

    /**
     * Property 6 (Sub-property): Events with exactly zero duration are omitted.
     * <p>
     * When startNanos == endNanos (zero duration), the event must not appear in the map.
     */
    public void testZeroDurationEventsOmitted() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            String eventName = "zero_dur_event_" + randomAlphaOfLength(4);
            long startNanos = absoluteStart + randomLongBetween(0, 500_000_000L);
            // endNanos == startNanos → zero duration
            breakdown.recordTimedEvent(eventName, startNanos, startNanos);

            Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

            assertFalse(
                "Event '" + eventName + "' with zero duration should be omitted from timed map, iteration " + i,
                timedMap.containsKey(eventName)
            );
        }
    }

    /**
     * Property 6 (Sub-property): Mix of zero and non-zero duration events.
     * <p>
     * Only events with duration_micros > 0 should appear. Zero-duration events must
     * be absent and must not affect other entries.
     */
    public void testMixedZeroAndNonZeroDurationEvents() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            // Record a clearly non-zero event (>= 1000 nanos → >= 1 micros)
            String nonZeroEvent = "positive_event_" + randomAlphaOfLength(4);
            long nonZeroStart = absoluteStart + randomLongBetween(1_000L, 100_000_000L);
            long nonZeroDurationNanos = randomLongBetween(1_000L, 100_000_000L);
            long nonZeroEnd = nonZeroStart + nonZeroDurationNanos;
            breakdown.recordTimedEvent(nonZeroEvent, nonZeroStart, nonZeroEnd);

            // Record a zero-duration event
            String zeroEvent = "zero_event_" + randomAlphaOfLength(4);
            long zeroStart = absoluteStart + randomLongBetween(1_000L, 100_000_000L);
            breakdown.recordTimedEvent(zeroEvent, zeroStart, zeroStart);

            Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

            long expectedDurationMicros = TimeUnit.NANOSECONDS.toMicros(nonZeroDurationNanos);
            if (expectedDurationMicros > 0) {
                assertTrue(
                    "Non-zero event '" + nonZeroEvent + "' should be present in timed map, iteration " + i,
                    timedMap.containsKey(nonZeroEvent)
                );
            }

            assertFalse(
                "Zero-duration event '" + zeroEvent + "' should NOT be in timed map, iteration " + i,
                timedMap.containsKey(zeroEvent)
            );

            // All entries in the map must have positive duration_micros
            for (Map.Entry<String, Map<String, Long>> entry : timedMap.entrySet()) {
                assertTrue(
                    "Entry '" + entry.getKey() + "' has non-positive duration_micros " + entry.getValue().get("duration_micros")
                        + " in iteration " + i,
                    entry.getValue().get("duration_micros") > 0
                );
            }
        }
    }

    /**
     * Property 6 (Sub-property): start_offset_micros is always non-negative when event starts at or after absoluteStart.
     * <p>
     * For events where startNanos >= absoluteStartNanos, the start_offset_micros must be >= 0.
     */
    public void testStartOffsetMicrosNonNegativeForValidEvents() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);

            int eventCount = randomIntBetween(1, 8);
            for (int e = 0; e < eventCount; e++) {
                String name = "offset_event_" + e + "_" + randomAlphaOfLength(3);
                // Ensure startNanos >= absoluteStart
                long startNanos = absoluteStart + randomLongBetween(0, 1_000_000_000L);
                // Ensure non-zero duration in micros (>= 1000 nanos)
                long endNanos = startNanos + randomLongBetween(1_000L, 200_000_000L);
                breakdown.recordTimedEvent(name, startNanos, endNanos);
            }

            Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStart);

            for (Map.Entry<String, Map<String, Long>> entry : timedMap.entrySet()) {
                assertTrue(
                    "start_offset_micros for '" + entry.getKey() + "' should be non-negative, got "
                        + entry.getValue().get("start_offset_micros") + " in iteration " + i,
                    entry.getValue().get("start_offset_micros") >= 0
                );
            }
        }
    }
}
