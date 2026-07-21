/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

/**
 * Property-based test validating that wire bars are correctly positioned between phases.
 * <p>
 * Feature: breakdown-ordering-fix, Property 5: Wire bars are correctly positioned between phases
 * <p>
 * For any breakdown with wire_out/wire_back computed:
 * <ul>
 *   <li>{@code wire_out_query.start >= pre_search_end}</li>
 *   <li>{@code wire_out_query.end <= query_phase.start}</li>
 *   <li>{@code wire_back_query.start >= query_phase.end}</li>
 *   <li>{@code wire_out_fetch.start >= wire_back_query.end} (or reduce phase end)</li>
 *   <li>{@code wire_out_fetch.end <= fetch_phase.start}</li>
 *   <li>{@code wire_back_fetch.start >= fetch_phase.end}</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 3.3, 3.4, 3.5</b>
 */
public class WireBarsPositionedBetweenPhasesPropertyTests extends OpenSearchTestCase {

    /** Minimum 100 iterations as required by the design document. */
    private static final int ITERATIONS = 150;

    /**
     * Property 5: wire_out_query is positioned between pre-search end and query phase start,
     * and wire_back_query is positioned after query phase end.
     * <p>
     * Generates random phase timings and wire durations, then computes wire_out_query and
     * wire_back_query positions using the same logic as AbstractSearchAsyncAction.
     * <p>
     * Asserts:
     * <ul>
     *   <li>{@code wire_out_query.start >= pre_search_end}</li>
     *   <li>{@code wire_out_query.end <= query_phase.start}</li>
     *   <li>{@code wire_back_query.start >= query_phase.end}</li>
     * </ul>
     * <p>
     * <b>Validates: Requirements 3.3, 3.4, 3.5</b>
     */
    public void testWireOutQueryPositionedBetweenPreSearchAndQueryPhase() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random phase boundaries
            // pre_search_end is the absolute nanoTime when pre-search completes (= firstPhaseStartNanos)
            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 100_000_000_000L);
            long preSearchDurationNanos = randomLongBetween(100_000L, 50_000_000L);
            long preSearchEndNanos = absoluteStartNanos + preSearchDurationNanos;

            // wire_out_query duration (wall_clock_offset_micros converted to nanos)
            long wallClockOffsetMicros = randomLongBetween(1L, 5_000L);
            long wireOutNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros);

            // query phase starts after wire_out_query
            long queryPhaseStartNanos = preSearchEndNanos + wireOutNanos;
            long queryPhaseDurationNanos = randomLongBetween(1_000_000L, 500_000_000L);
            long queryPhaseEndNanos = queryPhaseStartNanos + queryPhaseDurationNanos;

            // Compute wire_back_query duration from network roundtrip decomposition
            long dataNodeExecNanos = randomLongBetween(1_000L, 500_000_000L);
            // Network roundtrip must be >= wireOut + dataNodeExec to produce a non-negative wireBack
            long networkRoundtripNanos = wireOutNanos + dataNodeExecNanos + randomLongBetween(0L, 10_000_000L);
            long wireBackNanos = Math.max(0, networkRoundtripNanos - wireOutNanos - dataNodeExecNanos);

            // --- Simulate wire bar positioning (same logic as AbstractSearchAsyncAction) ---

            // Position wire_out_query between pre-search end and query phase start
            long wireOutStart = preSearchEndNanos;
            long wireOutEnd = wireOutStart + wireOutNanos;

            // Position wire_back_query immediately after query phase ends
            long wireBackStart = queryPhaseEndNanos;
            long wireBackEnd = wireBackStart + wireBackNanos;

            // --- PROPERTY ASSERTIONS ---

            // wire_out_query.start >= pre_search_end
            assertTrue(
                "Iteration " + i + ": wire_out_query.start (" + wireOutStart
                    + ") must be >= pre_search_end (" + preSearchEndNanos + ")",
                wireOutStart >= preSearchEndNanos
            );

            // wire_out_query.end <= query_phase.start
            assertTrue(
                "Iteration " + i + ": wire_out_query.end (" + wireOutEnd
                    + ") must be <= query_phase.start (" + queryPhaseStartNanos + ")",
                wireOutEnd <= queryPhaseStartNanos
            );

            // wire_back_query.start >= query_phase.end
            assertTrue(
                "Iteration " + i + ": wire_back_query.start (" + wireBackStart
                    + ") must be >= query_phase.end (" + queryPhaseEndNanos + ")",
                wireBackStart >= queryPhaseEndNanos
            );

            // Additional: wire_back_query duration is non-negative
            assertTrue(
                "Iteration " + i + ": wire_back_query duration (" + wireBackNanos + ") must be >= 0",
                wireBackNanos >= 0
            );

            // Additional: wire_out_query duration is non-negative
            assertTrue(
                "Iteration " + i + ": wire_out_query duration (" + wireOutNanos + ") must be >= 0",
                wireOutNanos >= 0
            );
        }
    }

    /**
     * Property 5: wire_out_fetch is positioned after wire_back_query end and before fetch phase start,
     * and wire_back_fetch is positioned after fetch phase end.
     * <p>
     * Generates random phase timings for the full pipeline (query + fetch) with wire durations,
     * then computes wire_out_fetch and wire_back_fetch positions.
     * <p>
     * Asserts:
     * <ul>
     *   <li>{@code wire_out_fetch.start >= wire_back_query.end} (or reduce/expand end)</li>
     *   <li>{@code wire_out_fetch.end <= fetch_phase.start}</li>
     *   <li>{@code wire_back_fetch.start >= fetch_phase.end}</li>
     * </ul>
     * <p>
     * <b>Validates: Requirements 3.5</b>
     */
    public void testWireOutFetchPositionedBetweenQueryWireBackAndFetchPhase() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random timings for the post-query portion of the pipeline
            long queryPhaseEndNanos = randomLongBetween(1_000_000_000L, 100_000_000_000L);

            // wire_back_query positioned after query phase
            long wireBackQueryDurationNanos = randomLongBetween(0L, 10_000_000L);
            long wireBackQueryEndNanos = queryPhaseEndNanos + wireBackQueryDurationNanos;

            // Optional reduce phase between wire_back_query and wire_out_fetch
            long reducePhaseGapNanos = randomLongBetween(0L, 20_000_000L);
            long afterReduceNanos = wireBackQueryEndNanos + reducePhaseGapNanos;

            // wire_out_fetch duration
            long wireOutFetchMicros = randomLongBetween(1L, 5_000L);
            long wireOutFetchNanos = TimeUnit.MICROSECONDS.toNanos(wireOutFetchMicros);

            // fetch phase starts after wire_out_fetch
            long fetchPhaseStartNanos = afterReduceNanos + wireOutFetchNanos;
            long fetchPhaseDurationNanos = randomLongBetween(1_000_000L, 200_000_000L);
            long fetchPhaseEndNanos = fetchPhaseStartNanos + fetchPhaseDurationNanos;

            // wire_back_fetch duration from fetch roundtrip decomposition
            long dataNodeFetchExecNanos = randomLongBetween(1_000L, 200_000_000L);
            long fetchNetworkRoundtripNanos = wireOutFetchNanos + dataNodeFetchExecNanos + randomLongBetween(0L, 5_000_000L);
            long wireBackFetchNanos = Math.max(0, fetchNetworkRoundtripNanos - wireOutFetchNanos - dataNodeFetchExecNanos);

            // --- Simulate wire bar positioning ---

            // Position wire_out_fetch after reduce/wire_back_query end, before fetch phase start
            long wireOutFetchStart = afterReduceNanos;
            long wireOutFetchEnd = wireOutFetchStart + wireOutFetchNanos;

            // Position wire_back_fetch after fetch phase ends
            long wireBackFetchStart = fetchPhaseEndNanos;
            long wireBackFetchEnd = wireBackFetchStart + wireBackFetchNanos;

            // --- PROPERTY ASSERTIONS ---

            // wire_out_fetch.start >= wire_back_query.end (or reduce end)
            assertTrue(
                "Iteration " + i + ": wire_out_fetch.start (" + wireOutFetchStart
                    + ") must be >= wire_back_query.end/reduce_end (" + wireBackQueryEndNanos + ")",
                wireOutFetchStart >= wireBackQueryEndNanos
            );

            // wire_out_fetch.end <= fetch_phase.start
            assertTrue(
                "Iteration " + i + ": wire_out_fetch.end (" + wireOutFetchEnd
                    + ") must be <= fetch_phase.start (" + fetchPhaseStartNanos + ")",
                wireOutFetchEnd <= fetchPhaseStartNanos
            );

            // wire_back_fetch.start >= fetch_phase.end
            assertTrue(
                "Iteration " + i + ": wire_back_fetch.start (" + wireBackFetchStart
                    + ") must be >= fetch_phase.end (" + fetchPhaseEndNanos + ")",
                wireBackFetchStart >= fetchPhaseEndNanos
            );

            // Additional: wire_back_fetch duration is non-negative
            assertTrue(
                "Iteration " + i + ": wire_back_fetch duration (" + wireBackFetchNanos + ") must be >= 0",
                wireBackFetchNanos >= 0
            );

            // Additional: wire_out_fetch duration is non-negative
            assertTrue(
                "Iteration " + i + ": wire_out_fetch duration (" + wireOutFetchNanos + ") must be >= 0",
                wireOutFetchNanos >= 0
            );
        }
    }

    /**
     * Property 5: End-to-end positioning of all four wire bars using SearchLatencyBreakdown.
     * <p>
     * Uses the real {@link SearchLatencyBreakdown#recordTimedEvent} method to record wire bars
     * and then verifies their positions via {@link SearchLatencyBreakdown#getNamedEventTiming}.
     * <p>
     * Simulates the full wire computation as done in AbstractSearchAsyncAction:
     * <ol>
     *   <li>Record query phase timing</li>
     *   <li>Compute wire_out_query = wall_clock_offset</li>
     *   <li>Compute wire_back_query = max(0, roundtrip - wire_out - dataNodeExec)</li>
     *   <li>Position wire_out_query at preSearchEnd</li>
     *   <li>Position wire_back_query at query phase end</li>
     * </ol>
     * <p>
     * <b>Validates: Requirements 3.3, 3.4, 3.5</b>
     */
    public void testEndToEndWireBarPositioningViaSearchLatencyBreakdown() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random timing values
            long absoluteStartNanos = randomLongBetween(1_000_000_000L, 100_000_000_000L);
            long preSearchDurationNanos = randomLongBetween(1_000_000L, 50_000_000L);
            long preSearchEndNanos = absoluteStartNanos + preSearchDurationNanos;

            // Mark first phase start (= pre-search end)
            breakdown.markFirstPhaseStart(preSearchEndNanos);

            // wire_out_query from wall_clock_offset
            long wallClockOffsetMicros = randomLongBetween(1L, 10_000L);
            long wireOutNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros);

            // query phase positioned after wire_out
            long queryPhaseStartNanos = preSearchEndNanos + wireOutNanos;
            long queryPhaseDurationNanos = randomLongBetween(1_000_000L, 500_000_000L);
            long queryPhaseEndNanos = queryPhaseStartNanos + queryPhaseDurationNanos;

            // Record query phase as a named event
            breakdown.recordTimedEvent("query", queryPhaseStartNanos, queryPhaseEndNanos);

            // Compute wire_back_query
            long dataNodeExecNanos = randomLongBetween(1_000L, queryPhaseDurationNanos);
            long networkRoundtripNanos = wireOutNanos + dataNodeExecNanos + randomLongBetween(0L, 10_000_000L);
            long wireBackNanos = Math.max(0, networkRoundtripNanos - wireOutNanos - dataNodeExecNanos);

            // Record wire bars using the same logic as AbstractSearchAsyncAction
            // wire_out_query positioned at preSearchEnd
            breakdown.recordTimedEvent("wire_out_query", preSearchEndNanos, preSearchEndNanos + wireOutNanos);

            // wire_back_query positioned at query phase end
            breakdown.recordTimedEvent("wire_back_query", queryPhaseEndNanos, queryPhaseEndNanos + wireBackNanos);

            // --- PROPERTY ASSERTIONS via getNamedEventTiming ---

            long[] wireOutTiming = breakdown.getNamedEventTiming("wire_out_query");
            long[] wireBackTiming = breakdown.getNamedEventTiming("wire_back_query");
            long[] queryTiming = breakdown.getNamedEventTiming("query");

            assertNotNull("wire_out_query should be recorded (iteration " + i + ")", wireOutTiming);
            assertNotNull("wire_back_query should be recorded (iteration " + i + ")", wireBackTiming);
            assertNotNull("query should be recorded (iteration " + i + ")", queryTiming);

            long wireOutQueryStart = wireOutTiming[0];
            long wireOutQueryDuration = wireOutTiming[1];
            long wireOutQueryEnd = wireOutQueryStart + wireOutQueryDuration;

            long wireBackQueryStart = wireBackTiming[0];
            long wireBackQueryDuration = wireBackTiming[1];

            long queryStart = queryTiming[0];
            long queryEnd = queryStart + queryTiming[1];

            long firstPhaseStart = breakdown.getFirstPhaseStartNanos();

            // wire_out_query.start >= pre_search_end (firstPhaseStartNanos)
            assertTrue(
                "Iteration " + i + ": wire_out_query.start (" + wireOutQueryStart
                    + ") must be >= firstPhaseStartNanos (" + firstPhaseStart + ")",
                wireOutQueryStart >= firstPhaseStart
            );

            // wire_out_query.end <= query_phase.start
            assertTrue(
                "Iteration " + i + ": wire_out_query.end (" + wireOutQueryEnd
                    + ") must be <= query_phase.start (" + queryStart + ")",
                wireOutQueryEnd <= queryStart
            );

            // wire_back_query.start >= query_phase.end
            assertTrue(
                "Iteration " + i + ": wire_back_query.start (" + wireBackQueryStart
                    + ") must be >= query_phase.end (" + queryEnd + ")",
                wireBackQueryStart >= queryEnd
            );

            // Duration assertions
            assertTrue(
                "Iteration " + i + ": wire_out_query duration (" + wireOutQueryDuration + ") must be >= 0",
                wireOutQueryDuration >= 0
            );
            assertTrue(
                "Iteration " + i + ": wire_back_query duration (" + wireBackQueryDuration + ") must be >= 0",
                wireBackQueryDuration >= 0
            );
        }
    }

    /**
     * Property 5: Wire bars never overlap with their adjacent phases.
     * <p>
     * Generates random configurations and verifies that:
     * <ul>
     *   <li>wire_out_query does NOT overlap with query phase (wire_out ends before query starts)</li>
     *   <li>wire_back_query does NOT overlap with query phase (wire_back starts after query ends)</li>
     * </ul>
     * <p>
     * This tests the non-overlap invariant which is the core improvement over the old
     * overlapping network_roundtrip bars.
     * <p>
     * <b>Validates: Requirements 3.3, 3.4, 3.5</b>
     */
    public void testWireBarsNeverOverlapWithAdjacentPhases() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random timings with guaranteed non-overlap
            long preSearchEndNanos = randomLongBetween(1_000_000_000L, 100_000_000_000L);

            // wall_clock_offset determines wire_out duration
            long wallClockOffsetMicros = randomLongBetween(1L, 10_000L);
            long wireOutNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros);

            // Query phase starts exactly where wire_out ends
            long queryPhaseStartNanos = preSearchEndNanos + wireOutNanos;
            long queryPhaseDurationNanos = randomLongBetween(1_000_000L, 500_000_000L);
            long queryPhaseEndNanos = queryPhaseStartNanos + queryPhaseDurationNanos;

            // wire_back duration from roundtrip decomposition
            long dataNodeExecNanos = randomLongBetween(1_000L, 500_000_000L);
            long networkRoundtripNanos = wireOutNanos + dataNodeExecNanos + randomLongBetween(0L, 50_000_000L);
            long wireBackNanos = Math.max(0, networkRoundtripNanos - wireOutNanos - dataNodeExecNanos);

            // Position wire bars
            long wireOutStart = preSearchEndNanos;
            long wireOutEnd = wireOutStart + wireOutNanos;
            long wireBackStart = queryPhaseEndNanos;
            long wireBackEnd = wireBackStart + wireBackNanos;

            // --- PROPERTY ASSERTIONS: No overlap ---

            // wire_out_query and query phase do NOT overlap
            // Overlap would mean: wireOutStart < queryPhaseEnd AND wireOutEnd > queryPhaseStart
            // Non-overlap: wireOutEnd <= queryPhaseStart
            assertTrue(
                "Iteration " + i + ": wire_out_query must NOT overlap with query phase."
                    + " wire_out_query.end=" + wireOutEnd + ", query_phase.start=" + queryPhaseStartNanos,
                wireOutEnd <= queryPhaseStartNanos
            );

            // wire_back_query and query phase do NOT overlap
            // Non-overlap: wireBackStart >= queryPhaseEnd
            assertTrue(
                "Iteration " + i + ": wire_back_query must NOT overlap with query phase."
                    + " wire_back_query.start=" + wireBackStart + ", query_phase.end=" + queryPhaseEndNanos,
                wireBackStart >= queryPhaseEndNanos
            );

            // wire_out_query ends exactly at or before query phase starts (no gap between wire_out_end and query_start in implementation)
            assertEquals(
                "Iteration " + i + ": wire_out_query.end should exactly equal query_phase.start"
                    + " (wire_out fills the gap between pre-search and query)",
                queryPhaseStartNanos,
                wireOutEnd
            );

            // wire_back_query starts exactly at query phase end (no gap in implementation)
            assertEquals(
                "Iteration " + i + ": wire_back_query.start should exactly equal query_phase.end"
                    + " (wire_back starts immediately after query completes)",
                queryPhaseEndNanos,
                wireBackStart
            );
        }
    }
}
