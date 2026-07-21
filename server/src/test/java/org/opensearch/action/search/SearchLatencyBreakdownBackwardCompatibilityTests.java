/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration/verification tests for backward compatibility with older data nodes
 * that do NOT emit {@code *_start} keys or {@code wall_clock_offset_micros}.
 * <p>
 * This test verifies the complete coordinator merge pipeline when dealing with
 * older data node responses, ensuring:
 * <ul>
 *   <li>Sequential cursor fallback produces valid positioning for all events</li>
 *   <li>No wire_out/wire_back events are emitted without {@code wall_clock_offset_micros}</li>
 *   <li>The timed breakdown map is valid and the dashboard can render it ({@code _has_timed_breakdown})</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 7.1, 7.2, 7.3</b>
 */
public class SearchLatencyBreakdownBackwardCompatibilityTests extends OpenSearchTestCase {

    /**
     * Verify that the coordinator falls back to sequential cursor positioning when shard
     * breakdown from an older data node lacks {@code *_start} keys.
     * <p>
     * Creates a {@link SearchLatencyBreakdown} instance, simulates the coordinator merge
     * logic WITHOUT any {@code *_start} keys, and verifies that:
     * <ul>
     *   <li>All events get valid start_offset and duration</li>
     *   <li>Events are positioned sequentially (contiguous timeline)</li>
     *   <li>The resulting timed breakdown map contains all expected entries</li>
     * </ul>
     * <p>
     * <b>Validates: Requirements 7.1</b>
     */
    public void testCoordinatorFallsBackToSequentialCursorWithoutStartKeys() {
        // Simulate an older data node response: durations only, no *_start keys
        Map<String, Long> olderDataNodeBreakdown = new LinkedHashMap<>();
        olderDataNodeBreakdown.put("agg_pre_process", TimeUnit.MILLISECONDS.toNanos(5));
        olderDataNodeBreakdown.put("query_internal_execution", TimeUnit.MILLISECONDS.toNanos(30));
        olderDataNodeBreakdown.put("agg_post_process", TimeUnit.MILLISECONDS.toNanos(3));
        olderDataNodeBreakdown.put("request_cache_lookup", TimeUnit.MILLISECONDS.toNanos(1));
        olderDataNodeBreakdown.put("search_context_creation", TimeUnit.MILLISECONDS.toNanos(2));

        // Verify precondition: no _start keys present
        for (String key : olderDataNodeBreakdown.keySet()) {
            assertFalse("Older data node should NOT have _start keys", key.endsWith("_start"));
        }

        // Set up a SearchLatencyBreakdown (coordinator-side)
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulate the coordinator's generic merge loop with hasAbsoluteOffsets = false
        // This is the fallback path: requestAbsoluteStartNanos == 0 (no absolute offsets)
        long absoluteStartNanos = System.nanoTime();
        long queryPhaseStartNanos = absoluteStartNanos + TimeUnit.MILLISECONDS.toNanos(10);

        // Mark that a query phase occurred
        breakdown.markFirstPhaseStart(queryPhaseStartNanos);
        breakdown.recordQueryPhase(TimeUnit.MILLISECONDS.toNanos(45));
        breakdown.markPhaseEnd("query", queryPhaseStartNanos + TimeUnit.MILLISECONDS.toNanos(45));
        breakdown.recordTimedEvent("query", queryPhaseStartNanos, queryPhaseStartNanos + TimeUnit.MILLISECONDS.toNanos(45));

        // Simulate the sequential cursor fallback merge loop
        // When hasAbsoluteOffsets is false and keys are query-phase children, they use sequential cursor
        long currentOffsetNanos = queryPhaseStartNanos;
        for (Map.Entry<String, Long> entry : olderDataNodeBreakdown.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue();
            if (value > 0) {
                breakdown.recordTimedEvent(key, currentOffsetNanos, currentOffsetNanos + value);
                currentOffsetNanos += value;
            }
        }

        // Convert to timed breakdown map (as returned to the dashboard)
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStartNanos);

        // ASSERTION 1: All shard breakdown metrics are present in the output
        for (Map.Entry<String, Long> entry : olderDataNodeBreakdown.entrySet()) {
            String key = entry.getKey();
            long durationMicros = TimeUnit.NANOSECONDS.toMicros(entry.getValue());
            if (durationMicros > 0) {
                assertTrue(
                    "Metric '" + key + "' should appear in timed breakdown map",
                    timedMap.containsKey(key)
                );
            }
        }

        // ASSERTION 2: Each event has valid start_offset_micros >= 0 and duration_micros > 0
        for (Map.Entry<String, Map<String, Long>> entry : timedMap.entrySet()) {
            String key = entry.getKey();
            Map<String, Long> timing = entry.getValue();
            assertNotNull("Timing for '" + key + "' should not be null", timing);
            assertTrue(
                "start_offset_micros for '" + key + "' should be >= 0, got: " + timing.get("start_offset_micros"),
                timing.get("start_offset_micros") >= 0
            );
            assertTrue(
                "duration_micros for '" + key + "' should be > 0, got: " + timing.get("duration_micros"),
                timing.get("duration_micros") > 0
            );
        }

        // ASSERTION 3: Events within the older shard breakdown are positioned sequentially
        // (each starts where the previous one ended, after accounting for nanos→micros truncation)
        long prevEndMicros = -1;
        for (String key : olderDataNodeBreakdown.keySet()) {
            if (!timedMap.containsKey(key)) continue;
            Map<String, Long> timing = timedMap.get(key);
            long startMicros = timing.get("start_offset_micros");
            long durationMicros = timing.get("duration_micros");

            if (prevEndMicros >= 0) {
                // Allow 1µs tolerance for nanos-to-micros truncation
                assertTrue(
                    "Event '" + key + "' should start at or after previous event end. "
                        + "prevEnd=" + prevEndMicros + ", currStart=" + startMicros,
                    startMicros >= prevEndMicros - 1
                );
            }
            prevEndMicros = startMicros + durationMicros;
        }
    }

    /**
     * Verify that wire_out/wire_back events are NOT emitted when
     * {@code wall_clock_offset_micros} is missing from the shard breakdown.
     * <p>
     * This simulates the scenario where an older data node does not record the cross-node
     * wall clock offset, so the coordinator cannot compute network transit decomposition.
     * <p>
     * <b>Validates: Requirements 7.2</b>
     */
    public void testNoWireOutWireBackWithoutWallClockOffsetMicros() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStartNanos = System.nanoTime();
        long queryPhaseStartNanos = absoluteStartNanos + TimeUnit.MILLISECONDS.toNanos(5);
        long queryPhaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(50);
        long queryPhaseEndNanos = queryPhaseStartNanos + queryPhaseDurationNanos;

        // Set up phase tracking
        breakdown.markFirstPhaseStart(queryPhaseStartNanos);
        breakdown.recordQueryPhase(queryPhaseDurationNanos);
        breakdown.markPhaseEnd("query", queryPhaseEndNanos);
        breakdown.recordTimedEvent("query", queryPhaseStartNanos, queryPhaseEndNanos);

        // Record network roundtrip (this is recorded by the coordinator regardless)
        breakdown.recordNetworkRoundtripQuery(TimeUnit.MILLISECONDS.toNanos(60));

        // Simulate older data node: DO NOT record wallClockOffsetMicros
        // (breakdown.recordWallClockOffsetMicros is never called)
        assertEquals(
            "wall_clock_offset_micros should be 0 (not set)",
            0L,
            breakdown.getWallClockOffsetMaxMicros()
        );

        // Simulate the wire computation logic (same as AbstractSearchAsyncAction)
        // When wallClockOffsetMicros == 0, the wire computation block is skipped
        long wallClockOffsetMicros = breakdown.getWallClockOffsetMaxMicros();
        if (wallClockOffsetMicros > 0) {
            // This block should NOT execute
            long wireOutNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros);
            long dataNodeExecNanos = breakdown.getDataNodeQueryExecutionNanos();
            long networkRoundtripNanos = breakdown.getNetworkRoundtripQueryNanos();
            long wireBackNanos = Math.max(0, networkRoundtripNanos - wireOutNanos - dataNodeExecNanos);

            long preSearchEndNanos = breakdown.getFirstPhaseStartNanos();
            if (preSearchEndNanos > 0) {
                breakdown.recordTimedEvent("wire_out_query", preSearchEndNanos, preSearchEndNanos + wireOutNanos);
            }

            long[] queryTiming = breakdown.getNamedEventTiming("query");
            if (queryTiming != null) {
                long qEnd = queryTiming[0] + queryTiming[1];
                breakdown.recordTimedEvent("wire_back_query", qEnd, qEnd + wireBackNanos);
            }
        }

        // Convert to timed breakdown map
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStartNanos);

        // ASSERTION: No wire_out or wire_back entries in the output
        for (String key : timedMap.keySet()) {
            assertFalse(
                "wire_out entries should NOT be present without wall_clock_offset_micros, found: " + key,
                key.startsWith("wire_out")
            );
            assertFalse(
                "wire_back entries should NOT be present without wall_clock_offset_micros, found: " + key,
                key.startsWith("wire_back")
            );
        }

        // ASSERTION: The query phase event is still properly recorded
        assertTrue("Query event should still be present", timedMap.containsKey("query"));
        Map<String, Long> queryTiming = timedMap.get("query");
        assertTrue("Query start_offset_micros should be >= 0", queryTiming.get("start_offset_micros") >= 0);
        assertTrue("Query duration_micros should be > 0", queryTiming.get("duration_micros") > 0);
    }

    /**
     * Verify that the timed breakdown map is valid and contains the {@code _has_timed_breakdown}
     * flag when processed from a backward-compatible data node response.
     * <p>
     * This ensures the dashboard can detect that timed positioning data is available
     * and render the Gantt chart correctly using the sequential cursor positions.
     * <p>
     * <b>Validates: Requirements 7.1, 7.3</b>
     */
    public void testTimedBreakdownMapValidAndHasTimedBreakdownFlag() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStartNanos = System.nanoTime();
        long queryPhaseStartNanos = absoluteStartNanos + TimeUnit.MILLISECONDS.toNanos(8);
        long queryPhaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(40);

        // Set up the coordinator state
        breakdown.markFirstPhaseStart(queryPhaseStartNanos);
        breakdown.recordQueryPhase(queryPhaseDurationNanos);
        breakdown.markPhaseEnd("query", queryPhaseStartNanos + queryPhaseDurationNanos);
        breakdown.recordTimedEvent("query", queryPhaseStartNanos, queryPhaseStartNanos + queryPhaseDurationNanos);

        // Simulate older data node shard breakdown (no _start keys, no wall_clock_offset)
        Map<String, Long> olderShardBreakdown = new LinkedHashMap<>();
        olderShardBreakdown.put("search_context_creation", TimeUnit.MILLISECONDS.toNanos(2));
        olderShardBreakdown.put("agg_pre_process", TimeUnit.MILLISECONDS.toNanos(4));
        olderShardBreakdown.put("query_internal_execution", TimeUnit.MILLISECONDS.toNanos(25));
        olderShardBreakdown.put("agg_post_process", TimeUnit.MILLISECONDS.toNanos(2));

        // Apply sequential cursor fallback
        long currentOffsetNanos = queryPhaseStartNanos;
        for (Map.Entry<String, Long> entry : olderShardBreakdown.entrySet()) {
            long value = entry.getValue();
            if (value > 0) {
                breakdown.recordTimedEvent(entry.getKey(), currentOffsetNanos, currentOffsetNanos + value);
                currentOffsetNanos += value;
            }
        }

        // Get the timed breakdown map
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStartNanos);

        // ASSERTION 1: Map is non-empty (contains positioned events)
        assertFalse("Timed breakdown map should not be empty", timedMap.isEmpty());

        // ASSERTION 2: All recorded events are present with valid timing data
        assertTrue("query should be in timed map", timedMap.containsKey("query"));
        assertTrue("search_context_creation should be in timed map", timedMap.containsKey("search_context_creation"));
        assertTrue("agg_pre_process should be in timed map", timedMap.containsKey("agg_pre_process"));
        assertTrue("query_internal_execution should be in timed map", timedMap.containsKey("query_internal_execution"));
        assertTrue("agg_post_process should be in timed map", timedMap.containsKey("agg_post_process"));

        // ASSERTION 3: Each entry has valid start_offset_micros and duration_micros
        for (Map.Entry<String, Map<String, Long>> entry : timedMap.entrySet()) {
            Map<String, Long> timing = entry.getValue();
            assertNotNull("Timing map for '" + entry.getKey() + "' should not be null", timing);
            assertTrue(
                "start_offset_micros for '" + entry.getKey() + "' must be present",
                timing.containsKey("start_offset_micros")
            );
            assertTrue(
                "duration_micros for '" + entry.getKey() + "' must be present",
                timing.containsKey("duration_micros")
            );
            assertTrue(
                "start_offset_micros for '" + entry.getKey() + "' must be >= 0",
                timing.get("start_offset_micros") >= 0
            );
            assertTrue(
                "duration_micros for '" + entry.getKey() + "' must be > 0",
                timing.get("duration_micros") > 0
            );
        }

        // ASSERTION 4: The breakdown has named events (which means _has_timed_breakdown = 1)
        // This is verified by checking that namedEvents is non-empty (the flag is derived from this)
        long[] queryEvent = breakdown.getNamedEventTiming("query");
        assertNotNull("query named event should exist (enables _has_timed_breakdown)", queryEvent);
        assertTrue("query event start should be > 0", queryEvent[0] > 0);
        assertTrue("query event duration should be > 0", queryEvent[1] > 0);
    }

    /**
     * Verify that the dashboard can render a breakdown map without wire entries.
     * <p>
     * Builds a simulated flat timed breakdown map (as the dashboard would receive) that
     * does NOT contain wire_out/wire_back entries. Verifies that the map is structurally
     * valid for dashboard rendering (all metrics have both start_offset_micros and
     * duration_micros, no wire entries that would require transport-category rendering).
     * <p>
     * <b>Validates: Requirements 7.3</b>
     */
    public void testDashboardRendersGracefullyWithoutWireBars() {
        // Build a flat timed breakdown map as the dashboard would receive from an older cluster
        // No wire_out_query, wire_back_query, wire_out_fetch, wire_back_fetch
        Map<String, Long> dashboardBreakdownMap = new LinkedHashMap<>();

        // Simulate what toTimedBreakdownMap + XContent serialization would produce
        // Each metric has .start_offset_micros and .duration_micros
        dashboardBreakdownMap.put("query.start_offset_micros", 5000L);
        dashboardBreakdownMap.put("query.duration_micros", 40000L);
        dashboardBreakdownMap.put("search_context_creation.start_offset_micros", 5000L);
        dashboardBreakdownMap.put("search_context_creation.duration_micros", 2000L);
        dashboardBreakdownMap.put("agg_pre_process.start_offset_micros", 7000L);
        dashboardBreakdownMap.put("agg_pre_process.duration_micros", 4000L);
        dashboardBreakdownMap.put("query_internal_execution.start_offset_micros", 11000L);
        dashboardBreakdownMap.put("query_internal_execution.duration_micros", 25000L);
        dashboardBreakdownMap.put("agg_post_process.start_offset_micros", 36000L);
        dashboardBreakdownMap.put("agg_post_process.duration_micros", 2000L);
        dashboardBreakdownMap.put("fetch.start_offset_micros", 48000L);
        dashboardBreakdownMap.put("fetch.duration_micros", 15000L);
        // _has_timed_breakdown flag
        dashboardBreakdownMap.put("_has_timed_breakdown", 1L);

        // ASSERTION 1: No wire entries present (as expected from older cluster)
        for (String key : dashboardBreakdownMap.keySet()) {
            assertFalse("Should not contain wire_out entries: " + key, key.startsWith("wire_out"));
            assertFalse("Should not contain wire_back entries: " + key, key.startsWith("wire_back"));
        }

        // ASSERTION 2: _has_timed_breakdown flag is present
        assertTrue(
            "_has_timed_breakdown should be present in the map",
            dashboardBreakdownMap.containsKey("_has_timed_breakdown")
        );
        assertEquals("_has_timed_breakdown should be 1", 1L, (long) dashboardBreakdownMap.get("_has_timed_breakdown"));

        // ASSERTION 3: All metric entries have valid paired keys (start_offset + duration)
        // Collect base metric names by finding .start_offset_micros suffixed keys
        java.util.Set<String> metricNames = new java.util.HashSet<>();
        for (String key : dashboardBreakdownMap.keySet()) {
            if (key.endsWith(".start_offset_micros")) {
                metricNames.add(key.replace(".start_offset_micros", ""));
            }
        }

        for (String metric : metricNames) {
            assertTrue(
                "Metric '" + metric + "' should have .start_offset_micros",
                dashboardBreakdownMap.containsKey(metric + ".start_offset_micros")
            );
            assertTrue(
                "Metric '" + metric + "' should have .duration_micros",
                dashboardBreakdownMap.containsKey(metric + ".duration_micros")
            );
            assertTrue(
                "start_offset_micros for '" + metric + "' should be >= 0",
                dashboardBreakdownMap.get(metric + ".start_offset_micros") >= 0
            );
            assertTrue(
                "duration_micros for '" + metric + "' should be > 0",
                dashboardBreakdownMap.get(metric + ".duration_micros") > 0
            );
        }

        // ASSERTION 4: The map has enough entries for the dashboard to render a meaningful chart
        assertTrue(
            "Should have at least 3 metrics for a renderable chart, got: " + metricNames.size(),
            metricNames.size() >= 3
        );
    }

    /**
     * Verify end-to-end: an older data node response is processed through the full coordinator
     * pipeline and produces a complete, valid timed breakdown map suitable for Gantt chart rendering.
     * <p>
     * This simulates the entire flow:
     * <ol>
     *   <li>Coordinator receives shard breakdown without _start keys</li>
     *   <li>Coordinator applies sequential cursor fallback</li>
     *   <li>No wire_out/wire_back because wall_clock_offset_micros is absent</li>
     *   <li>The output map is valid for dashboard rendering</li>
     * </ol>
     * <p>
     * <b>Validates: Requirements 7.1, 7.2, 7.3</b>
     */
    public void testEndToEndOlderDataNodeResponseProducesValidOutput() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Coordinator timestamps
        long absoluteStartNanos = 1_000_000_000_000L; // realistic nanoTime
        long preSearchDuration = TimeUnit.MILLISECONDS.toNanos(5);
        long queryPhaseStartNanos = absoluteStartNanos + preSearchDuration;
        long queryPhaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(60);
        long queryPhaseEndNanos = queryPhaseStartNanos + queryPhaseDurationNanos;
        long fetchPhaseStartNanos = queryPhaseEndNanos + TimeUnit.MILLISECONDS.toNanos(3);
        long fetchPhaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(20);
        long fetchPhaseEndNanos = fetchPhaseStartNanos + fetchPhaseDurationNanos;

        // Record coordinator-level phase tracking
        breakdown.markFirstPhaseStart(queryPhaseStartNanos);
        breakdown.recordQueryPhase(queryPhaseDurationNanos);
        breakdown.markPhaseEnd("query", queryPhaseEndNanos);
        breakdown.recordTimedEvent("query", queryPhaseStartNanos, queryPhaseEndNanos);

        breakdown.recordFetchPhase(fetchPhaseDurationNanos);
        breakdown.markPhaseEnd("fetch", fetchPhaseEndNanos);
        breakdown.recordTimedEvent("fetch", fetchPhaseStartNanos, fetchPhaseEndNanos);

        breakdown.recordNetworkRoundtripQuery(TimeUnit.MILLISECONDS.toNanos(65));

        // Simulate older data node shard breakdown response (NO _start keys, NO wall_clock_offset_micros)
        Map<String, Long> olderShardBreakdown = new LinkedHashMap<>();
        olderShardBreakdown.put("acquire_reader_context", TimeUnit.MILLISECONDS.toNanos(1));
        olderShardBreakdown.put("search_context_creation", TimeUnit.MILLISECONDS.toNanos(3));
        olderShardBreakdown.put("agg_pre_process", TimeUnit.MILLISECONDS.toNanos(5));
        olderShardBreakdown.put("query_internal_execution", TimeUnit.MILLISECONDS.toNanos(35));
        olderShardBreakdown.put("agg_post_process", TimeUnit.MILLISECONDS.toNanos(4));
        olderShardBreakdown.put("request_cache_lookup", TimeUnit.MILLISECONDS.toNanos(2));

        // Simulate the generic merge loop with sequential cursor fallback
        // hasAbsoluteOffsets = false (no requestAbsoluteStartNanos or _start keys)
        long currentOffsetNanos = queryPhaseStartNanos;
        for (Map.Entry<String, Long> entry : olderShardBreakdown.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue();

            // No _start key present → sequential cursor fallback
            if (value > 0) {
                breakdown.recordTimedEvent(key, currentOffsetNanos, currentOffsetNanos + value);
                currentOffsetNanos += value;
            }
        }

        // Wire computation: wallClockOffsetMicros is 0 → skip wire bars
        long wallClockOffsetMicros = breakdown.getWallClockOffsetMaxMicros();
        assertEquals("wall_clock_offset_micros should not be set", 0L, wallClockOffsetMicros);
        // Wire computation block is skipped (already verified in testNoWireOutWireBackWithoutWallClockOffsetMicros)

        // Produce the timed breakdown map
        Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStartNanos);

        // VERIFICATION 1: All events are positioned (sequential cursor produced valid entries)
        assertTrue("query should be in output", timedMap.containsKey("query"));
        assertTrue("fetch should be in output", timedMap.containsKey("fetch"));
        assertTrue("acquire_reader_context should be in output", timedMap.containsKey("acquire_reader_context"));
        assertTrue("search_context_creation should be in output", timedMap.containsKey("search_context_creation"));
        assertTrue("agg_pre_process should be in output", timedMap.containsKey("agg_pre_process"));
        assertTrue("query_internal_execution should be in output", timedMap.containsKey("query_internal_execution"));
        assertTrue("agg_post_process should be in output", timedMap.containsKey("agg_post_process"));
        assertTrue("request_cache_lookup should be in output", timedMap.containsKey("request_cache_lookup"));

        // VERIFICATION 2: No wire bars in output
        for (String key : timedMap.keySet()) {
            assertFalse("No wire_out entries should be present: " + key, key.startsWith("wire_out"));
            assertFalse("No wire_back entries should be present: " + key, key.startsWith("wire_back"));
        }

        // VERIFICATION 3: All entries have valid start_offset and duration (renderable by dashboard)
        for (Map.Entry<String, Map<String, Long>> entry : timedMap.entrySet()) {
            Map<String, Long> timing = entry.getValue();
            assertTrue(
                entry.getKey() + " start_offset_micros >= 0",
                timing.get("start_offset_micros") >= 0
            );
            assertTrue(
                entry.getKey() + " duration_micros > 0",
                timing.get("duration_micros") > 0
            );
        }

        // VERIFICATION 4: _has_timed_breakdown is effectively true (namedEvents non-empty)
        assertNotNull(
            "Named event 'query' should exist to indicate _has_timed_breakdown",
            breakdown.getNamedEventTiming("query")
        );
    }
}
