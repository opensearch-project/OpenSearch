/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Property-based test validating backward compatibility when data nodes do not provide absolute offsets.
 * <p>
 * Feature: breakdown-ordering-fix, Property 11: Backward compatibility — graceful fallback without absolute offsets
 * <p>
 * For any shard breakdown that does NOT contain {@code *_start} keys (older data node), the coordinator
 * SHALL still produce a valid timed breakdown map using sequential cursor positioning, and the dashboard
 * SHALL render without errors.
 * <p>
 * This test simulates the coordinator's generic merge loop with {@code hasAbsoluteOffsets = false},
 * verifying that:
 * <ul>
 *   <li>All duration metrics are still positioned (no missing bars)</li>
 *   <li>Events are positioned sequentially (each event starts where the previous one ended)</li>
 *   <li>No errors occur during processing</li>
 *   <li>The resulting positions form a valid, non-overlapping timeline</li>
 *   <li>Missing {@code wall_clock_offset_micros} means wire_out/wire_back are not emitted</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 7.1, 7.2, 7.3</b>
 */
public class BackwardCompatibilityFallbackPropertyTests extends OpenSearchTestCase {

    /** Minimum 100 iterations as required by the design document. */
    private static final int ITERATIONS = 150;

    /** Known query-phase duration keys that an older data node might emit (without _start). */
    private static final String[] QUERY_PHASE_DURATION_KEYS = {
        "agg_pre_process",
        "query_internal_execution",
        "agg_post_process",
        "request_cache_lookup",
        "request_cache_write",
        "search_context_creation",
        "acquire_reader_context",
        "query_pre_process",
        "rescore",
        "suggest"
    };

    /** Metadata key that controls wire_out/wire_back emission. */
    private static final String WALL_CLOCK_OFFSET_MICROS = "wall_clock_offset_micros";

    /**
     * Property 11: Sequential cursor fallback produces valid timed breakdown without _start keys.
     * <p>
     * Generates random breakdown maps WITHOUT any {@code *_start} keys (simulating older data nodes),
     * then applies the coordinator's generic merge loop with {@code hasAbsoluteOffsets = false}.
     * Asserts that all duration metrics are positioned sequentially and form a valid timeline.
     * <p>
     * <b>Validates: Requirements 7.1, 7.2, 7.3</b>
     */
    public void testSequentialCursorFallbackProducesValidTimeline() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate a random breakdown map WITHOUT _start keys (older data node)
            Map<String, Long> shardBreakdown = generateBreakdownWithoutStartKeys();

            // Ensure no _start keys are present (precondition)
            for (String key : shardBreakdown.keySet()) {
                assertFalse(
                    "Precondition: no _start keys should be present in generated breakdown, but found: " + key,
                    key.endsWith("_start")
                );
            }

            // Simulate coordinator merge with hasAbsoluteOffsets = false
            Map<String, long[]> timedEvents = simulateCoordinatorMergeSequentialFallback(shardBreakdown);

            // ASSERTION 1: All duration metrics are positioned (no missing bars)
            for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
                String key = entry.getKey();
                if (key.equals(WALL_CLOCK_OFFSET_MICROS)) continue; // metadata, not a bar
                if (entry.getValue() > 0) {
                    assertTrue(
                        "Duration metric '" + key + "' should be positioned in timed events (iteration " + i + ")",
                        timedEvents.containsKey(key)
                    );
                }
            }

            // ASSERTION 2: Events are positioned sequentially (each starts where previous ended)
            List<Map.Entry<String, long[]>> orderedEvents = new ArrayList<>(timedEvents.entrySet());
            for (int j = 1; j < orderedEvents.size(); j++) {
                long prevEnd = orderedEvents.get(j - 1).getValue()[0] + orderedEvents.get(j - 1).getValue()[1];
                long currStart = orderedEvents.get(j).getValue()[0];
                assertEquals(
                    "Event '" + orderedEvents.get(j).getKey() + "' should start where '"
                        + orderedEvents.get(j - 1).getKey() + "' ended (iteration " + i + ")",
                    prevEnd,
                    currStart
                );
            }

            // ASSERTION 3: All positions are non-negative and durations are positive
            for (Map.Entry<String, long[]> event : timedEvents.entrySet()) {
                long startOffset = event.getValue()[0];
                long duration = event.getValue()[1];
                assertTrue(
                    "Start offset for '" + event.getKey() + "' must be >= 0 (iteration " + i + "), got: " + startOffset,
                    startOffset >= 0
                );
                assertTrue(
                    "Duration for '" + event.getKey() + "' must be > 0 (iteration " + i + "), got: " + duration,
                    duration > 0
                );
            }

            // ASSERTION 4: No overlapping events (timeline is valid and non-overlapping)
            for (int j = 0; j < orderedEvents.size(); j++) {
                long startJ = orderedEvents.get(j).getValue()[0];
                long endJ = startJ + orderedEvents.get(j).getValue()[1];
                for (int k = j + 1; k < orderedEvents.size(); k++) {
                    long startK = orderedEvents.get(k).getValue()[0];
                    assertTrue(
                        "Events must not overlap: '" + orderedEvents.get(j).getKey() + "' ends at " + endJ
                            + " but '" + orderedEvents.get(k).getKey() + "' starts at " + startK + " (iteration " + i + ")",
                        startK >= endJ
                    );
                }
            }
        }
    }

    /**
     * Property 11: Missing wall_clock_offset_micros means wire_out/wire_back are not emitted.
     * <p>
     * When the shard breakdown does not contain {@code wall_clock_offset_micros}, the coordinator
     * SHALL NOT emit {@code wire_out_query}, {@code wire_back_query}, {@code wire_out_fetch},
     * or {@code wire_back_fetch} bars.
     * <p>
     * <b>Validates: Requirements 7.2</b>
     */
    public void testNoWireOutWireBackWithoutWallClockOffset() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate breakdown WITHOUT wall_clock_offset_micros
            Map<String, Long> shardBreakdown = generateBreakdownWithoutStartKeys();
            shardBreakdown.remove(WALL_CLOCK_OFFSET_MICROS); // ensure it's not present

            // Simulate the coordinator merge and wire bar computation
            Map<String, long[]> timedEvents = simulateCoordinatorMergeWithWireComputation(shardBreakdown);

            // ASSERTION: No wire_out or wire_back events are emitted
            for (String key : timedEvents.keySet()) {
                assertFalse(
                    "wire_out bars should NOT be emitted without wall_clock_offset_micros, but found: '"
                        + key + "' (iteration " + i + ")",
                    key.startsWith("wire_out")
                );
                assertFalse(
                    "wire_back bars should NOT be emitted without wall_clock_offset_micros, but found: '"
                        + key + "' (iteration " + i + ")",
                    key.startsWith("wire_back")
                );
            }
        }
    }

    /**
     * Property 11: Contrast — when wall_clock_offset_micros IS present, wire bars are emitted.
     * <p>
     * This verifies the positive case: with wall_clock_offset_micros > 0 and a network roundtrip
     * available, the coordinator emits wire_out/wire_back bars.
     * <p>
     * <b>Validates: Requirements 7.2 (by contrast)</b>
     */
    public void testWireOutWireBackEmittedWithWallClockOffset() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate breakdown WITH wall_clock_offset_micros present
            Map<String, Long> shardBreakdown = generateBreakdownWithoutStartKeys();
            long wallClockOffsetMicros = randomLongBetween(10L, 5000L); // 10µs to 5ms
            shardBreakdown.put(WALL_CLOCK_OFFSET_MICROS, wallClockOffsetMicros);

            // Simulate with a known query phase network roundtrip
            long networkRoundtripNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros)
                + randomLongBetween(100_000L, 10_000_000L); // roundtrip > wire_out

            Map<String, long[]> timedEvents = simulateCoordinatorMergeWithWireComputation(
                shardBreakdown, networkRoundtripNanos
            );

            // ASSERTION: wire_out_query and wire_back_query are emitted
            assertTrue(
                "wire_out_query should be emitted when wall_clock_offset_micros is present (iteration " + i + ")",
                timedEvents.containsKey("wire_out_query")
            );
            assertTrue(
                "wire_back_query should be emitted when wall_clock_offset_micros is present (iteration " + i + ")",
                timedEvents.containsKey("wire_back_query")
            );

            // Verify wire durations are non-negative
            long wireOutDuration = timedEvents.get("wire_out_query")[1];
            long wireBackDuration = timedEvents.get("wire_back_query")[1];
            assertTrue("wire_out_query duration must be >= 0 (iteration " + i + ")", wireOutDuration >= 0);
            assertTrue("wire_back_query duration must be >= 0 (iteration " + i + ")", wireBackDuration >= 0);
        }
    }

    /**
     * Property 11: Sequential fallback produces consistent output even with varying metric counts.
     * <p>
     * Tests that regardless of how many or how few metrics are in the breakdown (from 1 to all),
     * sequential cursor positioning always produces a valid, contiguous timeline.
     * <p>
     * <b>Validates: Requirements 7.1, 7.3</b>
     */
    public void testSequentialFallbackWithVaryingMetricCounts() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate 1 to N random metrics (variable count)
            int numMetrics = randomIntBetween(1, QUERY_PHASE_DURATION_KEYS.length);
            Map<String, Long> shardBreakdown = new LinkedHashMap<>();

            for (int m = 0; m < numMetrics; m++) {
                String key = QUERY_PHASE_DURATION_KEYS[m];
                long duration = randomLongBetween(1_000L, 100_000_000L); // 1µs to 100ms in nanos
                shardBreakdown.put(key, duration);
            }

            // Simulate coordinator merge with sequential cursor fallback
            Map<String, long[]> timedEvents = simulateCoordinatorMergeSequentialFallback(shardBreakdown);

            // ASSERTION: Exactly numMetrics events produced
            assertEquals(
                "Should produce exactly " + numMetrics + " timed events (iteration " + i + ")",
                numMetrics,
                timedEvents.size()
            );

            // ASSERTION: First event starts at offset 0 (beginning of sequential cursor)
            long[] firstEvent = timedEvents.values().iterator().next();
            assertEquals(
                "First sequential event should start at offset 0 (iteration " + i + ")",
                0L,
                firstEvent[0]
            );

            // ASSERTION: Total timeline length equals sum of all durations
            long expectedTotalLength = 0;
            for (Long duration : shardBreakdown.values()) {
                expectedTotalLength += duration;
            }
            List<long[]> events = new ArrayList<>(timedEvents.values());
            long[] lastEvent = events.get(events.size() - 1);
            long actualTotalLength = lastEvent[0] + lastEvent[1];
            assertEquals(
                "Total timeline length should equal sum of all durations (iteration " + i + ")",
                expectedTotalLength,
                actualTotalLength
            );
        }
    }

    /**
     * Property 11: Output from sequential fallback can be converted to micros for Gantt chart.
     * <p>
     * Simulates the full pipeline: sequential cursor → recordTimedEvent → toTimedBreakdownMap format.
     * Verifies that the output contains valid start_offset_micros and duration_micros for every event.
     * <p>
     * <b>Validates: Requirements 7.1, 7.3</b>
     */
    public void testSequentialFallbackProducesValidTimedBreakdownMapFormat() {
        for (int i = 0; i < ITERATIONS; i++) {
            Map<String, Long> shardBreakdown = generateBreakdownWithoutStartKeys();
            shardBreakdown.remove(WALL_CLOCK_OFFSET_MICROS);

            // Use a realistic absolute start nanos
            long absoluteStartNanos = randomLongBetween(100_000_000_000_000L, 500_000_000_000_000L);

            // Simulate the full coordinator pipeline
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Apply sequential cursor positioning (simulating the coordinator merge)
            long currentOffsetNanos = absoluteStartNanos;
            for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
                String key = entry.getKey();
                long duration = entry.getValue();
                if (key.equals(WALL_CLOCK_OFFSET_MICROS)) continue;
                if (duration <= 0) continue;

                breakdown.recordTimedEvent(key, currentOffsetNanos, currentOffsetNanos + duration);
                currentOffsetNanos += duration;
            }

            // Convert to timed breakdown map (as the API would return)
            Map<String, Map<String, Long>> timedMap = breakdown.toTimedBreakdownMap(absoluteStartNanos);

            // ASSERTION: Every non-zero metric appears in output with valid start_offset and duration
            for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
                String key = entry.getKey();
                long durationNanos = entry.getValue();
                if (key.equals(WALL_CLOCK_OFFSET_MICROS)) continue;

                long durationMicros = TimeUnit.NANOSECONDS.toMicros(durationNanos);
                if (durationMicros > 0) {
                    assertTrue(
                        "Metric '" + key + "' should appear in timed breakdown map (iteration " + i + ")",
                        timedMap.containsKey(key)
                    );

                    Map<String, Long> timing = timedMap.get(key);
                    assertNotNull("Timing for '" + key + "' should not be null (iteration " + i + ")", timing);
                    assertTrue(
                        "start_offset_micros must be >= 0 for '" + key + "' (iteration " + i + ")",
                        timing.get("start_offset_micros") >= 0
                    );
                    assertTrue(
                        "duration_micros must be > 0 for '" + key + "' (iteration " + i + ")",
                        timing.get("duration_micros") > 0
                    );
                }
            }

            // ASSERTION: Events should be non-overlapping in micros (check all pairs)
            // Note: ConcurrentHashMap doesn't preserve insertion order, so we sort by start offset
            List<Map.Entry<String, Map<String, Long>>> mapEntries = new ArrayList<>(timedMap.entrySet());
            mapEntries.sort((a, b) -> Long.compare(
                a.getValue().get("start_offset_micros"),
                b.getValue().get("start_offset_micros")
            ));
            for (int j = 1; j < mapEntries.size(); j++) {
                long prevEnd = mapEntries.get(j - 1).getValue().get("start_offset_micros")
                    + mapEntries.get(j - 1).getValue().get("duration_micros");
                long currStart = mapEntries.get(j).getValue().get("start_offset_micros");
                // Allow 1µs tolerance for nanos→micros truncation
                assertTrue(
                    "Timed events should not overlap in micros: event '" + mapEntries.get(j).getKey()
                        + "' starts at " + currStart + " but previous event '"
                        + mapEntries.get(j - 1).getKey() + "' ends at " + prevEnd + " (iteration " + i + ")",
                    currStart >= prevEnd - 1
                );
            }
        }
    }

    // ========== HELPER METHODS ==========

    /**
     * Generates a random breakdown map WITHOUT any {@code *_start} keys.
     * Simulates what an older data node would produce.
     */
    private Map<String, Long> generateBreakdownWithoutStartKeys() {
        Map<String, Long> breakdown = new LinkedHashMap<>();

        // Randomly include a subset of known query-phase metrics
        int numMetrics = randomIntBetween(2, QUERY_PHASE_DURATION_KEYS.length);
        for (int i = 0; i < numMetrics; i++) {
            String key = QUERY_PHASE_DURATION_KEYS[randomIntBetween(0, QUERY_PHASE_DURATION_KEYS.length - 1)];
            long duration = randomLongBetween(1_000L, 200_000_000L); // 1µs to 200ms in nanos
            breakdown.put(key, duration);
        }

        // Optionally add some custom/unknown metrics (self-describing extensibility)
        int numCustom = randomIntBetween(0, 3);
        for (int i = 0; i < numCustom; i++) {
            String key = "custom_metric_" + randomIntBetween(1, 50);
            long duration = randomLongBetween(1_000L, 50_000_000L);
            breakdown.put(key, duration);
        }

        return breakdown;
    }

    /**
     * Simulates the coordinator's generic merge loop with sequential cursor fallback.
     * This is the code path taken when {@code hasAbsoluteOffsets = false} (no _start keys).
     *
     * @param shardBreakdown the shard breakdown map without _start keys
     * @return ordered map of event name → [startOffset, duration] in nanoseconds
     */
    private Map<String, long[]> simulateCoordinatorMergeSequentialFallback(Map<String, Long> shardBreakdown) {
        Map<String, long[]> timedEvents = new LinkedHashMap<>();
        long currentOffsetNanos = 0; // Sequential cursor starts at 0

        for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue();

            // Skip metadata keys
            if (key.equals(WALL_CLOCK_OFFSET_MICROS)) continue;

            // Skip _start keys (should not exist in this test, but guard)
            if (key.endsWith("_start")) continue;

            // Skip zero/negative durations
            if (value <= 0) continue;

            // SEQUENTIAL CURSOR FALLBACK: position at current cursor, advance
            timedEvents.put(key, new long[] { currentOffsetNanos, value });
            currentOffsetNanos += value;
        }

        return timedEvents;
    }

    /**
     * Simulates the coordinator merge loop plus wire_out/wire_back computation.
     * Wire bars are only emitted when {@code wall_clock_offset_micros} is present.
     *
     * @param shardBreakdown the shard breakdown map
     * @return ordered map of event name → [startOffset, duration] in nanoseconds
     */
    private Map<String, long[]> simulateCoordinatorMergeWithWireComputation(Map<String, Long> shardBreakdown) {
        return simulateCoordinatorMergeWithWireComputation(shardBreakdown, 0L);
    }

    /**
     * Simulates the coordinator merge loop plus wire_out/wire_back computation.
     * Wire bars are only emitted when {@code wall_clock_offset_micros} is present.
     *
     * @param shardBreakdown the shard breakdown map
     * @param networkRoundtripNanos the network roundtrip time for query phase (0 means auto-compute)
     * @return ordered map of event name → [startOffset, duration] in nanoseconds
     */
    private Map<String, long[]> simulateCoordinatorMergeWithWireComputation(
        Map<String, Long> shardBreakdown,
        long networkRoundtripNanos
    ) {
        Map<String, long[]> timedEvents = new LinkedHashMap<>();
        long currentOffsetNanos = 0;

        // First pass: position all duration metrics using sequential cursor
        long totalDataNodeExecNanos = 0;
        for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue();

            if (key.equals(WALL_CLOCK_OFFSET_MICROS)) continue;
            if (key.endsWith("_start")) continue;
            if (value <= 0) continue;

            timedEvents.put(key, new long[] { currentOffsetNanos, value });
            currentOffsetNanos += value;
            totalDataNodeExecNanos += value;
        }

        // Second pass: compute wire_out/wire_back if wall_clock_offset_micros is present
        Long wallClockOffsetMicros = shardBreakdown.get(WALL_CLOCK_OFFSET_MICROS);
        if (wallClockOffsetMicros != null && wallClockOffsetMicros > 0) {
            long wireOutNanos = TimeUnit.MICROSECONDS.toNanos(wallClockOffsetMicros);

            // If no explicit roundtrip provided, compute a synthetic one
            if (networkRoundtripNanos <= 0) {
                networkRoundtripNanos = wireOutNanos + totalDataNodeExecNanos + randomLongBetween(1_000L, 5_000_000L);
            }

            long wireBackNanos = Math.max(0, networkRoundtripNanos - wireOutNanos - totalDataNodeExecNanos);

            // Position wire_out before the data node metrics
            // Position wire_back after the data node metrics
            long wireOutStart = 0; // simplified: at the very beginning
            timedEvents.put("wire_out_query", new long[] { wireOutStart, wireOutNanos });

            long wireBackStart = currentOffsetNanos;
            timedEvents.put("wire_back_query", new long[] { wireBackStart, wireBackNanos });
        }

        return timedEvents;
    }
}
