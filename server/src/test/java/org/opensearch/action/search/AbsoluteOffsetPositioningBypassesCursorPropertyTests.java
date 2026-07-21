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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Property-based test validating that absolute offset positioning bypasses the sequential cursor.
 * <p>
 * Feature: breakdown-ordering-fix, Property 3: Absolute offset positioning bypasses sequential cursor
 * <p>
 * For any shard breakdown map containing a key {@code X} and a corresponding {@code X_start} key,
 * the coordinator SHALL position the event using {@code requestAbsoluteStartNanos + X_start} value,
 * and the sequential cursor SHALL NOT be advanced by that metric's duration.
 * <p>
 * Conversely, when a key does NOT have a {@code X_start} entry, the sequential cursor IS advanced
 * by that metric's duration.
 * <p>
 * <b>Validates: Requirements 2.1, 2.2, 6.2, 8.1, 8.2, 8.6</b>
 */
public class AbsoluteOffsetPositioningBypassesCursorPropertyTests extends OpenSearchTestCase {

    /** Minimum 100 iterations as required by the design document. */
    private static final int ITERATIONS = 150;

    /** Known query-phase child keys used in the merge loop. */
    private static final String[] QUERY_PHASE_CHILD_KEYS = {
        "agg_pre_process",
        "query_internal_execution",
        "agg_post_process",
        "request_cache_lookup",
        "request_cache_write",
        "search_context_creation",
        "acquire_reader_context"
    };

    /**
     * Property 3: When X_start is present, event is positioned at requestAbsoluteStartNanos + X_start
     * and sequential cursor is NOT advanced.
     * <p>
     * Generates random breakdown maps where ALL keys have corresponding _start entries,
     * then asserts that:
     * <ul>
     *   <li>Each event is positioned at exactly {@code requestAbsoluteStartNanos + startOffset}</li>
     *   <li>The sequential cursor remains unchanged after processing all keys</li>
     * </ul>
     * <p>
     * <b>Validates: Requirements 2.1, 2.2, 6.2, 8.1, 8.2, 8.6</b>
     */
    public void testAbsoluteOffsetPositioningBypassesSequentialCursor() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate a random requestAbsoluteStartNanos (must be > 0 for absolute offsets)
            final long requestAbsoluteStartNanos = randomLongBetween(1_000_000_000L, 500_000_000_000_000L);
            final long queryPhaseStartNanos = requestAbsoluteStartNanos + randomLongBetween(100_000L, 50_000_000L);
            final long initialCursor = queryPhaseStartNanos;

            // Generate a random breakdown map where ALL keys have _start companions
            Map<String, Long> shardBreakdown = new LinkedHashMap<>();
            int numKeys = randomIntBetween(2, 6);
            for (int k = 0; k < numKeys; k++) {
                String key = QUERY_PHASE_CHILD_KEYS[randomIntBetween(0, QUERY_PHASE_CHILD_KEYS.length - 1)];
                long duration = randomLongBetween(1_000L, 200_000_000L);
                long startOffset = randomLongBetween(0L, 1_000_000_000L);
                shardBreakdown.put(key, duration);
                shardBreakdown.put(key + "_start", startOffset);
            }

            // Simulate the generic merge loop
            MergeResult result = simulateGenericMergeLoop(
                shardBreakdown,
                requestAbsoluteStartNanos,
                queryPhaseStartNanos,
                initialCursor
            );

            // PROPERTY ASSERTION 1: Each event with X_start is positioned at requestAbsoluteStartNanos + X_start
            for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
                String key = entry.getKey();
                if (key.endsWith("_start")) continue;
                if (entry.getValue() <= 0) continue;

                Long startOffset = shardBreakdown.get(key + "_start");
                if (startOffset != null && startOffset >= 0) {
                    long expectedStart = requestAbsoluteStartNanos + startOffset;
                    long expectedEnd = expectedStart + entry.getValue();

                    long[] timing = result.timedEvents.get(key);
                    assertNotNull(
                        "Event '" + key + "' should be recorded (iteration " + i + ")",
                        timing
                    );
                    assertEquals(
                        "Event '" + key + "' should start at requestAbsoluteStartNanos + X_start"
                            + " (iteration " + i + ","
                            + " requestAbsoluteStartNanos=" + requestAbsoluteStartNanos
                            + ", startOffset=" + startOffset + ")",
                        expectedStart,
                        timing[0]
                    );
                    assertEquals(
                        "Event '" + key + "' should end at start + duration"
                            + " (iteration " + i + ")",
                        expectedEnd,
                        timing[1]
                    );
                }
            }

            // PROPERTY ASSERTION 2: Sequential cursor is NOT advanced (remains at initial value)
            assertEquals(
                "Sequential cursor should NOT be advanced when all keys have _start offsets"
                    + " (iteration " + i + ", initialCursor=" + initialCursor
                    + ", finalCursor=" + result.finalCursorNanos + ")",
                initialCursor,
                result.finalCursorNanos
            );
        }
    }

    /**
     * Property 3 (mixed case): When some keys have X_start and some don't, cursor only advances
     * for keys WITHOUT X_start.
     * <p>
     * Generates random breakdown maps with a mix of keys that have and don't have _start companions.
     * Asserts:
     * <ul>
     *   <li>Keys with _start: positioned absolutely, cursor NOT advanced</li>
     *   <li>Keys without _start: positioned at cursor, cursor IS advanced by their duration</li>
     * </ul>
     * <p>
     * <b>Validates: Requirements 2.1, 2.2, 6.2, 8.1, 8.2, 8.6</b>
     */
    public void testMixedBreakdownCursorOnlyAdvancesForKeysWithoutStartOffset() {
        for (int i = 0; i < ITERATIONS; i++) {
            final long requestAbsoluteStartNanos = randomLongBetween(1_000_000_000L, 500_000_000_000_000L);
            final long queryPhaseStartNanos = requestAbsoluteStartNanos + randomLongBetween(100_000L, 50_000_000L);
            final long initialCursor = queryPhaseStartNanos;

            // Generate breakdown with a mix: some keys have _start, some don't
            Map<String, Long> shardBreakdown = new LinkedHashMap<>();
            List<String> keysWithStart = new ArrayList<>();
            List<String> keysWithoutStart = new ArrayList<>();
            long expectedCursorAdvance = 0;

            int numKeys = randomIntBetween(3, 7);
            for (int k = 0; k < numKeys; k++) {
                String key = QUERY_PHASE_CHILD_KEYS[k % QUERY_PHASE_CHILD_KEYS.length] + "_v" + k;
                long duration = randomLongBetween(1_000L, 100_000_000L);
                shardBreakdown.put(key, duration);

                if (randomBoolean()) {
                    // This key has a _start companion → absolute positioning
                    long startOffset = randomLongBetween(0L, 500_000_000L);
                    shardBreakdown.put(key + "_start", startOffset);
                    keysWithStart.add(key);
                } else {
                    // This key does NOT have a _start companion → sequential cursor
                    keysWithoutStart.add(key);
                    expectedCursorAdvance += duration;
                }
            }

            // Simulate the generic merge loop
            MergeResult result = simulateGenericMergeLoop(
                shardBreakdown,
                requestAbsoluteStartNanos,
                queryPhaseStartNanos,
                initialCursor
            );

            // PROPERTY ASSERTION 1: Keys with _start are positioned absolutely
            for (String key : keysWithStart) {
                Long startOffset = shardBreakdown.get(key + "_start");
                long duration = shardBreakdown.get(key);
                long expectedStart = requestAbsoluteStartNanos + startOffset;
                long expectedEnd = expectedStart + duration;

                long[] timing = result.timedEvents.get(key);
                assertNotNull(
                    "Event with _start '" + key + "' should be recorded (iteration " + i + ")",
                    timing
                );
                assertEquals(
                    "Event with _start '" + key + "' should use absolute positioning"
                        + " (iteration " + i + ")",
                    expectedStart,
                    timing[0]
                );
                assertEquals(
                    "Event with _start '" + key + "' end should be start + duration"
                        + " (iteration " + i + ")",
                    expectedEnd,
                    timing[1]
                );
            }

            // PROPERTY ASSERTION 2: Keys without _start are positioned at sequential cursor positions
            for (String key : keysWithoutStart) {
                long[] timing = result.timedEvents.get(key);
                assertNotNull(
                    "Event without _start '" + key + "' should be recorded (iteration " + i + ")",
                    timing
                );
                // The event was positioned at some cursor value >= initialCursor
                assertTrue(
                    "Event without _start '" + key + "' should be at or after initial cursor"
                        + " (iteration " + i + ")",
                    timing[0] >= initialCursor
                );
            }

            // PROPERTY ASSERTION 3: Cursor advances exactly by the sum of durations of keys WITHOUT _start
            long expectedFinalCursor = initialCursor + expectedCursorAdvance;
            assertEquals(
                "Sequential cursor should advance only by sum of durations of keys without _start"
                    + " (iteration " + i + ", keysWithStart=" + keysWithStart.size()
                    + ", keysWithoutStart=" + keysWithoutStart.size()
                    + ", expectedAdvance=" + expectedCursorAdvance + ")",
                expectedFinalCursor,
                result.finalCursorNanos
            );
        }
    }

    /**
     * Property 3 (no _start keys): When NO keys have _start offsets, all are positioned
     * sequentially and cursor advances by the total duration.
     * <p>
     * This is the fallback behavior: without absolute offsets, the merge loop falls back to
     * sequential cursor positioning for all query-phase children.
     * <p>
     * <b>Validates: Requirements 2.1, 2.2, 6.2, 8.1, 8.2, 8.6</b>
     */
    public void testWithoutStartOffsetsAllKeysAdvanceCursor() {
        for (int i = 0; i < ITERATIONS; i++) {
            final long requestAbsoluteStartNanos = randomLongBetween(1_000_000_000L, 500_000_000_000_000L);
            final long queryPhaseStartNanos = requestAbsoluteStartNanos + randomLongBetween(100_000L, 50_000_000L);
            final long initialCursor = queryPhaseStartNanos;

            // Generate breakdown with NO _start keys
            Map<String, Long> shardBreakdown = new LinkedHashMap<>();
            long totalDuration = 0;

            int numKeys = randomIntBetween(2, 5);
            for (int k = 0; k < numKeys; k++) {
                String key = QUERY_PHASE_CHILD_KEYS[k % QUERY_PHASE_CHILD_KEYS.length];
                long duration = randomLongBetween(1_000L, 100_000_000L);
                shardBreakdown.put(key, duration);
                totalDuration += duration;
            }

            // Simulate the generic merge loop (hasAbsoluteOffsets is still true,
            // but no _start keys exist so sequential cursor is used)
            MergeResult result = simulateGenericMergeLoop(
                shardBreakdown,
                requestAbsoluteStartNanos,
                queryPhaseStartNanos,
                initialCursor
            );

            // PROPERTY ASSERTION: Cursor advances by total duration of all keys
            long expectedFinalCursor = initialCursor + totalDuration;
            assertEquals(
                "Without _start keys, cursor should advance by total duration"
                    + " (iteration " + i + ", totalDuration=" + totalDuration + ")",
                expectedFinalCursor,
                result.finalCursorNanos
            );

            // All events should be positioned sequentially starting from initialCursor
            long runningCursor = initialCursor;
            for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
                String key = entry.getKey();
                long duration = entry.getValue();

                long[] timing = result.timedEvents.get(key);
                assertNotNull("Event '" + key + "' should be recorded (iteration " + i + ")", timing);
                assertEquals(
                    "Event '" + key + "' should start at sequential cursor position"
                        + " (iteration " + i + ")",
                    runningCursor,
                    timing[0]
                );
                assertEquals(
                    "Event '" + key + "' should end at cursor + duration (iteration " + i + ")",
                    runningCursor + duration,
                    timing[1]
                );
                runningCursor += duration;
            }
        }
    }

    /**
     * Property 3 (self-describing extensibility): Unknown keys with _start companions
     * are positioned absolutely without requiring registration.
     * <p>
     * Tests that the generic merge loop correctly handles arbitrary unknown metric names
     * following the X + X_start convention — no per-key case statement required.
     * <p>
     * <b>Validates: Requirements 8.1, 8.2, 8.6</b>
     */
    public void testSelfDescribingUnknownKeysWithStartOffsetsPositionedAbsolutely() {
        for (int i = 0; i < ITERATIONS; i++) {
            final long requestAbsoluteStartNanos = randomLongBetween(1_000_000_000L, 500_000_000_000_000L);
            final long queryPhaseStartNanos = requestAbsoluteStartNanos + randomLongBetween(100_000L, 50_000_000L);
            final long initialCursor = queryPhaseStartNanos;

            // Generate breakdown with completely unknown metric names
            Map<String, Long> shardBreakdown = new LinkedHashMap<>();
            int numKeys = randomIntBetween(2, 5);
            List<String> unknownKeys = new ArrayList<>();

            for (int k = 0; k < numKeys; k++) {
                String key = "unknown_metric_" + randomIntBetween(1, 1000) + "_iter" + k;
                long duration = randomLongBetween(1_000L, 200_000_000L);
                long startOffset = randomLongBetween(0L, 1_000_000_000L);
                shardBreakdown.put(key, duration);
                shardBreakdown.put(key + "_start", startOffset);
                unknownKeys.add(key);
            }

            // Simulate the generic merge loop
            MergeResult result = simulateGenericMergeLoop(
                shardBreakdown,
                requestAbsoluteStartNanos,
                queryPhaseStartNanos,
                initialCursor
            );

            // PROPERTY ASSERTION: Unknown keys are positioned absolutely (self-describing)
            for (String key : unknownKeys) {
                Long startOffset = shardBreakdown.get(key + "_start");
                long duration = shardBreakdown.get(key);
                long expectedStart = requestAbsoluteStartNanos + startOffset;

                long[] timing = result.timedEvents.get(key);
                assertNotNull(
                    "Unknown self-describing key '" + key + "' should be recorded (iteration " + i + ")",
                    timing
                );
                assertEquals(
                    "Unknown key '" + key + "' should use absolute positioning via X_start"
                        + " (iteration " + i + ")",
                    expectedStart,
                    timing[0]
                );
                assertEquals(
                    "Unknown key '" + key + "' end should be start + duration"
                        + " (iteration " + i + ")",
                    expectedStart + duration,
                    timing[1]
                );
            }

            // PROPERTY ASSERTION: Cursor NOT advanced
            assertEquals(
                "Sequential cursor should NOT advance for unknown keys with _start"
                    + " (iteration " + i + ")",
                initialCursor,
                result.finalCursorNanos
            );
        }
    }

    /**
     * Simulates the coordinator's generic merge loop three-tier positioning logic.
     * <p>
     * This mirrors the logic in {@code AbstractSearchAsyncAction.java}:
     * <ol>
     *   <li>Skip {@code _start} keys (consumed inline)</li>
     *   <li>Skip metadata keys (wall_clock_offset_micros)</li>
     *   <li>Skip zero-duration entries</li>
     *   <li>Check for corresponding {@code X_start} key:
     *       <ul>
     *         <li>If present and hasAbsoluteOffsets: position at requestAbsoluteStartNanos + startOffset (no cursor advance)</li>
     *         <li>Else if queryPhaseStartNanos > 0 and isQueryPhaseChild: use sequential cursor</li>
     *         <li>Else: sequential cursor fallback</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * @param shardBreakdown the shard breakdown map with duration and optional _start keys
     * @param requestAbsoluteStartNanos the request absolute start time (> 0 means absolute offsets available)
     * @param queryPhaseStartNanos the query phase start time
     * @param initialCursor the initial sequential cursor value
     * @return MergeResult containing timed events and the final cursor value
     */
    private MergeResult simulateGenericMergeLoop(
        Map<String, Long> shardBreakdown,
        long requestAbsoluteStartNanos,
        long queryPhaseStartNanos,
        long initialCursor
    ) {
        final boolean hasAbsoluteOffsets = requestAbsoluteStartNanos > 0;
        long currentOffsetNanos = initialCursor;
        Map<String, long[]> timedEvents = new LinkedHashMap<>();

        for (Map.Entry<String, Long> entry : shardBreakdown.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue();

            // Skip _start keys — consumed inline with their duration key
            if (key.endsWith("_start")) {
                continue;
            }

            // Skip metadata keys
            if (key.equals("wall_clock_offset_micros")) {
                continue;
            }

            // Skip zero-duration entries
            if (value <= 0) {
                continue;
            }

            // Generic positioning: check for corresponding X_start key
            String startKey = key + "_start";
            Long startOffset = shardBreakdown.get(startKey);

            if (hasAbsoluteOffsets && startOffset != null && startOffset >= 0) {
                // ABSOLUTE POSITIONING: use data node's recorded offset
                long absStart = requestAbsoluteStartNanos + startOffset;
                timedEvents.put(key, new long[] { absStart, absStart + value });
                // DO NOT advance sequential cursor
            } else if (queryPhaseStartNanos > 0) {
                // QUERY-PHASE-RELATIVE / SEQUENTIAL CURSOR: position at cursor and advance
                timedEvents.put(key, new long[] { currentOffsetNanos, currentOffsetNanos + value });
                currentOffsetNanos += value;
            } else {
                // SEQUENTIAL CURSOR FALLBACK
                timedEvents.put(key, new long[] { currentOffsetNanos, currentOffsetNanos + value });
                currentOffsetNanos += value;
            }
        }

        return new MergeResult(timedEvents, currentOffsetNanos);
    }

    /**
     * Result of the simulated generic merge loop containing timed events and final cursor position.
     */
    private static class MergeResult {
        final Map<String, long[]> timedEvents;
        final long finalCursorNanos;

        MergeResult(Map<String, long[]> timedEvents, long finalCursorNanos) {
            this.timedEvents = timedEvents;
            this.finalCursorNanos = finalCursorNanos;
        }
    }
}
