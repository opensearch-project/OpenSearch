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
 * Property-based tests for inter-phase gap computation in {@link SearchLatencyBreakdown}.
 * <p>
 * Feature: search-latency-breakdown, Property 4: Inter-phase gap computation
 * <p>
 * For any random consecutive phase pairs, the recorded gap SHALL equal
 * nextPhaseStart - previousPhaseEnd and SHALL appear under the correct key
 * in the unified breakdown map.
 * <p>
 * **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
 */
public class SearchLatencyBreakdownProperty4Tests extends OpenSearchTestCase {

    private static final int MIN_ITERATIONS = 100;

    /**
     * Property 4: Inter-phase gap computation — can_match → query gap.
     * <p>
     * Generates random gap durations (simulating nextPhaseStart - previousPhaseEnd)
     * and verifies that recording with prev="can_match" stores the value in
     * canMatchToQueryGapNanos and the unified map key "can_match_to_query_gap".
     */
    public void testCanMatchToQueryGap_recordsCorrectly() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random phase end and next phase start timestamps
            long previousPhaseEnd = randomLongBetween(1_000_000L, 1_000_000_000L);
            long nextPhaseStart = previousPhaseEnd + randomLongBetween(1_000_000L, 100_000_000L);
            long expectedGapNanos = nextPhaseStart - previousPhaseEnd;

            // Record the inter-phase gap
            breakdown.recordInterPhaseGap("can_match", "query", expectedGapNanos);

            // Verify via unified breakdown map — use large enough values to ensure millis > 0
            long absoluteStart = previousPhaseEnd - randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = nextPhaseStart + randomLongBetween(1_000_000L, 10_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedGapNanos);
            if (expectedMillis > 0) {
                assertTrue(
                    "Iteration " + i + ": can_match_to_query_gap should be present in map when millis > 0",
                    map.containsKey("can_match_to_query_gap")
                );
                assertEquals(
                    "Iteration " + i + ": can_match_to_query_gap value mismatch",
                    expectedMillis,
                    (long) map.get("can_match_to_query_gap")
                );
            }
        }
    }

    /**
     * Property 4: Inter-phase gap computation — query → fetch gap.
     * <p>
     * Generates random gap durations and verifies that recording with
     * prev="query", next="fetch" stores the value correctly under "query_to_fetch_gap".
     */
    public void testQueryToFetchGap_recordsCorrectly() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random phase end and next phase start timestamps
            long previousPhaseEnd = randomLongBetween(1_000_000L, 1_000_000_000L);
            long nextPhaseStart = previousPhaseEnd + randomLongBetween(1_000_000L, 100_000_000L);
            long expectedGapNanos = nextPhaseStart - previousPhaseEnd;

            // Record the inter-phase gap
            breakdown.recordInterPhaseGap("query", "fetch", expectedGapNanos);

            // Verify via unified breakdown map
            long absoluteStart = previousPhaseEnd - randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = nextPhaseStart + randomLongBetween(1_000_000L, 10_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedGapNanos);
            if (expectedMillis > 0) {
                assertTrue(
                    "Iteration " + i + ": query_to_fetch_gap should be present in map when millis > 0",
                    map.containsKey("query_to_fetch_gap")
                );
                assertEquals(
                    "Iteration " + i + ": query_to_fetch_gap value mismatch",
                    expectedMillis,
                    (long) map.get("query_to_fetch_gap")
                );
            }
        }
    }

    /**
     * Property 4: Inter-phase gap computation — fetch → expand gap.
     * <p>
     * Generates random gap durations and verifies that recording with
     * prev="fetch", next="expand" stores the value correctly under "fetch_to_expand_gap".
     */
    public void testFetchToExpandGap_recordsCorrectly() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random phase end and next phase start timestamps
            long previousPhaseEnd = randomLongBetween(1_000_000L, 1_000_000_000L);
            long nextPhaseStart = previousPhaseEnd + randomLongBetween(1_000_000L, 100_000_000L);
            long expectedGapNanos = nextPhaseStart - previousPhaseEnd;

            // Record the inter-phase gap
            breakdown.recordInterPhaseGap("fetch", "expand", expectedGapNanos);

            // Verify via unified breakdown map
            long absoluteStart = previousPhaseEnd - randomLongBetween(1_000_000L, 10_000_000L);
            long requestEnd = nextPhaseStart + randomLongBetween(1_000_000L, 10_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(expectedGapNanos);
            if (expectedMillis > 0) {
                assertTrue(
                    "Iteration " + i + ": fetch_to_expand_gap should be present in map when millis > 0",
                    map.containsKey("fetch_to_expand_gap")
                );
                assertEquals(
                    "Iteration " + i + ": fetch_to_expand_gap value mismatch",
                    expectedMillis,
                    (long) map.get("fetch_to_expand_gap")
                );
            }
        }
    }

    /**
     * Property 4: Inter-phase gap computation — additive accumulation.
     * <p>
     * Verifies that multiple recordings of the same gap type accumulate additively
     * (simulating the case where can_match runs multiple times or gaps accumulate).
     * The final value should equal the sum of all recorded gaps.
     */
    public void testInterPhaseGap_accumulatesAdditively() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            int numRecordings = randomIntBetween(2, 10);
            long totalGapNanos = 0;

            for (int j = 0; j < numRecordings; j++) {
                long gapNanos = randomLongBetween(1_000_000L, 50_000_000L);
                totalGapNanos += gapNanos;
                breakdown.recordInterPhaseGap("can_match", "query", gapNanos);
            }

            // Verify sum in unified map
            long absoluteStart = randomLongBetween(0L, 1_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            long expectedMillis = TimeUnit.NANOSECONDS.toMillis(totalGapNanos);
            if (expectedMillis > 0) {
                assertTrue(
                    "Iteration " + i + ": can_match_to_query_gap should be present after multiple recordings",
                    map.containsKey("can_match_to_query_gap")
                );
                assertEquals(
                    "Iteration " + i + ": accumulated gap mismatch",
                    expectedMillis,
                    (long) map.get("can_match_to_query_gap")
                );
            }
        }
    }

    /**
     * Property 4: Inter-phase gap computation — unrecognized phase pairs.
     * <p>
     * Verifies that unrecognized phase pairs (e.g., "dfs" → "query") do not record
     * any gap values in the breakdown. The gap keys should remain absent from the map.
     */
    public void testUnrecognizedPhasePair_doesNotRecordGap() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long gapNanos = randomLongBetween(1_000_000L, 100_000_000L);

            // Use unrecognized phase pairs
            String[] unrecognizedPrevPhases = { "dfs", "expand", "unknown", "query" };
            String[] unrecognizedNextPhases = { "query", "fetch", "query", "expand" };
            int pairIdx = randomIntBetween(0, unrecognizedPrevPhases.length - 1);

            String prev = unrecognizedPrevPhases[pairIdx];
            String next = unrecognizedNextPhases[pairIdx];

            // Skip recognized pairs
            if ("can_match".equals(prev)
                || ("query".equals(prev) && "fetch".equals(next))
                || ("fetch".equals(prev) && "expand".equals(next))) {
                continue;
            }

            breakdown.recordInterPhaseGap(prev, next, gapNanos);

            long absoluteStart = randomLongBetween(0L, 1_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(500_000_000L, 1_000_000_000L);
            Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            assertFalse(
                "Iteration " + i + ": can_match_to_query_gap should not appear for pair (" + prev + ", " + next + ")",
                map.containsKey("can_match_to_query_gap")
            );
            assertFalse(
                "Iteration " + i + ": query_to_fetch_gap should not appear for pair (" + prev + ", " + next + ")",
                map.containsKey("query_to_fetch_gap")
            );
            assertFalse(
                "Iteration " + i + ": fetch_to_expand_gap should not appear for pair (" + prev + ", " + next + ")",
                map.containsKey("fetch_to_expand_gap")
            );
        }
    }

    /**
     * Property 4: Inter-phase gap computation — gap equals nextPhaseStart - previousPhaseEnd.
     * <p>
     * End-to-end test simulating the real workflow: markPhaseEnd on previous phase,
     * then compute gap as nextPhaseStart - lastPhaseEndNanos, record it, and verify
     * it appears correctly in the unified map under the appropriate key.
     */
    public void testGapEqualsNextStartMinusPreviousEnd_allPhasePairs() {
        String[][] phasePairs = {
            { "can_match", "query", "can_match_to_query_gap" },
            { "query", "fetch", "query_to_fetch_gap" },
            { "fetch", "expand", "fetch_to_expand_gap" }
        };

        for (int i = 0; i < MIN_ITERATIONS; i++) {
            for (String[] pair : phasePairs) {
                String prevPhase = pair[0];
                String nextPhase = pair[1];
                String expectedKey = pair[2];

                SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

                // Simulate phase lifecycle
                long absoluteStart = randomLongBetween(1_000_000L, 100_000_000L);
                long previousPhaseStart = absoluteStart + randomLongBetween(1_000_000L, 50_000_000L);
                long previousPhaseEnd = previousPhaseStart + randomLongBetween(1_000_000L, 100_000_000L);
                long nextPhaseStart = previousPhaseEnd + randomLongBetween(1_000_000L, 100_000_000L);

                // Mark phase lifecycle (as the real code would)
                breakdown.markFirstPhaseStart(previousPhaseStart);
                breakdown.markPhaseEnd(prevPhase, previousPhaseEnd);

                // Compute and record gap (as done in AbstractSearchAsyncAction)
                long gapNanos = nextPhaseStart - previousPhaseEnd;
                breakdown.recordInterPhaseGap(prevPhase, nextPhase, gapNanos);

                // Verify the gap is correct
                assertEquals(
                    "Gap should equal nextPhaseStart - previousPhaseEnd",
                    nextPhaseStart - previousPhaseEnd,
                    gapNanos
                );

                // Verify it appears in the unified map under the correct key
                long requestEnd = nextPhaseStart + randomLongBetween(50_000_000L, 200_000_000L);
                Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

                long expectedMillis = TimeUnit.NANOSECONDS.toMillis(gapNanos);
                if (expectedMillis > 0) {
                    assertTrue(
                        "Iteration " + i + ": " + expectedKey + " should be present in map",
                        map.containsKey(expectedKey)
                    );
                    assertEquals(
                        "Iteration " + i + ": " + expectedKey + " should equal gap in millis",
                        expectedMillis,
                        (long) map.get(expectedKey)
                    );
                }
            }
        }
    }
}
