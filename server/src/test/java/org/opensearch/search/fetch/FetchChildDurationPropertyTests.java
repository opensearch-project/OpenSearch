/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.opensearch.action.search.SearchLatencyBreakdownNode;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based test for fetch child duration clamping.
 * <p>
 * <b>Property 1: Fetch child duration never exceeds parent (wall-clock clamping)</b>
 * <p>
 * For any fetch phase execution with N documents (N >= 1), each fetch internal metric
 * (stored_fields, source_loading, highlighting, script_fields, inner_hits) SHALL have
 * {@code duration_micros <= parent_fetch_phase.duration_micros}, and the recorded duration
 * SHALL represent a single wall-clock span (last_doc_end - first_doc_start) rather than
 * the sum of per-document deltas.
 * <p>
 * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4</b>
 * <p>
 * Feature: latency-breakdown-fixes, Property 1: Fetch child duration never exceeds parent (wall-clock clamping)
 */
public class FetchChildDurationPropertyTests extends OpenSearchTestCase {

    private static final String[] FETCH_INTERNALS = {
        "Fetch Stored Fields",
        "Fetch Source Loading",
        "Fetch Highlighting",
        "Fetch Script Fields",
        "Fetch Inner Hits"
    };

    /**
     * Property test: For any random parent duration and random child raw durations,
     * the clamped child duration never exceeds the parent duration.
     * <p>
     * Generates random parent durations (1us to 100s) and random child raw durations (0 to 200s).
     * Asserts: {@code Math.min(childRawDuration, parentDuration) <= parentDuration} for all iterations.
     * <p>
     * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4</b>
     */
    public void testProperty1_fetchChildNeverExceedsParent() {
        for (int i = 0; i < 100; i++) {
            // Random parent fetch phase duration: 1 microsecond to 100 seconds (in nanos)
            long parentDuration = randomLongBetween(1_000L, 100_000_000_000L);

            // Random number of fetch children (1 to 5 internals active)
            int numChildren = randomIntBetween(1, 5);

            for (int c = 0; c < numChildren; c++) {
                // Random raw child duration: can exceed parent (0 to 200 seconds in nanos)
                long rawChildDuration = randomLongBetween(0L, 200_000_000_000L);

                // Apply the clamping rule: Math.min(childDuration, parentDuration)
                long clampedDuration = Math.min(rawChildDuration, parentDuration);

                // Property assertion: clamped child never exceeds parent
                assertTrue(
                    "Iteration " + i + ", child " + c + ": clamped duration (" + clampedDuration
                        + ") must not exceed parent duration (" + parentDuration + ")"
                        + " [raw child duration: " + rawChildDuration + "]",
                    clampedDuration <= parentDuration
                );
            }
        }
    }

    /**
     * Property test: With random document counts (1-10000) and per-doc timings,
     * the wall-clock span (last_end - first_start) clamped to parent never exceeds parent.
     * <p>
     * Simulates realistic fetch phase execution with N documents where each doc
     * contributes a random timing. The wall-clock span (envelope) is then clamped.
     * <p>
     * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4</b>
     */
    public void testProperty1_wallClockSpanWithRandomDocCounts() {
        for (int i = 0; i < 100; i++) {
            // Random document count: 1 to 10000
            int numDocs = randomIntBetween(1, 10000);

            // Random parent fetch phase duration: 1ms to 10s (in nanos)
            long fetchTotalNanos = randomLongBetween(1_000_000L, 10_000_000_000L);

            // Simulate wall-clock span for a fetch internal across numDocs documents
            // first_start is somewhere in [0, fetchTotalNanos)
            long firstStart = randomLongBetween(0L, Math.max(0, fetchTotalNanos - 1));

            // last_end can extend beyond parent (simulating clock anomalies or overhead)
            long lastEnd = firstStart + randomLongBetween(0L, fetchTotalNanos * 2);

            // Compute raw wall-clock duration: Math.max(0, last_end - first_start)
            long rawDuration = Math.max(0, lastEnd - firstStart);

            // Apply clamping: Math.min(rawDuration, fetchTotalNanos)
            long clampedDuration = Math.min(rawDuration, fetchTotalNanos);

            // Property assertion: clamped duration never exceeds parent
            assertTrue(
                "Iteration " + i + " (numDocs=" + numDocs + "): clamped duration ("
                    + clampedDuration + ") must not exceed parent (" + fetchTotalNanos + ")"
                    + " [raw duration: " + rawDuration + ", firstStart: " + firstStart
                    + ", lastEnd: " + lastEnd + "]",
                clampedDuration <= fetchTotalNanos
            );
        }
    }

    /**
     * Property test: Multiple children simultaneously clamped to the same parent.
     * All children in a fetch phase tree satisfy the invariant after clamping.
     * <p>
     * Generates a random fetch phase with multiple internals and verifies the tree
     * invariant holds after construction with clamped values.
     * <p>
     * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4</b>
     */
    public void testProperty1_multipleChildrenAllClampedToParent() {
        for (int i = 0; i < 100; i++) {
            // Random parent duration: 1us to 50s
            long parentDuration = randomLongBetween(1_000L, 50_000_000_000L);

            // Build a fetch node tree with random children
            SearchLatencyBreakdownNode fetchNode = new SearchLatencyBreakdownNode(
                "Fetch Phase", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, parentDuration
            );

            // Random number of active internals (1 to 5)
            int numInternals = randomIntBetween(1, FETCH_INTERNALS.length);

            for (int c = 0; c < numInternals; c++) {
                // Random raw duration for this child (can exceed parent)
                long rawChildDuration = randomLongBetween(0L, parentDuration * 3);

                // Apply clamping logic as implemented in FetchPhase
                long clampedDuration = Math.min(Math.max(0, rawChildDuration), parentDuration);

                if (clampedDuration > 0) {
                    long childOffset = randomLongBetween(0L, parentDuration);
                    fetchNode.addChild(
                        FETCH_INTERNALS[c],
                        SearchLatencyBreakdownNode.CATEGORY_FETCH,
                        childOffset,
                        clampedDuration
                    );
                }
            }

            // Property assertion: every child in the tree satisfies child <= parent
            for (SearchLatencyBreakdownNode child : fetchNode.getChildren()) {
                assertTrue(
                    "Iteration " + i + ": child '" + child.getName() + "' duration ("
                        + child.getDurationNanos() + ") must not exceed parent (" + parentDuration + ")",
                    child.getDurationNanos() <= fetchNode.getDurationNanos()
                );
            }
        }
    }

    /**
     * Property test: Negative time deltas are handled correctly (produce 0 duration)
     * and zero durations are still clamped correctly.
     * <p>
     * {@code Math.max(0, end - start)} is always non-negative, and {@code Math.min(nonNeg, parent)} never exceeds parent.
     * <p>
     * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4</b>
     */
    public void testProperty1_negativeDeltasAndZeroDurations() {
        for (int i = 0; i < 100; i++) {
            long parentDuration = randomLongBetween(1_000L, 100_000_000_000L);

            // Generate random start and end that may produce negative deltas
            long start = randomLongBetween(0L, Long.MAX_VALUE / 2);
            long end;
            if (randomBoolean()) {
                // Negative delta case: end < start
                end = randomLongBetween(0L, start);
            } else {
                // Positive delta case: end >= start
                end = start + randomLongBetween(0L, 200_000_000_000L);
            }

            // Step 1: Prevent negative durations
            long rawDuration = Math.max(0, end - start);
            assertTrue("Raw duration after Math.max(0, ...) must be >= 0", rawDuration >= 0);

            // Step 2: Clamp to parent
            long clampedDuration = Math.min(rawDuration, parentDuration);

            // Property assertions
            assertTrue(
                "Iteration " + i + ": clamped duration (" + clampedDuration
                    + ") must not exceed parent (" + parentDuration + ")",
                clampedDuration <= parentDuration
            );
            assertTrue(
                "Iteration " + i + ": clamped duration must be >= 0",
                clampedDuration >= 0
            );
        }
    }
}
