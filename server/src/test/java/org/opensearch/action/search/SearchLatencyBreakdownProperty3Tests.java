/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based tests for pre-phase and post-phase overhead computation in
 * {@link SearchLatencyBreakdown}.
 *
 * <p><b>Property 3: Pre-phase and post-phase overhead computation</b></p>
 * <p>For any absoluteStartNanos, firstPhaseStartNanos, lastPhaseEndNanos, and requestEndNanos
 * where absoluteStartNanos &lt;= firstPhaseStartNanos &lt;= lastPhaseEndNanos &lt;= requestEndNanos,
 * the computed pre_phase_overhead SHALL equal firstPhaseStartNanos - absoluteStartNanos and
 * the computed post_phase_overhead SHALL equal requestEndNanos - lastPhaseEndNanos. Both values
 * SHALL be non-negative.</p>
 *
 * <p><b>Validates: Requirements 2.4, 2.5</b></p>
 *
 * Feature: search-latency-breakdown, Property 3: Pre/post-phase overhead computation
 */
public class SearchLatencyBreakdownProperty3Tests extends OpenSearchTestCase {

    private static final int ITERATIONS = 100;

    /**
     * Property 3: For random valid timestamp orderings
     * (absoluteStart &lt;= firstPhaseStart &lt;= lastPhaseEnd &lt;= requestEnd),
     * computePrePhaseOverhead returns firstPhaseStartNanos - absoluteStartNanos
     * and computePostPhaseOverhead returns requestEndNanos - lastPhaseEndNanos.
     * Both values are non-negative.
     *
     * Validates: Requirements 2.4, 2.5
     */
    public void testPrePhaseAndPostPhaseOverheadComputation() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate four ordered timestamps: absoluteStart <= firstPhaseStart <= lastPhaseEnd <= requestEnd
            long absoluteStart = randomLongBetween(1_000_000L, 1_000_000_000L);
            long firstPhaseStart = absoluteStart + randomLongBetween(0, 100_000_000L);
            long lastPhaseEnd = firstPhaseStart + randomLongBetween(0, 500_000_000L);
            long requestEnd = lastPhaseEnd + randomLongBetween(0, 100_000_000L);

            // Set up the breakdown state
            breakdown.markFirstPhaseStart(firstPhaseStart);
            breakdown.markPhaseEnd("query", lastPhaseEnd);

            // Compute overheads
            long prePhaseOverhead = breakdown.computePrePhaseOverhead(absoluteStart);
            long postPhaseOverhead = breakdown.computePostPhaseOverhead(requestEnd);

            // Verify pre-phase overhead equals the expected difference
            long expectedPreOverhead = firstPhaseStart - absoluteStart;
            assertEquals(
                "Pre-phase overhead should equal firstPhaseStartNanos - absoluteStartNanos (iteration " + i + ")",
                expectedPreOverhead,
                prePhaseOverhead
            );

            // Verify post-phase overhead equals the expected difference
            long expectedPostOverhead = requestEnd - lastPhaseEnd;
            assertEquals(
                "Post-phase overhead should equal requestEndNanos - lastPhaseEndNanos (iteration " + i + ")",
                expectedPostOverhead,
                postPhaseOverhead
            );

            // Verify both are non-negative
            assertTrue(
                "Pre-phase overhead must be non-negative (iteration " + i + "): got " + prePhaseOverhead,
                prePhaseOverhead >= 0
            );
            assertTrue(
                "Post-phase overhead must be non-negative (iteration " + i + "): got " + postPhaseOverhead,
                postPhaseOverhead >= 0
            );
        }
    }

    /**
     * Property 3 edge case: When firstPhaseStart == absoluteStart, pre-phase overhead is 0.
     * When requestEnd == lastPhaseEnd, post-phase overhead is 0.
     *
     * Validates: Requirements 2.4, 2.5
     */
    public void testOverheadIsZeroWhenTimestampsAreEqual() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long timestamp = randomLongBetween(1_000_000L, 1_000_000_000L);
            long phaseEnd = timestamp + randomLongBetween(0, 500_000_000L);

            // firstPhaseStart == absoluteStart → pre overhead should be 0
            breakdown.markFirstPhaseStart(timestamp);
            breakdown.markPhaseEnd("fetch", phaseEnd);

            long prePhaseOverhead = breakdown.computePrePhaseOverhead(timestamp);
            assertEquals(
                "Pre-phase overhead should be 0 when firstPhaseStart == absoluteStart (iteration " + i + ")",
                0L,
                prePhaseOverhead
            );

            // requestEnd == lastPhaseEnd → post overhead should be 0
            long postPhaseOverhead = breakdown.computePostPhaseOverhead(phaseEnd);
            assertEquals(
                "Post-phase overhead should be 0 when requestEnd == lastPhaseEnd (iteration " + i + ")",
                0L,
                postPhaseOverhead
            );

            // Both must still be non-negative
            assertTrue("Pre-phase overhead must be non-negative", prePhaseOverhead >= 0);
            assertTrue("Post-phase overhead must be non-negative", postPhaseOverhead >= 0);
        }
    }

    /**
     * Property 3: Verifies that computePrePhaseOverhead clamps to 0 when a timing
     * anomaly produces absoluteStart > firstPhaseStart (Math.max(0, ...) behavior).
     *
     * Validates: Requirements 2.4, 2.5
     */
    public void testPrePhaseOverheadClampedToZeroOnTimingAnomaly() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Simulate timing anomaly: absoluteStart comes after firstPhaseStart
            long firstPhaseStart = randomLongBetween(1_000_000L, 500_000_000L);
            long absoluteStart = firstPhaseStart + randomLongBetween(1, 100_000_000L);

            breakdown.markFirstPhaseStart(firstPhaseStart);

            long prePhaseOverhead = breakdown.computePrePhaseOverhead(absoluteStart);

            // Math.max(0, ...) should clamp to 0
            assertEquals(
                "Pre-phase overhead should be clamped to 0 on timing anomaly (iteration " + i + ")",
                0L,
                prePhaseOverhead
            );
            assertTrue("Pre-phase overhead must be non-negative", prePhaseOverhead >= 0);
        }
    }

    /**
     * Property 3: Verifies that computePostPhaseOverhead clamps to 0 when a timing
     * anomaly produces requestEnd &lt; lastPhaseEnd (Math.max(0, ...) behavior).
     *
     * Validates: Requirements 2.4, 2.5
     */
    public void testPostPhaseOverheadClampedToZeroOnTimingAnomaly() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Simulate timing anomaly: requestEnd comes before lastPhaseEnd
            long lastPhaseEnd = randomLongBetween(100_000_000L, 1_000_000_000L);
            long requestEnd = lastPhaseEnd - randomLongBetween(1, 100_000_000L);

            breakdown.markPhaseEnd("query", lastPhaseEnd);

            long postPhaseOverhead = breakdown.computePostPhaseOverhead(requestEnd);

            // Math.max(0, ...) should clamp to 0
            assertEquals(
                "Post-phase overhead should be clamped to 0 on timing anomaly (iteration " + i + ")",
                0L,
                postPhaseOverhead
            );
            assertTrue("Post-phase overhead must be non-negative", postPhaseOverhead >= 0);
        }
    }

    /**
     * Property 3: markFirstPhaseStart only sets the timestamp once (first call wins).
     * Verifies that computePrePhaseOverhead uses the first phase start, not subsequent calls.
     *
     * Validates: Requirements 2.4, 2.5
     */
    public void testMarkFirstPhaseStartOnlyRecordsFirst() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absoluteStart = randomLongBetween(1_000_000L, 500_000_000L);
            long firstPhaseStart = absoluteStart + randomLongBetween(1, 50_000_000L);
            long secondPhaseStart = firstPhaseStart + randomLongBetween(1, 50_000_000L);

            // First call sets the value
            breakdown.markFirstPhaseStart(firstPhaseStart);
            // Second call should be ignored (only if currently 0)
            breakdown.markFirstPhaseStart(secondPhaseStart);

            long prePhaseOverhead = breakdown.computePrePhaseOverhead(absoluteStart);

            // Should use the first value, not the second
            long expectedOverhead = firstPhaseStart - absoluteStart;
            assertEquals(
                "Pre-phase overhead should use the first marked phase start (iteration " + i + ")",
                expectedOverhead,
                prePhaseOverhead
            );
        }
    }
}
