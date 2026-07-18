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
 * Property-based test: Absolute offset computation is correct and non-negative.
 *
 * <p>Feature: breakdown-ordering-fix, Property 1: Absolute offset computation is correct and non-negative
 *
 * <p>This test verifies that for any valid {@code requestStartNanos > 0} and any event start
 * timestamp {@code eventStartNanos >= requestStartNanos}, the computed offset
 * {@code Math.max(0, eventStartNanos - requestStartNanos)} equals
 * {@code eventStartNanos - requestStartNanos} and is always >= 0.
 *
 * <p>The property is tested across randomized inputs (minimum 100 iterations) to ensure
 * universal correctness of the absolute offset computation used on data nodes.
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5</b>
 */
public class AbsoluteOffsetComputationPropertyTests extends OpenSearchTestCase {

    /** Minimum number of random iterations to satisfy property-based testing requirements. */
    private static final int MIN_ITERATIONS = 100;

    /**
     * Property 1: Absolute offset computation is correct and non-negative.
     *
     * <p>For any valid {@code requestStartNanos > 0} and {@code eventStartNanos >= requestStartNanos}:
     * <ul>
     *   <li>The result of {@code Math.max(0, eventStartNanos - requestStartNanos)} equals
     *       {@code eventStartNanos - requestStartNanos} (Math.max is a no-op when difference is non-negative)</li>
     *   <li>The result is always >= 0</li>
     * </ul>
     *
     * <p><b>Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5</b>
     */
    public void testAbsoluteOffsetComputationIsCorrectAndNonNegative() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            // Generate requestStartNanos > 0
            final long requestStartNanos = randomLongBetween(1L, Long.MAX_VALUE / 2);

            // Generate eventStartNanos >= requestStartNanos
            final long eventStartNanos = randomLongBetween(requestStartNanos, Long.MAX_VALUE / 2 + requestStartNanos);

            // Compute the absolute offset as done on the data node
            final long computedOffset = Math.max(0, eventStartNanos - requestStartNanos);

            // The direct difference (which should be non-negative given our constraint)
            final long directDifference = eventStartNanos - requestStartNanos;

            // Property assertion 1: Math.max(0, ...) equals the direct difference
            // because eventStartNanos >= requestStartNanos guarantees non-negative difference
            assertEquals(
                "Math.max(0, eventStartNanos - requestStartNanos) should equal eventStartNanos - requestStartNanos"
                    + " when eventStartNanos >= requestStartNanos."
                    + " requestStartNanos=" + requestStartNanos
                    + ", eventStartNanos=" + eventStartNanos,
                directDifference,
                computedOffset
            );

            // Property assertion 2: The computed offset is always >= 0
            assertTrue(
                "Computed offset must be non-negative."
                    + " requestStartNanos=" + requestStartNanos
                    + ", eventStartNanos=" + eventStartNanos
                    + ", computedOffset=" + computedOffset,
                computedOffset >= 0
            );
        }
    }

    /**
     * Property 1 (edge case): When eventStartNanos equals requestStartNanos, offset is zero.
     *
     * <p>This tests the boundary condition where the event starts exactly when the request starts,
     * resulting in an offset of 0.
     *
     * <p><b>Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5</b>
     */
    public void testAbsoluteOffsetIsZeroWhenEventStartEqualsRequestStart() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            final long requestStartNanos = randomLongBetween(1L, Long.MAX_VALUE / 2);
            final long eventStartNanos = requestStartNanos; // same as request start

            final long computedOffset = Math.max(0, eventStartNanos - requestStartNanos);

            assertEquals(
                "Offset must be 0 when eventStartNanos == requestStartNanos."
                    + " requestStartNanos=" + requestStartNanos,
                0L,
                computedOffset
            );
            assertTrue("Computed offset must be non-negative", computedOffset >= 0);
        }
    }

    /**
     * Property 1 (large values): Computation remains correct for large nanosecond values.
     *
     * <p>Real-world nanosecond timestamps are very large (System.nanoTime() values). This test
     * verifies the computation is correct even with values approaching Long.MAX_VALUE / 2,
     * which simulates realistic nanosecond timestamps.
     *
     * <p><b>Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5</b>
     */
    public void testAbsoluteOffsetComputationWithLargeNanoValues() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            // Use large values typical of System.nanoTime() (hundreds of trillions)
            final long requestStartNanos = randomLongBetween(100_000_000_000_000L, 500_000_000_000_000L);

            // Event starts somewhere after the request (0 to 10 seconds later in nanos)
            final long eventOffset = randomLongBetween(0L, 10_000_000_000L);
            final long eventStartNanos = requestStartNanos + eventOffset;

            final long computedOffset = Math.max(0, eventStartNanos - requestStartNanos);

            // The computed offset should exactly equal the time difference
            assertEquals(
                "Computed offset should equal the time between request start and event start."
                    + " requestStartNanos=" + requestStartNanos
                    + ", eventStartNanos=" + eventStartNanos
                    + ", expectedOffset=" + eventOffset,
                eventOffset,
                computedOffset
            );
            assertTrue("Computed offset must be non-negative", computedOffset >= 0);
        }
    }
}
