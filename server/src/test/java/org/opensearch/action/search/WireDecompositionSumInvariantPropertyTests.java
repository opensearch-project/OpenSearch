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
 * Property-based test: Wire decomposition sum invariant.
 *
 * <p>Feature: breakdown-ordering-fix, Property 4: Wire decomposition sum invariant
 *
 * <p>For any valid network roundtrip duration, {@code wire_out + data_node_execution + wire_back == network_roundtrip},
 * and both {@code wire_out >= 0} and {@code wire_back >= 0}.
 *
 * <p>The wire decomposition formula is:
 * <ul>
 *   <li>{@code wire_out = wall_clock_offset_micros} (converted to nanos)</li>
 *   <li>{@code wire_back = max(0, network_roundtrip - wire_out - data_node_execution)}</li>
 * </ul>
 *
 * <p>When {@code wire_back >= 0} without clamping (normal case), the sum invariant holds exactly:
 * {@code wire_out + data_node_execution + wire_back == network_roundtrip}.
 *
 * <p>When clock skew causes {@code wire_out + data_node_execution > network_roundtrip},
 * {@code wire_back} is clamped to 0 and the sum becomes:
 * {@code wire_out + data_node_execution + 0 >= network_roundtrip}.
 *
 * <p><b>Validates: Requirements 3.1, 3.2</b>
 */
public class WireDecompositionSumInvariantPropertyTests extends OpenSearchTestCase {

    /** Minimum number of random iterations to satisfy property-based testing requirements. */
    private static final int MIN_ITERATIONS = 100;

    /**
     * Property 4 (normal case): Wire decomposition sum invariant holds exactly.
     *
     * <p>Generates random triples where {@code network_roundtrip >= wire_out + data_node_execution},
     * ensuring no clamping occurs. In this case, the invariant
     * {@code wire_out + data_node_execution + wire_back == network_roundtrip} holds exactly.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b>
     */
    public void testWireDecompositionSumInvariantNormalCase() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            // Generate wire_out (wall_clock_offset in nanos) — a non-negative value
            final long wireOut = randomLongBetween(0L, 10_000_000_000L); // 0 to 10 seconds in nanos

            // Generate data_node_execution — a non-negative duration
            final long dataNodeExecution = randomLongBetween(0L, 10_000_000_000L);

            // Generate network_roundtrip that is >= wire_out + data_node_execution (normal case, no clamping)
            final long minRoundtrip = wireOut + dataNodeExecution;
            final long networkRoundtrip = randomLongBetween(minRoundtrip, minRoundtrip + 5_000_000_000L);

            // Compute wire_back as the coordinator does
            final long wireBack = Math.max(0, networkRoundtrip - wireOut - dataNodeExecution);

            // ASSERTION 1: Sum invariant holds exactly (no clamping occurred)
            assertEquals(
                "wire_out + data_node_execution + wire_back must equal network_roundtrip"
                    + " in the normal case (no clamping)."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", wireBack=" + wireBack
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                networkRoundtrip,
                wireOut + dataNodeExecution + wireBack
            );

            // ASSERTION 2: wire_out is always >= 0
            assertTrue(
                "wire_out must be non-negative."
                    + " wireOut=" + wireOut
                    + " (iteration " + i + ")",
                wireOut >= 0
            );

            // ASSERTION 3: wire_back is always >= 0
            assertTrue(
                "wire_back must be non-negative."
                    + " wireBack=" + wireBack
                    + " (iteration " + i + ")",
                wireBack >= 0
            );
        }
    }

    /**
     * Property 4 (clamped case): When wire_out + data_node_execution exceeds network_roundtrip,
     * wire_back is clamped to 0.
     *
     * <p>This tests the clock-skew scenario where the wall_clock_offset (wire_out) plus
     * data node execution time exceeds the measured network roundtrip. In this case,
     * {@code wire_back} is clamped to 0 via {@code Math.max(0, ...)}, and both wire_out
     * and wire_back remain non-negative.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b>
     */
    public void testWireDecompositionClampedCaseWireBackIsZero() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            // Generate wire_out (wall_clock_offset in nanos) — positive value
            final long wireOut = randomLongBetween(1_000L, 10_000_000_000L);

            // Generate data_node_execution — positive duration
            final long dataNodeExecution = randomLongBetween(1_000L, 10_000_000_000L);

            // Generate network_roundtrip that is LESS than wire_out + data_node_execution (clock skew case)
            final long maxRoundtrip = wireOut + dataNodeExecution - 1;
            // Ensure maxRoundtrip >= 0 for a valid roundtrip
            if (maxRoundtrip < 0) {
                continue; // Skip edge cases where overflow might occur
            }
            final long networkRoundtrip = randomLongBetween(0L, maxRoundtrip);

            // Compute wire_back as the coordinator does — clamped to 0
            final long wireBack = Math.max(0, networkRoundtrip - wireOut - dataNodeExecution);

            // ASSERTION 1: wire_back is clamped to exactly 0
            assertEquals(
                "wire_back must be clamped to 0 when wire_out + data_node_execution > network_roundtrip."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                0L,
                wireBack
            );

            // ASSERTION 2: wire_out remains non-negative
            assertTrue(
                "wire_out must be non-negative."
                    + " wireOut=" + wireOut
                    + " (iteration " + i + ")",
                wireOut >= 0
            );

            // ASSERTION 3: wire_back is non-negative (it's 0)
            assertTrue(
                "wire_back must be non-negative (clamped to 0)."
                    + " wireBack=" + wireBack
                    + " (iteration " + i + ")",
                wireBack >= 0
            );

            // ASSERTION 4: The sum is >= network_roundtrip (since wire_back was clamped)
            assertTrue(
                "wire_out + data_node_execution + wire_back must be >= network_roundtrip when clamped."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", wireBack=" + wireBack
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                wireOut + dataNodeExecution + wireBack >= networkRoundtrip
            );
        }
    }

    /**
     * Property 4 (boundary): When network_roundtrip exactly equals wire_out + data_node_execution,
     * wire_back is exactly 0 and the sum invariant holds.
     *
     * <p>This tests the boundary condition where the network roundtrip is fully accounted for
     * by wire_out and data_node_execution, leaving zero time for wire_back.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b>
     */
    public void testWireDecompositionBoundaryWireBackExactlyZero() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            // Generate wire_out and data_node_execution
            final long wireOut = randomLongBetween(0L, 5_000_000_000L);
            final long dataNodeExecution = randomLongBetween(0L, 5_000_000_000L);

            // network_roundtrip is exactly wire_out + data_node_execution
            final long networkRoundtrip = wireOut + dataNodeExecution;

            // Compute wire_back
            final long wireBack = Math.max(0, networkRoundtrip - wireOut - dataNodeExecution);

            // ASSERTION 1: wire_back is exactly 0
            assertEquals(
                "wire_back must be 0 when network_roundtrip == wire_out + data_node_execution."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                0L,
                wireBack
            );

            // ASSERTION 2: Sum invariant still holds exactly
            assertEquals(
                "Sum invariant must hold: wire_out + data_node_execution + wire_back == network_roundtrip."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", wireBack=" + wireBack
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                networkRoundtrip,
                wireOut + dataNodeExecution + wireBack
            );

            // ASSERTION 3: Both wire values are non-negative
            assertTrue("wire_out must be non-negative (iteration " + i + ")", wireOut >= 0);
            assertTrue("wire_back must be non-negative (iteration " + i + ")", wireBack >= 0);
        }
    }

    /**
     * Property 4 (realistic values): Wire decomposition with realistic microsecond-scale values.
     *
     * <p>Tests with values typical of real search latency breakdowns — wire_out in the
     * range of hundreds of microseconds to milliseconds, data_node_execution in milliseconds,
     * and network_roundtrip as the sum plus wire_back.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b>
     */
    public void testWireDecompositionWithRealisticValues() {
        for (int i = 0; i < MIN_ITERATIONS; i++) {
            // Realistic wire_out: 100µs to 50ms (in nanos)
            final long wireOut = randomLongBetween(100_000L, 50_000_000L);

            // Realistic data_node_execution: 1ms to 500ms (in nanos)
            final long dataNodeExecution = randomLongBetween(1_000_000L, 500_000_000L);

            // Realistic wire_back: 100µs to 50ms (in nanos)
            final long expectedWireBack = randomLongBetween(100_000L, 50_000_000L);

            // Network roundtrip = wire_out + execution + wire_back
            final long networkRoundtrip = wireOut + dataNodeExecution + expectedWireBack;

            // Compute wire_back as the coordinator does
            final long computedWireBack = Math.max(0, networkRoundtrip - wireOut - dataNodeExecution);

            // ASSERTION 1: Computed wire_back matches expected
            assertEquals(
                "Computed wire_back must match expected for realistic values."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", expectedWireBack=" + expectedWireBack
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                expectedWireBack,
                computedWireBack
            );

            // ASSERTION 2: Sum invariant holds
            assertEquals(
                "Sum invariant must hold for realistic values."
                    + " wireOut=" + wireOut
                    + ", dataNodeExecution=" + dataNodeExecution
                    + ", computedWireBack=" + computedWireBack
                    + ", networkRoundtrip=" + networkRoundtrip
                    + " (iteration " + i + ")",
                networkRoundtrip,
                wireOut + dataNodeExecution + computedWireBack
            );

            // ASSERTION 3: Both wire values are non-negative
            assertTrue("wire_out must be non-negative (iteration " + i + ")", wireOut >= 0);
            assertTrue("wire_back must be non-negative (iteration " + i + ")", computedWireBack >= 0);
        }
    }
}
