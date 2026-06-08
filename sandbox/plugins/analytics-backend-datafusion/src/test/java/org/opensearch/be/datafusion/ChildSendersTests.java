/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.DatafusionReduceSink.ChildSenders;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Pure unit coverage for {@link ChildSenders#laneIndex(int, int)} — the lane
 * dispatch math used by the per-shard partition-stream wiring on the
 * coordinator-reduce side. The actual {@code laneForOrdinal} / {@code pickLane}
 * helpers compose this with array indexing; the index calculation itself is
 * the only piece that has interesting branches (pow-2 mask vs floorMod
 * fallback) and the only piece that touches negative or wrap-around inputs.
 *
 * <p>End-to-end coverage of the multi-lane feed/drain wiring lives in
 * {@code DatafusionReduceSinkMultiPartitionTests} and the coordinator IT suite.
 */
public class ChildSendersTests extends OpenSearchTestCase {

    public void testLaneIndexPowerOfTwoUsesBitmask() {
        // n=4: ordinal i should map to i & 3
        assertEquals(0, ChildSenders.laneIndex(4, 0));
        assertEquals(1, ChildSenders.laneIndex(4, 1));
        assertEquals(2, ChildSenders.laneIndex(4, 2));
        assertEquals(3, ChildSenders.laneIndex(4, 3));
        // wrap-around
        assertEquals(0, ChildSenders.laneIndex(4, 4));
        assertEquals(1, ChildSenders.laneIndex(4, 5));
        assertEquals(3, ChildSenders.laneIndex(4, 7));
        assertEquals(0, ChildSenders.laneIndex(4, 8));
        // large value
        assertEquals(3, ChildSenders.laneIndex(4, 1023));
    }

    public void testLaneIndexNonPowerOfTwoUsesFloorMod() {
        // n=3: not a power of two — falls through to floorMod
        assertEquals(0, ChildSenders.laneIndex(3, 0));
        assertEquals(1, ChildSenders.laneIndex(3, 1));
        assertEquals(2, ChildSenders.laneIndex(3, 2));
        assertEquals(0, ChildSenders.laneIndex(3, 3));
        assertEquals(2, ChildSenders.laneIndex(3, 8));
    }

    public void testLaneIndexSinglePartition() {
        // n=1 is a power of two (1 & 0 == 0): everything lands in lane 0.
        for (int i = -3; i < 17; i++) {
            assertEquals("ordinal " + i + " must land in lane 0 when n=1", 0, ChildSenders.laneIndex(1, i));
        }
    }

    /**
     * Negative inputs can arise from the {@link java.util.concurrent.atomic.AtomicInteger}
     * round-robin counter wrapping past {@code Integer.MAX_VALUE}. Both branches MUST
     * return a value in {@code [0, n)} — array indexing on a negative index would
     * blow up otherwise. The pow-2 bitmask path naturally produces this; floorMod
     * is the explicit fallback for non-pow-2.
     */
    public void testLaneIndexNegativeOrdinalReturnsValidIndex() {
        // n=4 (pow-2): -1 & 3 == 3
        assertEquals(3, ChildSenders.laneIndex(4, -1));
        assertEquals(0, ChildSenders.laneIndex(4, -4));
        assertEquals(0, ChildSenders.laneIndex(4, Integer.MIN_VALUE));

        // n=3 (non-pow-2): floorMod(-1, 3) == 2
        assertEquals(2, ChildSenders.laneIndex(3, -1));
        assertEquals(0, ChildSenders.laneIndex(3, -3));
        // Defensive check: any negative input must produce a value in [0, n).
        for (int neg : new int[] { -1, -2, -3, -100, Integer.MIN_VALUE }) {
            for (int n : new int[] { 1, 2, 3, 4, 5, 7, 8, 16 }) {
                int lane = ChildSenders.laneIndex(n, neg);
                assertTrue("n=" + n + ", neg=" + neg + ": lane " + lane + " out of [0," + n + ")", lane >= 0 && lane < n);
            }
        }
    }

    /**
     * Ordinals cycle through all lanes exactly once before wrapping. Documents
     * that {@code numShards == numLanes} (the factory's intended sizing) gives
     * shard {@code i} → lane {@code i} with zero collisions.
     */
    public void testEveryLaneIsHitWhenOrdinalCountEqualsLaneCount() {
        for (int n : new int[] { 1, 2, 3, 4, 5, 8, 16 }) {
            boolean[] hit = new boolean[n];
            for (int ordinal = 0; ordinal < n; ordinal++) {
                int lane = ChildSenders.laneIndex(n, ordinal);
                assertTrue("lane " + lane + " out of range [0," + n + ")", lane >= 0 && lane < n);
                hit[lane] = true;
            }
            for (int i = 0; i < n; i++) {
                assertTrue("n=" + n + ": lane " + i + " was never hit", hit[i]);
            }
        }
    }
}
