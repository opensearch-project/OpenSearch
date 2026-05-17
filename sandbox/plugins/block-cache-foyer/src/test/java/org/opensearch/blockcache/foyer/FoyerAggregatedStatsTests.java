/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FoyerAggregatedStats}.
 *
 * Tests the FFM buffer parsing (snapshot factory) directly without the native
 * library. Any field-ordinal bug would be caught here.
 *
 * Buffer layout: 20 longs — section 0 (overall) at [0..9], section 1 (block-level) at [10..19].
 *   [0] HIT_COUNT  [1] HIT_BYTES  [2] MISS_COUNT  [3] MISS_BYTES
 *   [4] EVICTION_COUNT  [5] EVICTION_BYTES  [6] USED_BYTES
 *   [7] REMOVED_COUNT  [8] REMOVED_BYTES  [9] ACTIVE_IN_BYTES
 */
public class FoyerAggregatedStatsTests extends OpenSearchTestCase {

    /** Full 20-arg helper: 10 fields per section, 2 sections. */
    private static long[] buf(
        long hc0,
        long hb0,
        long mc0,
        long mb0,
        long ec0,
        long eb0,
        long ub0,
        long rc0,
        long rb0,
        long ai0,
        long hc1,
        long hb1,
        long mc1,
        long mb1,
        long ec1,
        long eb1,
        long ub1,
        long rc1,
        long rb1,
        long ai1
    ) {
        return new long[] { hc0, hb0, mc0, mb0, ec0, eb0, ub0, rc0, rb0, ai0, hc1, hb1, mc1, mb1, ec1, eb1, ub1, rc1, rb1, ai1 };
    }

    /** 16-arg shorthand: removed and active default to 0 for both sections. */
    private static long[] buf(
        long hc0,
        long hb0,
        long mc0,
        long mb0,
        long ec0,
        long eb0,
        long ub0,
        long hc1,
        long hb1,
        long mc1,
        long mb1,
        long ec1,
        long eb1,
        long ub1
    ) {
        return buf(hc0, hb0, mc0, mb0, ec0, eb0, ub0, 0, 0, 0, hc1, hb1, mc1, mb1, ec1, eb1, ub1, 0, 0, 0);
    }

    /** Builds a symmetric buffer where both sections are identical and active=0. */
    private static long[] uniform(long hc, long hb, long mc, long mb, long ec, long eb, long ub) {
        return buf(hc, hb, mc, mb, ec, eb, ub, 0, 0, 0, hc, hb, mc, mb, ec, eb, ub, 0, 0, 0);
    }

    // ── Non-null guarantees ───────────────────────────────────────────────────

    public void testSnapshotNonNull() {
        assertNotNull(FoyerAggregatedStats.snapshot(new long[20], 0L));
    }

    public void testOverallStatsNonNull() {
        assertNotNull(FoyerAggregatedStats.snapshot(new long[20], 0L).overallStats());
    }

    public void testBlockLevelStatsNonNull() {
        assertNotNull(FoyerAggregatedStats.snapshot(new long[20], 0L).blockLevelStats());
    }

    // ── overallStats field mapping (one-hot) ──────────────────────────────────

    public void testHitCountFromIndex0() {
        assertEquals(42L, FoyerAggregatedStats.snapshot(buf(42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().hits());
    }

    public void testHitBytesFromIndex1() {
        assertEquals(1024L, FoyerAggregatedStats.snapshot(buf(0, 1024, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().hitBytes());
    }

    public void testMissCountFromIndex2() {
        assertEquals(7L, FoyerAggregatedStats.snapshot(buf(0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().misses());
    }

    public void testMissBytesFromIndex3() {
        assertEquals(512L, FoyerAggregatedStats.snapshot(buf(0, 0, 0, 512, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().missBytes());
    }

    public void testEvictionCountFromIndex4() {
        assertEquals(3L, FoyerAggregatedStats.snapshot(buf(0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().evictions());
    }

    public void testEvictionBytesFromIndex5() {
        assertEquals(
            2048L,
            FoyerAggregatedStats.snapshot(buf(0, 0, 0, 0, 0, 2048, 0, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().evictionBytes()
        );
    }

    public void testUsedBytesFromIndex6() {
        assertEquals(
            999L,
            FoyerAggregatedStats.snapshot(buf(0, 0, 0, 0, 0, 0, 999, 0, 0, 0, 0, 0, 0, 0), 1L).overallStats().diskBytesUsed()
        );
    }

    public void testActiveInBytesFromIndex9() {
        // Section 0, index 9 = ACTIVE_IN_BYTES → should map to activeInBytes()
        long[] raw = buf(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            777L,   // section 0: activeInBytes=777
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
        );      // section 1: all zero
        assertEquals(777L, FoyerAggregatedStats.snapshot(raw, 0L).overallStats().activeInBytes());
    }

    public void testActiveInBytesDefaultsToZero() {
        // A fresh 20-zero buffer should have activeInBytes == 0
        assertEquals(0L, FoyerAggregatedStats.snapshot(new long[20], 0L).overallStats().activeInBytes());
    }

    public void testActiveInBytesIsIndexedIndependentlyFromSection1() {
        // section 0 activeInBytes = 123, section 1 activeInBytes = 456
        long[] raw = buf(0, 0, 0, 0, 0, 0, 0, 0, 0, 123L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 456L);
        assertEquals(123L, FoyerAggregatedStats.snapshot(raw, 0L).overallStats().activeInBytes());
        assertEquals(456L, FoyerAggregatedStats.snapshot(raw, 0L).blockLevelStats().activeInBytes());
    }

    // ── blockLevelStats section isolation ─────────────────────────────────────

    public void testBlockLevelReadsFromSection1() {
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(buf(10, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0), 1L);
        assertEquals(10L, s.overallStats().hits());
        assertEquals(99L, s.blockLevelStats().hits());
    }

    public void testBlockLevelAllFieldsMapped() {
        long[] raw = buf(0, 0, 0, 0, 0, 0, 0, 11, 22, 33, 44, 55, 66, 77);
        BlockCacheStats bl = FoyerAggregatedStats.snapshot(raw, 100L).blockLevelStats();
        assertEquals(11L, bl.hits());
        assertEquals(22L, bl.hitBytes());
        assertEquals(33L, bl.misses());
        assertEquals(44L, bl.missBytes());
        assertEquals(55L, bl.evictions());
        assertEquals(66L, bl.evictionBytes());
        assertEquals(77L, bl.diskBytesUsed());
    }

    public void testSection0DoesNotAffectSection1() {
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(buf(0, 0, 0, 0, 999, 0, 0, 0, 0, 0, 0, 0, 0, 0), 0L);
        assertEquals(999L, s.overallStats().evictions());
        assertEquals(0L, s.blockLevelStats().evictions());
    }

    // ── capacityBytes ─────────────────────────────────────────────────────────

    public void testCapacityPassedToOverall() {
        assertEquals(1_073_741_824L, FoyerAggregatedStats.snapshot(new long[20], 1_073_741_824L).overallStats().totalBytes());
    }

    public void testCapacityPassedToBlockLevel() {
        assertEquals(1_073_741_824L, FoyerAggregatedStats.snapshot(new long[20], 1_073_741_824L).blockLevelStats().totalBytes());
    }

    public void testZeroCapacity() {
        assertEquals(0L, FoyerAggregatedStats.snapshot(new long[20], 0L).overallStats().totalBytes());
    }

    // ── Foyer-specific zero fields ─────────────────────────────────────────────

    public void testMemoryBytesUsedAlwaysZeroOverall() {
        assertEquals(0L, FoyerAggregatedStats.snapshot(uniform(10, 100, 5, 50, 2, 20, 500), 1000L).overallStats().memoryBytesUsed());
    }

    public void testMemoryBytesUsedAlwaysZeroBlockLevel() {
        assertEquals(0L, FoyerAggregatedStats.snapshot(uniform(10, 100, 5, 50, 2, 20, 500), 1000L).blockLevelStats().memoryBytesUsed());
    }

    public void testRemovedAlwaysZeroOverall() {
        assertEquals(0L, FoyerAggregatedStats.snapshot(uniform(10, 100, 5, 50, 2, 20, 500), 1000L).overallStats().removed());
        assertEquals(0L, FoyerAggregatedStats.snapshot(uniform(10, 100, 5, 50, 2, 20, 500), 1000L).overallStats().removedBytes());
    }

    public void testRemovedAlwaysZeroBlockLevel() {
        assertEquals(0L, FoyerAggregatedStats.snapshot(uniform(10, 100, 5, 50, 2, 20, 500), 1000L).blockLevelStats().removed());
        assertEquals(0L, FoyerAggregatedStats.snapshot(uniform(10, 100, 5, 50, 2, 20, 500), 1000L).blockLevelStats().removedBytes());
    }

    // ── STATS_BUFFER_SIZE contract ─────────────────────────────────────────────

    public void testStatsBufferSizeIs20() {
        // 10 fields × 2 sections = 20. Verified here so changes to Field enum break tests fast.
        assertEquals(20, FoyerAggregatedStats.STATS_BUFFER_SIZE);
    }

    // ── All-zeros ─────────────────────────────────────────────────────────────

    public void testAllZeroBuffer() {
        BlockCacheStats s = FoyerAggregatedStats.snapshot(new long[20], 0L).overallStats();
        assertEquals(0L, s.hits());
        assertEquals(0L, s.misses());
        assertEquals(0L, s.evictions());
        assertEquals(0L, s.diskBytesUsed());
        assertEquals(0L, s.activeInBytes());
    }

    // ── Large values ──────────────────────────────────────────────────────────

    public void testLargeValuesNoCorruption() {
        long large = Long.MAX_VALUE / 2;
        BlockCacheStats s = FoyerAggregatedStats.snapshot(uniform(large, large, large, large, large, large, large), large).overallStats();
        assertEquals(large, s.hits());
        assertEquals(large, s.diskBytesUsed());
        assertEquals(large, s.totalBytes());
    }

    // ── Complete projection ───────────────────────────────────────────────────

    public void testCompleteProjection() {
        long[] raw = buf(100, 1000, 10, 200, 5, 500, 4096, 0, 0, 0, 0, 0, 0, 0);
        BlockCacheStats bc = FoyerAggregatedStats.snapshot(raw, 8192L).overallStats();
        assertEquals(100L, bc.hits());
        assertEquals(1000L, bc.hitBytes());
        assertEquals(10L, bc.misses());
        assertEquals(200L, bc.missBytes());
        assertEquals(5L, bc.evictions());
        assertEquals(500L, bc.evictionBytes());
        assertEquals(4096L, bc.diskBytesUsed());
        assertEquals(8192L, bc.totalBytes());
        assertEquals(0L, bc.memoryBytesUsed());
        assertEquals(0L, bc.removed());
        assertEquals(0L, bc.removedBytes());
        assertEquals(0L, bc.activeInBytes());
    }

    public void testCompleteProjectionWithActiveInBytes() {
        // Full 20-field buffer with activeInBytes in section 0
        long[] raw = buf(100, 1000, 10, 200, 5, 500, 4096, 3, 300, 55L, 50, 500, 2, 100, 1, 200, 2048, 1, 150, 20L);
        BlockCacheStats overall = FoyerAggregatedStats.snapshot(raw, 8192L).overallStats();
        BlockCacheStats blockLevel = FoyerAggregatedStats.snapshot(raw, 8192L).blockLevelStats();

        assertEquals(100L, overall.hits());
        assertEquals(4096L, overall.diskBytesUsed());
        assertEquals(3L, overall.removed());
        assertEquals(300L, overall.removedBytes());
        assertEquals(55L, overall.activeInBytes());

        assertEquals(50L, blockLevel.hits());
        assertEquals(2048L, blockLevel.diskBytesUsed());
        assertEquals(1L, blockLevel.removed());
        assertEquals(150L, blockLevel.removedBytes());
        assertEquals(20L, blockLevel.activeInBytes());
    }
}
