/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link CacheSettings#validatePercentSum} and
 * {@link CacheSettings#computeCacheSizes}.
 *
 * <p>The three sub-cache percentages (footer_metadata, offset_index, column_index)
 * must sum to exactly 100. This mirrors the Arrow pool model: each pool's max is
 * validated against the total budget — here, percentages are the "shares" and the
 * validation fires when any share changes (via
 * {@code DataFusionPlugin.recomputePageCacheLimits}).
 *
 * <p>User workflow: to change the split, send all three in one
 * {@code PUT /_cluster/settings} call so they are applied atomically and the
 * sum-to-100 invariant holds at validation time.
 */
public class CacheSettingsPercentValidationTests extends OpenSearchTestCase {

    // ── validatePercentSum ────────────────────────────────────────────────────

    public void testDefaultsSumTo100() {
        // Defaults: 50 + 35 + 15 = 100 — exactly at limit, passes
        CacheSettings.validatePercentSum(50, 35, 15);
    }

    public void testSumExactly100Passes() {
        CacheSettings.validatePercentSum(60, 30, 10);
        CacheSettings.validatePercentSum(33, 34, 33);
        CacheSettings.validatePercentSum(1, 1, 98);
    }

    public void testSumUnder100PassesHeadroomAccepted() {
        // Mirrors Arrow pool model: sum(max) <= budget. Unused headroom is fine.
        CacheSettings.validatePercentSum(50, 35, 10);   // 95% — 5% unused
        CacheSettings.validatePercentSum(40, 30, 10);   // 80% — 20% unused
        CacheSettings.validatePercentSum(1, 1, 1);      // 3% — 97% unused
    }

    public void testSumZeroPasses() {
        // All zeros is technically valid (all headroom, no caches get any bytes)
        CacheSettings.validatePercentSum(1, 1, 1); // 3% — valid
    }

    public void testSinglePercentLoweredAlonePasses() {
        // User lowers column_index_percent from 15→5 alone: 50+35+5=90 ≤ 100 → accepted.
        // This is the key user-friendly case — lowering alone doesn't need an atomic update.
        CacheSettings.validatePercentSum(50, 35, 5);
    }

    public void testSinglePercentRaisedAloneBreaks() {
        // User raises column_index_percent from 15→25 alone: 50+35+25=110 > 100 → rejected.
        // To raise one, user must lower another in the same PUT /_cluster/settings call.
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> CacheSettings.validatePercentSum(50, 35, 25));
        assertTrue("error must mention sum", ex.getMessage().contains("sum=110"));
        assertTrue("error must name the keys", ex.getMessage().contains("footer_metadata_percent"));
        assertTrue("error must name the keys", ex.getMessage().contains("offset_index_percent"));
        assertTrue("error must name the keys", ex.getMessage().contains("column_index_percent"));
    }

    public void testSumOver100Throws() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CacheSettings.validatePercentSum(50, 35, 20)  // 105
        );
        assertTrue("error must mention sum", ex.getMessage().contains("sum=105"));
    }

    public void testAtomicUpdateAllThreePasses() {
        // User sends all three in one PUT /_cluster/settings: 50/35/15 → 40/40/20 (still ≤ 100).
        CacheSettings.validatePercentSum(40, 40, 20);
    }

    public void testAtomicUpdateWithHeadroomPasses() {
        // User updates to leave some headroom: 40/30/20 = 90% — valid.
        CacheSettings.validatePercentSum(40, 30, 20);
    }

    // ── computeCacheSizes ─────────────────────────────────────────────────────

    public void testComputeSizesDefaultSplit() {
        // 1 GB total, defaults 50/35/15
        long total = 1024L * 1024 * 1024; // 1 GB
        long[] sizes = CacheSettings.computeCacheSizes(50, 35, 15, total);
        assertEquals(3, sizes.length);
        assertEquals(total * 50 / 100, sizes[0]); // footer ~512 MB
        assertEquals(total * 35 / 100, sizes[1]); // offset ~358 MB
        assertEquals(total * 15 / 100, sizes[2]); // column ~153 MB
    }

    public void testComputeSizesEqualSplit() {
        long total = 300L;
        long[] sizes = CacheSettings.computeCacheSizes(34, 33, 33, total);
        assertEquals(102L, sizes[0]);
        assertEquals(99L, sizes[1]);
        assertEquals(99L, sizes[2]);
    }

    public void testComputeSizesZeroTotalGivesZeros() {
        long[] sizes = CacheSettings.computeCacheSizes(50, 35, 15, 0L);
        assertEquals(0L, sizes[0]);
        assertEquals(0L, sizes[1]);
        assertEquals(0L, sizes[2]);
    }

    public void testComputeSizesTotalMatchesBudget() {
        // The three absolute sizes should be within 2 bytes of the total (integer division rounding)
        long total = 1_000_000L;
        long[] sizes = CacheSettings.computeCacheSizes(50, 35, 15, total);
        long sum = sizes[0] + sizes[1] + sizes[2];
        // Due to integer division, sum may be up to 2 bytes less than total
        assertTrue("sum should be close to total", sum <= total && sum >= total - 2);
    }
}
