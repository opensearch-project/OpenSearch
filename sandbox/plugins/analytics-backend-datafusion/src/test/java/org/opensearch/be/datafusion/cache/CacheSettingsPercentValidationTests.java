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
 */
public class CacheSettingsPercentValidationTests extends OpenSearchTestCase {

    // ── validatePercentSum ────────────────────────────────────────────────────

    public void testDefaultsSumTo100() {
        // Defaults: 48 + 34 + 13 + 5 = 100
        CacheSettings.validatePercentSum(48, 34, 13, 5);
    }

    public void testSumExactly100Passes() {
        CacheSettings.validatePercentSum(60, 25, 10, 5);
        CacheSettings.validatePercentSum(33, 34, 28, 5);
        CacheSettings.validatePercentSum(1, 1, 97, 1);
    }

    public void testSumUnder100PassesHeadroomAccepted() {
        CacheSettings.validatePercentSum(48, 34, 13, 1);  // 96% — 4% unused
        CacheSettings.validatePercentSum(40, 30, 10, 5);  // 85% — headroom OK
        CacheSettings.validatePercentSum(1, 1, 1, 1);     // 4% — valid
    }

    public void testSinglePercentRaisedAloneBreaks() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CacheSettings.validatePercentSum(48, 34, 13, 15)  // 110
        );
        assertTrue("error must mention sum", ex.getMessage().contains("sum=110"));
        assertTrue("error must name statistics key", ex.getMessage().contains("statistics_percent"));
    }

    public void testSumOver100Throws() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CacheSettings.validatePercentSum(50, 35, 20, 5)  // 110
        );
        assertTrue("error must mention sum", ex.getMessage().contains("sum=110"));
    }

    public void testAtomicUpdateAllFourPasses() {
        CacheSettings.validatePercentSum(45, 33, 12, 5);  // 95% — headroom allowed
        CacheSettings.validatePercentSum(40, 40, 15, 5);  // 100% — exact
    }

    public void testErrorMessageNamesAllKeys() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CacheSettings.validatePercentSum(48, 34, 13, 10)  // 105
        );
        assertTrue(ex.getMessage().contains("footer_metadata_percent"));
        assertTrue(ex.getMessage().contains("offset_index_percent"));
        assertTrue(ex.getMessage().contains("column_index_percent"));
        assertTrue(ex.getMessage().contains("statistics_percent"));
        assertTrue(ex.getMessage().contains("sum=105"));
    }

    // ── computeCacheSizes ─────────────────────────────────────────────────────

    public void testComputeSizesDefaultSplit() {
        long total = 1_150_000_000L; // ~1.15 GB sample metadata-index cache total budget
        long[] sizes = CacheSettings.computeCacheSizes(48, 34, 13, 5, total);
        assertEquals(4, sizes.length);
        assertEquals(total * 48 / 100, sizes[0]); // footer ~552 MB
        assertEquals(total * 34 / 100, sizes[1]); // offset index ~391 MB
        assertEquals(total * 13 / 100, sizes[2]); // column index ~150 MB
        assertEquals(total * 5 / 100, sizes[3]);  // statistics ~58 MB
    }

    public void testComputeSizesStatisticsNotZero() {
        long total = 1_000_000L;
        long[] sizes = CacheSettings.computeCacheSizes(48, 34, 13, 5, total);
        assertTrue("statistics cache must get non-zero bytes", sizes[3] > 0);
        assertEquals(50_000L, sizes[3]); // 5% of 1_000_000
    }

    public void testComputeSizesZeroTotalGivesZeros() {
        long[] sizes = CacheSettings.computeCacheSizes(48, 34, 13, 5, 0L);
        for (long s : sizes)
            assertEquals(0L, s);
    }

    public void testComputeSizesSumWithinBudget() {
        long total = 1_000_000L;
        long[] sizes = CacheSettings.computeCacheSizes(48, 34, 13, 5, total);
        long sum = sizes[0] + sizes[1] + sizes[2] + sizes[3];
        assertTrue("sum must not exceed total", sum <= total);
        // 48+34+13+5=100, integer division may lose up to 3 bytes
        assertTrue("sum must be close to total", sum >= total - 3);
    }

    public void testStatisticsUsesPercentDerivedLimit() {
        // Statistics bytes come from the unified percent budget, not a standalone hardcoded limit.
        long total = 1_000_000L;
        long[] sizes = CacheSettings.computeCacheSizes(48, 34, 13, 5, total);
        assertEquals("statistics cache must use percent-derived limit", 50_000L, sizes[3]);
    }
}
