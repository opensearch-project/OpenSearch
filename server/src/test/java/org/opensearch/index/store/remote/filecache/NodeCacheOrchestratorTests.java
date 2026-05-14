/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheProvider;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link NodeCacheOrchestrator}: budget validation, addBlockCache,
 * capacity and utilization accessors — all without real FileCache from disk.
 */
public class NodeCacheOrchestratorTests extends OpenSearchTestCase {

    // ── validate() ────────────────────────────────────────────────────────────

    public void testValidatePassesLegalValues() {
        NodeCacheOrchestrator.validate(600L, 200L, 1000L);
    }

    public void testValidatePassesZeroBlockCache() {
        NodeCacheOrchestrator.validate(800L, 0L, 1000L);
    }

    public void testValidatePassesSumEqualsTotalSSD() {
        NodeCacheOrchestrator.validate(600L, 400L, 1000L);
    }

    public void testValidateThrowsZeroTotalSSD() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> NodeCacheOrchestrator.validate(600L, 200L, 0L));
        assertTrue(ex.getMessage().contains("SSD capacity"));
    }

    public void testValidateThrowsNegativeTotalSSD() {
        expectThrows(IllegalArgumentException.class, () -> NodeCacheOrchestrator.validate(600L, 200L, -1L));
    }

    public void testValidateThrowsNegativeBlockCacheBytes() {
        expectThrows(IllegalArgumentException.class, () -> NodeCacheOrchestrator.validate(800L, -1L, 1000L));
    }

    public void testValidateThrowsFileCacheBytesZero() {
        expectThrows(IllegalArgumentException.class, () -> NodeCacheOrchestrator.validate(0L, 1000L, 1000L));
    }

    public void testValidateThrowsFileCacheBytesNegative() {
        expectThrows(IllegalArgumentException.class, () -> NodeCacheOrchestrator.validate(-1L, 600L, 1000L));
    }

    public void testValidateThrowsSumExceedsTotalSSD() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> NodeCacheOrchestrator.validate(700L, 400L, 1000L));
        assertTrue(ex.getMessage().contains("exceeds") || ex.getMessage().contains("Reduce"));
    }

    // ── addBlockCache / blockCaches ───────────────────────────────────────────

    public void testAddNullBlockCacheIgnored() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(null);
        assertEquals(0, orc.blockCaches().size());
    }

    public void testAddBlockCacheRegistersIt() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        BlockCache bc = mockBlockCache(0, 0, 0, 0, 1000L);
        orc.addBlockCache(bc);
        assertEquals(1, orc.blockCaches().size());
        assertSame(bc, orc.blockCaches().get(0));
    }

    public void testAddMultipleBlockCaches() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 500L));
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 500L));
        assertEquals(2, orc.blockCaches().size());
    }

    public void testBlockCachesListIsUnmodifiable() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 1L));
        expectThrows(UnsupportedOperationException.class, () -> orc.blockCaches().add(null));
    }

    public void testFileCacheAccessor() {
        FileCache fc = mock(FileCache.class);
        assertSame(fc, new NodeCacheOrchestrator(fc, 0L).fileCache());
    }

    // ── blockCacheCapacityBytes ───────────────────────────────────────────────

    public void testBlockCacheCapacityZeroWhenNone() {
        FileCache fc = mock(FileCache.class);
        assertEquals(0L, new NodeCacheOrchestrator(fc, 0L).blockCacheCapacityBytes());
    }

    public void testBlockCacheCapacitySumsAcrossCaches() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 400L));
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 600L));
        assertEquals(1000L, orc.blockCacheCapacityBytes());
    }

    // ── cacheUtilizedBytes ────────────────────────────────────────────────────

    public void testCacheUtilizedIncludesFileCache() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(500L);
        assertEquals(500L, new NodeCacheOrchestrator(fc, 0L).cacheUtilizedBytes());
    }

    public void testCacheUtilizedIncludesBlockCacheDisk() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(100L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(mockBlockCache(0, 0, 300L, 0, 0));
        assertEquals(400L, orc.cacheUtilizedBytes());
    }

    public void testCacheUtilizedIncludesBlockCacheMemory() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        BlockCacheStats stats = new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 50L, 200L, 0L);
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        orc.addBlockCache(bc);
        assertEquals(250L, orc.cacheUtilizedBytes());
    }

    // ── blockCacheDiskBytesUsed ───────────────────────────────────────────────

    public void testBlockCacheDiskBytesUsedZeroWhenNone() {
        FileCache fc = mock(FileCache.class);
        assertEquals(0L, new NodeCacheOrchestrator(fc, 0L).blockCacheDiskBytesUsed());
    }

    public void testBlockCacheDiskBytesUsedSumsAcrossCaches() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        // diskUsed is the 3rd positional arg in mockBlockCache
        orc.addBlockCache(mockBlockCache(0, 0, 300L, 0, 1000L));
        orc.addBlockCache(mockBlockCache(0, 0, 200L, 0, 1000L));
        assertEquals(500L, orc.blockCacheDiskBytesUsed());
    }

    public void testBlockCacheDiskBytesUsedIgnoresMemory() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        // memory-only entry: memUsed=100, diskUsed=0
        orc.addBlockCache(mockBlockCache(0, 0, 0L, 100L, 1000L));
        assertEquals(0L, orc.blockCacheDiskBytesUsed());
    }

    // ── computeBlockCacheBudget ───────────────────────────────────────────────

    public void testComputeBlockCacheBudgetNoProviders() {
        assertEquals(0L, NodeCacheOrchestrator.computeBlockCacheBudget(List.of(), Settings.EMPTY, 1000L));
    }

    public void testComputeBlockCacheBudgetSingleProvider() {
        // Provider requests 25% of 1000 = 250
        var provider = mockProvider("25%", 1.0);
        assertEquals(250L, NodeCacheOrchestrator.computeBlockCacheBudget(List.of(provider), Settings.EMPTY, 1000L));
    }

    public void testComputeBlockCacheBudgetMultipleProviders() {
        var p1 = mockProvider("25%", 1.0);  // 250 of 1000
        var p2 = mockProvider("10%", 1.0);  // 100 of 1000
        assertEquals(350L, NodeCacheOrchestrator.computeBlockCacheBudget(List.of(p1, p2), Settings.EMPTY, 1000L));
    }

    public void testComputeBlockCacheBudgetZeroPercentProvider() {
        var provider = mockProvider("0%", 1.0);
        assertEquals(0L, NodeCacheOrchestrator.computeBlockCacheBudget(List.of(provider), Settings.EMPTY, 1000L));
    }

    // ── computeVirtualBlockCacheBytes ─────────────────────────────────────────

    public void testComputeVirtualBlockCacheBytesNoProviders() {
        assertEquals(0L, NodeCacheOrchestrator.computeVirtualBlockCacheBytes(List.of(), Settings.EMPTY, 1000L));
    }

    public void testComputeVirtualBlockCacheBytesAppliesRatio() {
        // 25% of 1000 = 250 bytes reserved × ratio 5.0 = 1250 virtual bytes
        var provider = mockProvider("25%", 5.0);
        assertEquals(1250L, NodeCacheOrchestrator.computeVirtualBlockCacheBytes(List.of(provider), Settings.EMPTY, 1000L));
    }

    public void testComputeVirtualBlockCacheBytesSumsMultipleProviders() {
        var p1 = mockProvider("20%", 4.0);  // 200 × 4 = 800
        var p2 = mockProvider("10%", 2.0);  // 100 × 2 = 200
        assertEquals(1000L, NodeCacheOrchestrator.computeVirtualBlockCacheBytes(List.of(p1, p2), Settings.EMPTY, 1000L));
    }

    public void testComputeVirtualBlockCacheBytesRatioOneEqualsReserved() {
        var provider = mockProvider("30%", 1.0);  // 300 × 1 = 300
        assertEquals(300L, NodeCacheOrchestrator.computeVirtualBlockCacheBytes(List.of(provider), Settings.EMPTY, 1000L));
    }

    // ── registerProviders ─────────────────────────────────────────────────────

    public void testRegisterProvidersWithEmptyOptionalSkipsCache() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);

        org.opensearch.plugins.BlockCacheProvider emptyProvider = mock(org.opensearch.plugins.BlockCacheProvider.class);
        when(emptyProvider.getBlockCache()).thenReturn(java.util.Optional.empty());

        orc.registerProviders(List.of(emptyProvider));
        assertEquals(0, orc.blockCaches().size());
    }

    public void testRegisterProvidersWiresCacheFromProvider() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);

        BlockCache bc = mockBlockCache(0, 0, 0, 0, 500L);
        org.opensearch.plugins.BlockCacheProvider provider = mock(org.opensearch.plugins.BlockCacheProvider.class);
        when(provider.getBlockCache()).thenReturn(java.util.Optional.of(bc));

        orc.registerProviders(List.of(provider));
        assertEquals(1, orc.blockCaches().size());
        assertSame(bc, orc.blockCaches().get(0));
    }

    public void testRegisterProvidersMixedEmptyAndPresent() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);

        BlockCache bc = mockBlockCache(0, 0, 0, 0, 100L);

        org.opensearch.plugins.BlockCacheProvider withCache = mock(org.opensearch.plugins.BlockCacheProvider.class);
        when(withCache.getBlockCache()).thenReturn(java.util.Optional.of(bc));

        org.opensearch.plugins.BlockCacheProvider withoutCache = mock(org.opensearch.plugins.BlockCacheProvider.class);
        when(withoutCache.getBlockCache()).thenReturn(java.util.Optional.empty());

        orc.registerProviders(List.of(withCache, withoutCache));
        assertEquals(1, orc.blockCaches().size());
    }

    // ── aggregateStats — mergeStats path (block cache present) ───────────────

    public void testAggregateStatsMergesHitsFromBlockCache() {
        FileCache fc = fileCacheWithStats(0, 0, 0, 0, 1000L, 0L, 0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        // block cache: 50 hits, 10 misses, diskUsed=0, total=500
        orc.addBlockCache(mockBlockCacheWithFullStats(50L, 10L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 500L));
        AggregateFileCacheStats s = orc.aggregateStats();
        assertEquals(50L, s.getCacheHits());
        assertEquals(10L, s.getCacheMisses());
    }

    public void testAggregateStatsMergesTotalCapacityFromBlockCache() {
        FileCache fc = fileCacheWithStats(0, 0, 0, 0, 1000L, 0L, 0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(mockBlockCacheWithFullStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 500L));
        AggregateFileCacheStats s = orc.aggregateStats();
        // total = fileCache total (1000) + blockCache total (500) = 1500
        assertEquals(1500L, s.getTotal().getBytes());
    }

    public void testAggregateStatsMergesEvictionBytesFromBlockCache() {
        FileCache fc = fileCacheWithStats(0, 0, 0, 0, 1000L, 100L, 0L);  // fileCache evicted=100
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        // block cache: evictionBytes=200
        orc.addBlockCache(mockBlockCacheWithFullStats(0, 0, 0, 0, 0, 200L, 0, 0, 0, 200L, 500L));
        AggregateFileCacheStats s = orc.aggregateStats();
        // overall evicted = fileCache(100) + blockCache evictionBytes(200) = 300
        assertEquals(300L, s.getEvicted().getBytes());
    }

    public void testAggregateStatsMergesUsedBytesFromBlockCacheDisk() {
        FileCache fc = fileCacheWithStats(0, 0, 400L, 0, 1000L, 0L, 0L);  // fileCache used=400
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        // block cache: diskBytesUsed=200
        orc.addBlockCache(mockBlockCacheWithFullStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 200L, 500L));
        AggregateFileCacheStats s = orc.aggregateStats();
        // overall used = fileCache(400) + blockCache disk(200) = 600
        assertEquals(600L, s.getUsed().getBytes());
    }

    public void testAggregateStatsBlockSectionAlsoMerged() {
        FileCache fc = fileCacheWithStats(0, 0, 0, 0, 1000L, 0L, 0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        orc.addBlockCache(mockBlockCacheWithFullStats(7L, 3L, 0, 0, 0, 0, 0, 0, 0, 100L, 200L));
        AggregateFileCacheStats s = orc.aggregateStats();
        // block section: hits should include the block cache's hits
        assertEquals(7L, s.getBlockFileCacheStats().getCacheHits());
    }

    public void testAggregateStatsNoBlockCacheReturnsFileCacheStatsOnly() {
        FileCache fc = fileCacheWithStats(0, 0, 500L, 0, 1000L, 50L, 0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc, 0L);
        AggregateFileCacheStats s = orc.aggregateStats();
        assertEquals(1000L, s.getTotal().getBytes());
        assertEquals(500L, s.getUsed().getBytes());
        assertEquals(50L, s.getEvicted().getBytes());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Creates a {@link BlockCacheProvider} mock whose {@code requestedCapacityBytes}
     * returns {@code Math.round(totalBudget * ratio)} where {@code ratio} is parsed
     * from {@code sizePercent} (e.g. "25%" → 0.25), and whose
     * {@code dataToCapacityRatio} returns {@code amplification}.
     */
    private BlockCacheProvider mockProvider(String sizePercent, double amplification) {
        BlockCacheProvider provider = mock(BlockCacheProvider.class);
        // Parse the percentage string into a double ratio (e.g. "25%" → 0.25)
        String stripped = sizePercent.endsWith("%") ? sizePercent.substring(0, sizePercent.length() - 1) : sizePercent;
        double ratio = Double.parseDouble(stripped) / 100.0;
        when(provider.requestedCapacityBytes(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong())).thenAnswer(
            inv -> Math.round((long) inv.getArgument(1) * ratio)
        );
        when(provider.dataToCapacityRatio(org.mockito.ArgumentMatchers.any())).thenReturn(amplification);
        when(provider.getBlockCache()).thenReturn(Optional.empty());
        return provider;
    }

    private BlockCache mockBlockCache(long hits, long misses, long diskUsed, long memUsed, long total) {
        BlockCacheStats stats = new BlockCacheStats(hits, misses, 0, 0, 0, 0, 0, 0, memUsed, diskUsed, total);
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        return bc;
    }

    /** Full BlockCacheStats constructor: hits,misses,hitBytes,missBytes,evictions,evictionBytes,
     *  removed,removedBytes,memBytesUsed,diskBytesUsed,totalBytes */
    private BlockCache mockBlockCacheWithFullStats(
        long hits,
        long misses,
        long hitBytes,
        long missBytes,
        long evictions,
        long evictionBytes,
        long removed,
        long removedBytes,
        long memBytesUsed,
        long diskBytesUsed,
        long totalBytes
    ) {
        BlockCacheStats stats = new BlockCacheStats(
            hits,
            misses,
            hitBytes,
            missBytes,
            evictions,
            evictionBytes,
            removed,
            removedBytes,
            memBytesUsed,
            diskBytesUsed,
            totalBytes
        );
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        return bc;
    }

    /**
     * Creates a mock FileCache whose {@code fileCacheStats()} returns an
     * {@link AggregateFileCacheStats} built from the provided values.
     * All sub-sections (full, block, pinned) are zero; only the overall section
     * carries the values, which is sufficient for mergeStats tests.
     */
    private FileCache fileCacheWithStats(long active, long hits, long used, long pinned, long total, long evicted, long removed) {
        FileCacheStats overall = new FileCacheStats(
            active,
            total,
            used,
            pinned,
            evicted,
            removed,
            hits,
            0L,
            AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
        );
        FileCacheStats zero = new FileCacheStats(0, 0, 0, 0, 0, 0, 0, 0, AggregateFileCacheStats.FileCacheStatsType.BLOCK_FILE_STATS);
        FileCacheStats zeroPinned = new FileCacheStats(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            AggregateFileCacheStats.FileCacheStatsType.PINNED_FILE_STATS
        );
        FileCacheStats zeroFull = new FileCacheStats(0, 0, 0, 0, 0, 0, 0, 0, AggregateFileCacheStats.FileCacheStatsType.FULL_FILE_STATS);
        AggregateFileCacheStats agg = new AggregateFileCacheStats(System.currentTimeMillis(), overall, zeroFull, zero, zeroPinned);
        FileCache fc = mock(FileCache.class);
        when(fc.fileCacheStats()).thenReturn(agg);
        when(fc.usage()).thenReturn(used);
        return fc;
    }
}
