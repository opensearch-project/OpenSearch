/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.plugins.BlockCacheStats;

/**
 * Rich stats snapshot for the Foyer block cache, combining two sections read
 * from the native FFM transfer buffer:
 * <ul>
 *   <li>{@code overallStats} — cross-tier rollup (section 0): covers both the
 *       in-memory admission buffer and the on-disk block store.</li>
 *   <li>{@code blockLevelStats} — disk-tier only (section 1): isolates the
 *       on-disk block store behaviour, useful for SSD I/O accounting.</li>
 * </ul>
 *
 * <p>This is a Foyer-internal type. Core ({@code UnifiedCacheService}) only
 * ever sees the flat {@link BlockCacheStats} record returned by
 * {@link #toSpiStats()}. Foyer-aware code that needs richer counters — e.g.
 * per-tier bandwidth ({@code hitBytes}, {@code missBytes}), capacity ratios, or
 * node-stats contributions — can obtain a {@code FoyerAggregatedStats} via
 * {@link FoyerBlockCache#foyerStats()}.
 *
 * <h3>FFM buffer layout</h3>
 * The native {@code foyer_snapshot_stats} call fills a flat {@code long[]}:
 * <pre>
 *   index 0 .. {@link FoyerBlockCacheStats.Field#COUNT}-1   → section 0 (overall)
 *   index N .. 2N-1                                         → section 1 (block-level)
 * </pre>
 * where {@code N = FoyerBlockCacheStats.Field.COUNT}.
 *
 * @opensearch.internal
 */
public final class FoyerAggregatedStats {

    /** Cross-tier rollup (section 0 of the FFM buffer). */
    private final FoyerBlockCacheStats overallStats;

    /** Disk-tier only (section 1 of the FFM buffer). */
    private final FoyerBlockCacheStats blockLevelStats;

    private FoyerAggregatedStats(FoyerBlockCacheStats overallStats, FoyerBlockCacheStats blockLevelStats) {
        this.overallStats   = overallStats;
        this.blockLevelStats = blockLevelStats;
    }

    /**
     * Parses both sections from the raw FFM buffer returned by
     * {@code FoyerBridge.snapshotStats(cachePtr)}.
     *
     * @param raw          raw {@code long[]} from the native call; length must be
     *                     {@code >= 2 * FoyerBlockCacheStats.Field.COUNT}
     * @param capacityBytes configured disk capacity of this cache instance
     * @return populated snapshot; never {@code null}
     */
    public static FoyerAggregatedStats fromRaw(long[] raw, long capacityBytes) {
        int n = FoyerBlockCacheStats.Field.COUNT;
        FoyerBlockCacheStats overall    = FoyerBlockCacheStats.fromRaw(raw, 0, capacityBytes);
        FoyerBlockCacheStats blockLevel = FoyerBlockCacheStats.fromRaw(raw, n, capacityBytes);
        return new FoyerAggregatedStats(overall, blockLevel);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Cross-tier rollup stats — hits, misses, evictions, bytes, and used bytes
     * across both the in-memory admission buffer and the on-disk block store.
     */
    public FoyerBlockCacheStats overallStats() {
        return overallStats;
    }

    /**
     * Disk-tier-only stats — isolates the on-disk block store. Useful for SSD
     * I/O bandwidth accounting and disk-tier eviction pressure analysis.
     */
    public FoyerBlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SPI projection
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Projects the {@code overallStats} section onto the flat SPI record
     * consumed by core ({@link BlockCacheStats}).
     *
     * <p>Foyer is a disk-only cache; {@code memoryBytesUsed} is always zero.
     * {@code diskBytesUsed} is taken from the overall section's {@code usedBytes}.
     *
     * @return SPI-compatible flat record; never {@code null}
     */
    public BlockCacheStats toSpiStats() {
        return new BlockCacheStats(
            overallStats.hitCount(),
            overallStats.missCount(),
            overallStats.evictionCount(),
            0L,                          // memoryBytesUsed — Foyer is disk-only
            overallStats.usedBytes()     // diskBytesUsed
        );
    }
}
