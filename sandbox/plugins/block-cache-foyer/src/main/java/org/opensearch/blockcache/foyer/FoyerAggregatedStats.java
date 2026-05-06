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
 * <p>Both sections are represented as {@link BlockCacheStats} records, which
 * carry the full 11-field set including Foyer-native counters
 * ({@code hitBytes}, {@code missBytes}) alongside the universal counters used
 * by {@code UnifiedCacheService.mergeStats()}.
 *
 * <p>Core ({@code UnifiedCacheService}) only ever sees the {@link BlockCacheStats}
 * returned by {@link #overallStats()} — passed via {@link FoyerBlockCache#stats()}.
 * Foyer-aware code that needs the per-tier breakdown obtains this object via
 * {@link FoyerBlockCache#foyerStats()}.
 *
 * <h3>FFM buffer layout</h3>
 * The native {@code foyer_snapshot_stats} call fills a flat {@code long[]}:
 * <pre>
 *   index 0 .. {@link Field#COUNT}-1   → section 0 (overall)
 *   index N .. 2N-1                    → section 1 (block-level)
 * </pre>
 * where {@code N = Field.COUNT = 7}.
 *
 * @opensearch.internal
 */
public final class FoyerAggregatedStats {

    // ─── FFM buffer field layout ──────────────────────────────────────────────

    /**
     * Ordered fields of a single stats section in the native FFM transfer buffer.
     * The ordinal of each constant is its array index within one section.
     * Must match the field order in the native {@code foyer_snapshot_stats} output.
     */
    private enum Field {
        HIT_COUNT,
        HIT_BYTES,
        MISS_COUNT,
        MISS_BYTES,
        EVICTION_COUNT,
        EVICTION_BYTES,
        USED_BYTES;

        /** Number of fields per section in the FFM buffer. */
        static final int COUNT = values().length;
    }

    // ─── State ────────────────────────────────────────────────────────────────

    /** Cross-tier rollup (section 0 of the FFM buffer). */
    private final BlockCacheStats overallStats;

    /** Disk-tier only (section 1 of the FFM buffer). */
    private final BlockCacheStats blockLevelStats;

    private FoyerAggregatedStats(BlockCacheStats overallStats, BlockCacheStats blockLevelStats) {
        this.overallStats    = overallStats;
        this.blockLevelStats = blockLevelStats;
    }

    // ─── FFM factory ─────────────────────────────────────────────────────────

    /**
     * Parses both sections from the point-in-time FFM stats buffer returned by
     * {@code FoyerBridge.snapshotStats(cachePtr)} and returns a snapshot.
     *
     * <p>The name mirrors the native call ({@code foyer_snapshot_stats}) and
     * signals the transient, point-in-time nature of the data.
     *
     * @param raw           the full stats buffer; length must be {@code >= 2 * Field.COUNT}
     * @param capacityBytes configured disk capacity for this cache instance
     * @return populated snapshot; never {@code null}
     */
    public static FoyerAggregatedStats snapshot(long[] raw, long capacityBytes) {
        return new FoyerAggregatedStats(
            readSection(raw, 0, capacityBytes),
            readSection(raw, Field.COUNT, capacityBytes)
        );
    }

    /**
     * Reads one section from the FFM buffer at the given field {@code offset}.
     *
     * @param raw           raw buffer from {@code FoyerBridge.snapshotStats()}
     * @param offset        field index of the first field for this section
     * @param capacityBytes configured disk capacity
     */
    private static BlockCacheStats readSection(long[] raw, int offset, long capacityBytes) {
        return new BlockCacheStats(
            raw[offset + Field.HIT_COUNT.ordinal()],
            raw[offset + Field.MISS_COUNT.ordinal()],
            raw[offset + Field.HIT_BYTES.ordinal()],
            raw[offset + Field.MISS_BYTES.ordinal()],
            raw[offset + Field.EVICTION_COUNT.ordinal()],
            raw[offset + Field.EVICTION_BYTES.ordinal()],
            0L,                                              // removes — no explicit invalidation yet
            0L,                                              // removeBytes — same
            0L,                                              // memoryBytesUsed — Foyer is disk-only
            raw[offset + Field.USED_BYTES.ordinal()],        // diskBytesUsed
            capacityBytes                                    // totalBytes — configured capacity
        );
    }

    // ─── Accessors ────────────────────────────────────────────────────────────

    /**
     * Cross-tier rollup stats (section 0) — hits, misses, byte counters, and
     * used/capacity bytes across both the in-memory admission buffer and the
     * on-disk block store.
     *
     * <p>This is the value returned by {@link FoyerBlockCache#stats()} for core
     * consumption.
     */
    public BlockCacheStats overallStats() {
        return overallStats;
    }

    /**
     * Disk-tier-only stats (section 1) — isolates the on-disk block store.
     * Useful for SSD I/O bandwidth accounting and disk-tier eviction pressure
     * analysis.
     */
    public BlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }
}
