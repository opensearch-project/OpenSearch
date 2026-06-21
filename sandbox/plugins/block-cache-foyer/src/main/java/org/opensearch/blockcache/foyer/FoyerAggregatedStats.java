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
 * Point-in-time stats snapshot for the Foyer block cache.
 *
 * <p>Combines two sections parsed from the native FFM stats buffer:
 * section 0 (overall cross-tier rollup) and section 1 (disk-tier only).
 * Each section is a {@link BlockCacheStats} record carrying the full
 * counter set.
 *
 * <p>Core only ever sees the {@link BlockCacheStats} from {@link #overallStats()},
 * via {@link FoyerBlockCache#stats()}. The per-tier breakdown is available to
 * Foyer-aware code via {@link FoyerBlockCache#foyerStats()}.
 *
 * @opensearch.internal
 */
public final class FoyerAggregatedStats {

    /**
     * Field layout of one section in the native FFM stats buffer.
     * Ordinal equals the field's array index within the section.
     * Must match the Rust {@code FoyerStatsCounter::snapshot()} field order exactly.
     */
    private enum Field {
        HIT_COUNT,
        HIT_BYTES,
        MISS_COUNT,
        MISS_BYTES,
        EVICTION_COUNT,
        EVICTION_BYTES,
        USED_BYTES,
        REMOVED_COUNT,
        REMOVED_BYTES,
        ACTIVE_IN_BYTES;

        static final int COUNT = values().length;
    }

    /**
     * Buffer size for {@link FoyerBridge#snapshotStats}: 2 sections × {@code Field.COUNT} values.
     * Automatically kept in sync with the field count — no manual update needed when adding fields.
     */
    static final int STATS_BUFFER_SIZE = Field.COUNT * 2;

    /** Cross-tier rollup — section 0 of the FFM buffer. */
    private final BlockCacheStats overallStats;

    /** Disk-tier only — section 1 of the FFM buffer. */
    private final BlockCacheStats blockLevelStats;

    /** Data cache capacity — used to report per-tier capacity in tiered mode. */
    private final long dataCapacityBytes;

    /** Metadata cache capacity — used to report per-tier capacity in tiered mode. */
    private final long metadataCapacityBytes;

    private FoyerAggregatedStats(BlockCacheStats overallStats, BlockCacheStats blockLevelStats) {
        this.overallStats = overallStats;
        this.blockLevelStats = blockLevelStats;
        this.dataCapacityBytes = 0L;
        this.metadataCapacityBytes = 0L;
    }

    private FoyerAggregatedStats(
        BlockCacheStats overallStats,
        BlockCacheStats blockLevelStats,
        long dataCapacityBytes,
        long metadataCapacityBytes
    ) {
        this.overallStats = overallStats;
        this.blockLevelStats = blockLevelStats;
        this.dataCapacityBytes = dataCapacityBytes;
        this.metadataCapacityBytes = metadataCapacityBytes;
    }

    /**
     * Parses both sections from the FFM stats buffer returned by
     * {@code FoyerBridge.snapshotStats(cachePtr)}.
     *
     * @param raw           stats buffer; length must be {@code >= 2 * Field.COUNT}
     * @param capacityBytes configured disk capacity for this cache instance
     */
    public static FoyerAggregatedStats snapshot(long[] raw, long capacityBytes) {
        return new FoyerAggregatedStats(readSection(raw, 0, capacityBytes), readSection(raw, Field.COUNT, capacityBytes));
    }

    /**
     * Parses the FFM stats buffer for a tiered cache (data + metadata on separate SSDs).
     *
     * <p>The Rust FFM writes:
     * <ul>
     *   <li>Indices 0–9: data cache stats</li>
     *   <li>Indices 10–19: metadata cache stats</li>
     * </ul>
     *
     * <p>The {@code overallStats()} method returns a merged view (summed counters).
     * Use {@link #dataCacheStats()} and {@link #metadataCacheStats()} for per-tier breakdown.
     *
     * @param raw                 stats buffer; length must be {@code >= 2 * Field.COUNT}
     * @param dataCapacityBytes   data cache disk capacity
     * @param metaCapacityBytes   metadata cache disk capacity
     */
    public static FoyerAggregatedStats snapshotTiered(long[] raw, long dataCapacityBytes, long metaCapacityBytes) {
        BlockCacheStats dataStats = readSection(raw, 0, dataCapacityBytes);
        BlockCacheStats metaStats = readSection(raw, Field.COUNT, metaCapacityBytes);
        BlockCacheStats merged = merge(dataStats, metaStats);
        return new FoyerAggregatedStats(merged, dataStats, dataCapacityBytes, metaCapacityBytes);
    }

    private static BlockCacheStats readSection(long[] raw, int offset, long capacityBytes) {
        return new BlockCacheStats(
            raw[offset + Field.HIT_COUNT.ordinal()],
            raw[offset + Field.MISS_COUNT.ordinal()],
            raw[offset + Field.HIT_BYTES.ordinal()],
            raw[offset + Field.MISS_BYTES.ordinal()],
            raw[offset + Field.EVICTION_COUNT.ordinal()],
            raw[offset + Field.EVICTION_BYTES.ordinal()],
            raw[offset + Field.REMOVED_COUNT.ordinal()],
            raw[offset + Field.REMOVED_BYTES.ordinal()],
            0L,                                                             // memoryBytesUsed — disk-only cache
            raw[offset + Field.USED_BYTES.ordinal()],
            capacityBytes,
            // Clamp to 0: activeInBytes is a point-in-time gauge and must never be negative.
            // Other counters are cumulative and intentionally left unclamped — a negative there
            // signals a real bug and should stay visible.
            Math.max(0L, raw[offset + Field.ACTIVE_IN_BYTES.ordinal()])
        );
    }

    /** Cross-tier rollup stats — used by core via {@link FoyerBlockCache#stats()}. */
    public BlockCacheStats overallStats() {
        return overallStats;
    }

    /** Disk-tier-only stats — SSD I/O and eviction pressure breakdown. */
    public BlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }

    /**
     * Data cache stats (section 0 of the FFM buffer).
     * In tiered mode, this is the large data SSD cache.
     * In single-cache mode, equivalent to {@link #overallStats()}.
     */
    public BlockCacheStats dataCacheStats() {
        return blockLevelStats;
    }

    /**
     * Metadata cache stats (section 1 in tiered mode).
     * Returns the metadata-specific counters from the second Foyer instance.
     * In single-cache mode, returns the same as {@link #blockLevelStats()} (duplicate section).
     */
    public BlockCacheStats metadataCacheStats() {
        if (metadataCapacityBytes > 0) {
            return new BlockCacheStats(
                overallStats.hits() - blockLevelStats.hits(),
                overallStats.misses() - blockLevelStats.misses(),
                overallStats.hitBytes() - blockLevelStats.hitBytes(),
                overallStats.missBytes() - blockLevelStats.missBytes(),
                overallStats.evictions() - blockLevelStats.evictions(),
                overallStats.evictionBytes() - blockLevelStats.evictionBytes(),
                overallStats.removed() - blockLevelStats.removed(),
                overallStats.removedBytes() - blockLevelStats.removedBytes(),
                0L,
                overallStats.diskBytesUsed() - blockLevelStats.diskBytesUsed(),
                metadataCapacityBytes,
                Math.max(0L, overallStats.activeInBytes() - blockLevelStats.activeInBytes())
            );
        }
        return blockLevelStats;
    }

    /** Data cache configured capacity. 0 if not in tiered mode. */
    public long dataCapacityBytes() {
        return dataCapacityBytes;
    }

    /** Metadata cache configured capacity. 0 if not in tiered mode. */
    public long metadataCapacityBytes() {
        return metadataCapacityBytes;
    }

    /** Whether this snapshot represents a tiered cache (data + metadata). */
    public boolean isTiered() {
        return metadataCapacityBytes > 0;
    }

    private static BlockCacheStats merge(BlockCacheStats data, BlockCacheStats meta) {
        return new BlockCacheStats(
            data.hits() + meta.hits(),
            data.misses() + meta.misses(),
            data.hitBytes() + meta.hitBytes(),
            data.missBytes() + meta.missBytes(),
            data.evictions() + meta.evictions(),
            data.evictionBytes() + meta.evictionBytes(),
            data.removed() + meta.removed(),
            data.removedBytes() + meta.removedBytes(),
            0L,
            data.diskBytesUsed() + meta.diskBytesUsed(),
            data.totalBytes() + meta.totalBytes(),
            data.activeInBytes() + meta.activeInBytes()
        );
    }
}
