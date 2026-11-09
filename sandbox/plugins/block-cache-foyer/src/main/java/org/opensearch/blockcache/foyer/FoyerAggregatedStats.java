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
 * <p>Two construction modes:
 * <ul>
 *   <li><b>Single-cache</b> — built via {@link #snapshot(long[], long)}. The FFM
 *       buffer's section 0 (overall) and section 1 (block-level / disk-tier) are
 *       written by Rust as identical copies; we keep both for back-compat with
 *       existing tests.</li>
 *   <li><b>Tiered</b> — built via {@link #snapshotTiered(long[], long, long)}.
 *       Section 0 is the data tier and section 1 is the metadata tier. Both
 *       sections are stored directly; {@link #overallStats()} returns the
 *       eagerly-computed merge of the two so callers see a single rolled-up view.</li>
 * </ul>
 *
 * <p>Core only ever sees {@link #overallStats()} via {@link FoyerBlockCache#stats()}.
 * Foyer-aware code can reach for {@link #dataCacheStats()} / {@link #metadataCacheStats()}
 * which return {@code null} in single-cache mode and the per-tier sections in tiered mode.
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

    /** Cross-tier rollup. In tiered mode = {@code merge(data, meta)}; in single mode = section 0. */
    private final BlockCacheStats overallStats;

    /**
     * Disk-tier breakdown. In tiered mode == {@link #dataCacheStats}; in single mode = section 1
     * of the FFM buffer (which Rust mirrors from section 0 in single-cache mode).
     * Kept as a separate accessor for back-compat with existing tests.
     */
    private final BlockCacheStats blockLevelStats;

    /** Tiered mode only. {@code null} in single-cache mode. */
    private final BlockCacheStats dataCacheStats;

    /** Tiered mode only. {@code null} in single-cache mode. */
    private final BlockCacheStats metadataCacheStats;

    /** Data cache configured capacity. {@code 0} in single-cache mode. */
    private final long dataCapacityBytes;

    /** Metadata cache configured capacity. {@code 0} in single-cache mode. */
    private final long metadataCapacityBytes;

    private FoyerAggregatedStats(
        BlockCacheStats overallStats,
        BlockCacheStats blockLevelStats,
        BlockCacheStats dataCacheStats,
        BlockCacheStats metadataCacheStats,
        long dataCapacityBytes,
        long metadataCapacityBytes
    ) {
        this.overallStats = overallStats;
        this.blockLevelStats = blockLevelStats;
        this.dataCacheStats = dataCacheStats;
        this.metadataCacheStats = metadataCacheStats;
        this.dataCapacityBytes = dataCapacityBytes;
        this.metadataCapacityBytes = metadataCapacityBytes;
    }

    /**
     * Parses both sections from the FFM stats buffer for a single-cache (non-tiered) Foyer.
     *
     * @param raw           stats buffer; length must be {@code >= 2 * Field.COUNT}
     * @param capacityBytes configured disk capacity for this cache instance
     */
    public static FoyerAggregatedStats snapshot(long[] raw, long capacityBytes) {
        BlockCacheStats overall = readSection(raw, 0, capacityBytes);
        BlockCacheStats blockLevel = readSection(raw, Field.COUNT, capacityBytes);
        // dataCacheStats / metadataCacheStats are null in single-cache mode — callers asking
        // for per-tier breakdown are expected to check isTiered() first.
        return new FoyerAggregatedStats(overall, blockLevel, null, null, 0L, 0L);
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
     * <p>{@link #dataCacheStats()} and {@link #metadataCacheStats()} return their respective
     * sections directly — no subtraction, no derivation — so per-tier counters reflect
     * exactly what the native side emitted. {@link #overallStats()} is the eagerly-computed
     * merge of both tiers for callers that want a single rolled-up view.
     *
     * @param raw                 stats buffer; length must be {@code >= 2 * Field.COUNT}
     * @param dataCapacityBytes   data cache disk capacity
     * @param metaCapacityBytes   metadata cache disk capacity
     */
    public static FoyerAggregatedStats snapshotTiered(long[] raw, long dataCapacityBytes, long metaCapacityBytes) {
        BlockCacheStats data = readSection(raw, 0, dataCapacityBytes);
        BlockCacheStats meta = readSection(raw, Field.COUNT, metaCapacityBytes);
        BlockCacheStats overall = merge(data, meta);
        // blockLevelStats == data is intentional: the historical "block-level" view in tiered
        // mode pointed at the data tier, and existing tests assert on that. Keep it stable.
        return new FoyerAggregatedStats(overall, data, data, meta, dataCapacityBytes, metaCapacityBytes);
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

    /**
     * Disk-tier-only stats — SSD I/O and eviction pressure breakdown.
     * In tiered mode this returns the data tier (back-compat with the historical contract).
     * Use {@link #dataCacheStats()} / {@link #metadataCacheStats()} for explicit per-tier access.
     */
    public BlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }

    /**
     * Data cache stats. Returns the data-tier section directly in tiered mode,
     * or {@code null} in single-cache mode.
     */
    public BlockCacheStats dataCacheStats() {
        return dataCacheStats;
    }

    /**
     * Metadata cache stats. Returns the metadata-tier section directly in tiered mode,
     * or {@code null} in single-cache mode.
     */
    public BlockCacheStats metadataCacheStats() {
        return metadataCacheStats;
    }

    /** Data cache configured capacity. {@code 0} in single-cache mode. */
    public long dataCapacityBytes() {
        return dataCapacityBytes;
    }

    /** Metadata cache configured capacity. {@code 0} in single-cache mode. */
    public long metadataCapacityBytes() {
        return metadataCapacityBytes;
    }

    /** Whether this snapshot represents a tiered cache (data + metadata). */
    public boolean isTiered() {
        return metadataCacheStats != null;
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
