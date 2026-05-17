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
     * Must match the native {@code foyer_snapshot_stats} field order.
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
        REMOVED_BYTES;

        static final int COUNT = values().length;
    }

    /** Buffer size for {@link FoyerBridge#snapshotStats}: 2 sections × Field.COUNT values. */
    static final int STATS_BUFFER_SIZE = Field.COUNT * 2;

    /** Cross-tier rollup — section 0 of the FFM buffer. */
    private final BlockCacheStats overallStats;

    /** Disk-tier only — section 1 of the FFM buffer. */
    private final BlockCacheStats blockLevelStats;

    private FoyerAggregatedStats(BlockCacheStats overallStats, BlockCacheStats blockLevelStats) {
        this.overallStats = overallStats;
        this.blockLevelStats = blockLevelStats;
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
            0L,                                          // memoryBytesUsed — disk-only cache
            raw[offset + Field.USED_BYTES.ordinal()],
            capacityBytes
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
}
