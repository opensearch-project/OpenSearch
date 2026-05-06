/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

/**
 * Internal stats snapshot for the Foyer block cache, containing two sections
 * read from the native FFM transfer buffer:
 * <ul>
 *   <li>{@code overallStats} — cross-tier rollup</li>
 *   <li>{@code blockLevelStats} — disk-tier (block-level) stats</li>
 * </ul>
 *
 * <p>This is Foyer-specific. Core only sees the flat
 * {@link org.opensearch.plugins.BlockCacheStats} record returned by
 * {@link FoyerBlockCache#stats()}.
 *
 * @opensearch.internal
 */
public final class FoyerBlockCacheStats {

    /**
     * Ordered fields of a single stats section in the FFM transfer buffer.
     * The ordinal of each constant is its index within one section.
     * Must match the field order in the native {@code foyer_snapshot_stats} output.
     */
    public enum Field {
        HIT_COUNT,
        HIT_BYTES,
        MISS_COUNT,
        MISS_BYTES,
        EVICTION_COUNT,
        EVICTION_BYTES,
        USED_BYTES;

        /** Number of fields per section. */
        public static final int COUNT = values().length;
    }

    private final long hitCount;
    private final long hitBytes;
    private final long missCount;
    private final long missBytes;
    private final long evictionCount;
    private final long evictionBytes;
    private final long usedBytes;
    private final long capacityBytes;

    public FoyerBlockCacheStats(
        long hitCount, long hitBytes,
        long missCount, long missBytes,
        long evictionCount, long evictionBytes,
        long usedBytes, long capacityBytes
    ) {
        this.hitCount      = hitCount;
        this.hitBytes      = hitBytes;
        this.missCount     = missCount;
        this.missBytes     = missBytes;
        this.evictionCount = evictionCount;
        this.evictionBytes = evictionBytes;
        this.usedBytes     = usedBytes;
        this.capacityBytes = capacityBytes;
    }

    /** Reads one section from a raw FFM buffer at the given offset. */
    public static FoyerBlockCacheStats fromRaw(long[] raw, int offset, long capacityBytes) {
        return new FoyerBlockCacheStats(
            raw[offset + Field.HIT_COUNT.ordinal()],
            raw[offset + Field.HIT_BYTES.ordinal()],
            raw[offset + Field.MISS_COUNT.ordinal()],
            raw[offset + Field.MISS_BYTES.ordinal()],
            raw[offset + Field.EVICTION_COUNT.ordinal()],
            raw[offset + Field.EVICTION_BYTES.ordinal()],
            raw[offset + Field.USED_BYTES.ordinal()],
            capacityBytes
        );
    }

    public long hitCount()      { return hitCount;      }
    public long hitBytes()      { return hitBytes;      }
    public long missCount()     { return missCount;     }
    public long missBytes()     { return missBytes;     }
    public long evictionCount() { return evictionCount; }
    public long evictionBytes() { return evictionBytes; }
    public long usedBytes()     { return usedBytes;     }
    public long capacityBytes() { return capacityBytes; }
}
