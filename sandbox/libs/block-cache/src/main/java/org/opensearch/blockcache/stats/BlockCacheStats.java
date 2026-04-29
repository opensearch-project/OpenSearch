/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Immutable snapshot of block cache statistics.
 *
 * <p>Analogous to
 * {@link org.opensearch.index.store.remote.utils.cache.stats.RefCountedCacheStats}
 * for the Java {@link org.opensearch.index.store.remote.filecache.FileCache},
 * with the addition of {@link #hitBytes()} and {@link #missBytes()} for
 * variable-size entries where byte-level hit rate is more meaningful than
 * count-based hit rate.
 *
 * @opensearch.experimental
 */
public final class BlockCacheStats implements IBlockCacheStats, Writeable, ToXContentFragment {

    /**
     * Ordered fields of a single stats section in the FFM transfer buffer.
     *
     * <p>The ordinal of each constant is its index within one section.
     * The ordinal order <strong>must match</strong> the field order in
     * the native cache stats snapshot function.
     *
     * <p>To add a field: add a constant here in the correct position AND add the
     * corresponding value to the native snapshot array at the same offset.
     * {@link #COUNT} then updates automatically — buffer sizes and section offsets
     * stay correct without further changes.
     */
    public enum Field {
        HIT_COUNT,
        HIT_BYTES,
        MISS_COUNT,
        MISS_BYTES,
        EVICTION_COUNT,
        EVICTION_BYTES,
        USED_BYTES;

        /** Number of fields per section — derived from the enum, not a separate constant. */
        public static final int COUNT = values().length;
    }

    /** Sentinel returned when no block cache is configured. */
    public static final BlockCacheStats EMPTY = new BlockCacheStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    /**
     * Reads one stats section from a raw FFM buffer.
     *
     * <p>Field positions are defined by {@link Field}; {@code offset} selects the section
     * ({@code 0} = overall, {@code Field.COUNT} = block-level).
     *
     * @param raw          buffer returned by the native cache stats snapshot
     * @param offset       start index of the section within {@code raw}
     * @param capacityBytes configured disk capacity in bytes
     * @return an immutable {@code BlockCacheStats} snapshot
     */
    public static BlockCacheStats fromRaw(long[] raw, int offset, long capacityBytes) {
        return new BlockCacheStats(
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

    private final long hitCount;
    private final long hitBytes;
    private final long missCount;
    private final long missBytes;
    private final long evictionCount;
    private final long evictionBytes;
    private final long usedBytes;
    private final long capacityBytes;

    /**
     * Creates an immutable stats snapshot.
     *
     * @param hitCount      number of {@code get()} calls that returned a cached value
     * @param hitBytes      bytes served from cache across all hits
     * @param missCount     number of {@code get()} calls that returned no value
     * @param missBytes     bytes corresponding to missed entries, fetched from remote storage
     * @param evictionCount number of entries removed by LRU pressure
     * @param evictionBytes total bytes removed by LRU pressure
     * @param usedBytes     current bytes resident in the cache on disk
     * @param capacityBytes configured capacity in bytes
     */
    public BlockCacheStats(
        long hitCount,
        long hitBytes,
        long missCount,
        long missBytes,
        long evictionCount,
        long evictionBytes,
        long usedBytes,
        long capacityBytes
    ) {
        this.hitCount = hitCount;
        this.hitBytes = hitBytes;
        this.missCount = missCount;
        this.missBytes = missBytes;
        this.evictionCount = evictionCount;
        this.evictionBytes = evictionBytes;
        this.usedBytes = usedBytes;
        this.capacityBytes = capacityBytes;
    }

    @Override public long hitCount()      { return hitCount;      }
    @Override public long hitBytes()      { return hitBytes;      }
    @Override public long missCount()     { return missCount;     }
    @Override public long missBytes()     { return missBytes;     }
    @Override public long evictionCount() { return evictionCount; }
    @Override public long evictionBytes() { return evictionBytes; }
    @Override public long usedBytes()     { return usedBytes;     }
    @Override public long capacityBytes() { return capacityBytes; }

    // ─────────────────────────────────────────────────────────────────────────
    // Writeable — StreamInput constructor + writeTo
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Deserialises a {@code BlockCacheStats} from the wire.
     * Field order matches {@link #writeTo(StreamOutput)}.
     */
    public BlockCacheStats(StreamInput in) throws IOException {
        this.hitCount      = in.readLong();
        this.hitBytes      = in.readLong();
        this.missCount     = in.readLong();
        this.missBytes     = in.readLong();
        this.evictionCount = in.readLong();
        this.evictionBytes = in.readLong();
        this.usedBytes     = in.readLong();
        this.capacityBytes = in.readLong();
    }

    /**
     * Serialises this snapshot to the wire.
     * Field order matches {@link #BlockCacheStats(StreamInput)}.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hitCount);
        out.writeLong(hitBytes);
        out.writeLong(missCount);
        out.writeLong(missBytes);
        out.writeLong(evictionCount);
        out.writeLong(evictionBytes);
        out.writeLong(usedBytes);
        out.writeLong(capacityBytes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ToXContentFragment
    // ─────────────────────────────────────────────────────────────────────────

    static final class Fields {
        static final String HIT_COUNT       = "hit_count";
        static final String HIT_BYTES       = "hit_bytes";
        static final String MISS_COUNT      = "miss_count";
        static final String MISS_BYTES      = "miss_bytes";
        static final String BYTE_HIT_RATE   = "byte_hit_rate";
        static final String HIT_RATE        = "hit_rate";
        static final String EVICTION_COUNT  = "eviction_count";
        static final String EVICTION_BYTES  = "eviction_bytes";
        static final String USED_IN_BYTES   = "used_in_bytes";
        static final String CAPACITY_IN_BYTES = "capacity_in_bytes";
        static final String USED_PERCENT    = "used_percent";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.HIT_COUNT, hitCount);
        builder.field(Fields.HIT_BYTES, hitBytes);
        builder.field(Fields.MISS_COUNT, missCount);
        builder.field(Fields.MISS_BYTES, missBytes);
        long totalBytes = hitBytes + missBytes;
        builder.field(Fields.BYTE_HIT_RATE, totalBytes == 0 ? 1.0 : (double) hitBytes / totalBytes);
        builder.field(Fields.HIT_RATE, hitRate());
        builder.field(Fields.EVICTION_COUNT, evictionCount);
        builder.field(Fields.EVICTION_BYTES, evictionBytes);
        builder.field(Fields.USED_IN_BYTES, usedBytes);
        builder.field(Fields.CAPACITY_IN_BYTES, capacityBytes);
        builder.field(Fields.USED_PERCENT, usedPercent());
        return builder;
    }
}
