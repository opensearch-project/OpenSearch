/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.BlockCacheStats;

import java.io.IOException;

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
 * Foyer-aware code that needs the per-tier breakdown or the rich
 * {@code GET /_nodes/foyer_cache/stats} REST output obtains this object via
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
 * <h3>Wire format</h3>
 * Serialised as two consecutive {@link BlockCacheStats} instances
 * (overall first, block-level second). Used by
 * {@code TransportFoyerCacheStatsAction} to carry per-node stats to the
 * coordinating node.
 *
 * @opensearch.internal
 */
public final class FoyerAggregatedStats implements Writeable, ToXContentFragment {

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

    // ─── Wire serialization ───────────────────────────────────────────────────

    /**
     * Deserialises from the transport wire.
     * Order must match {@link #writeTo(StreamOutput)}.
     */
    public FoyerAggregatedStats(StreamInput in) throws IOException {
        // BlockCacheStats is a record; read fields in declaration order.
        this.overallStats    = readBlockCacheStats(in);
        this.blockLevelStats = readBlockCacheStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeBlockCacheStats(out, overallStats);
        writeBlockCacheStats(out, blockLevelStats);
    }

    private static BlockCacheStats readBlockCacheStats(StreamInput in) throws IOException {
        return new BlockCacheStats(
            in.readLong(),  // hits
            in.readLong(),  // misses
            in.readLong(),  // hitBytes
            in.readLong(),  // missBytes
            in.readLong(),  // evictions
            in.readLong(),  // evictionBytes
            in.readLong(),  // removes
            in.readLong(),  // removeBytes
            in.readLong(),  // memoryBytesUsed
            in.readLong(),  // diskBytesUsed
            in.readLong()   // totalBytes
        );
    }

    private static void writeBlockCacheStats(StreamOutput out, BlockCacheStats s) throws IOException {
        out.writeLong(s.hits());
        out.writeLong(s.misses());
        out.writeLong(s.hitBytes());
        out.writeLong(s.missBytes());
        out.writeLong(s.evictions());
        out.writeLong(s.evictionBytes());
        out.writeLong(s.removes());
        out.writeLong(s.removeBytes());
        out.writeLong(s.memoryBytesUsed());
        out.writeLong(s.diskBytesUsed());
        out.writeLong(s.totalBytes());
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
     * analysis, exposed via {@code GET /_nodes/foyer_cache/stats}.
     */
    public BlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }

    // ─── XContent rendering ───────────────────────────────────────────────────

    /**
     * Renders both sections into the current XContent context under
     * {@code "foyer_block_cache"}. Used by the
     * {@code GET /_nodes/foyer_cache/stats} REST endpoint.
     *
     * <p>Example output:
     * <pre>{@code
     * "foyer_block_cache": {
     *   "overall": {
     *     "hit_count": 1234,
     *     "hit_bytes": "256mb",
     *     "hit_bytes_in_bytes": 268435456,
     *     ...
     *   },
     *   "block_level": { ... }
     * }
     * }</pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FOYER_BLOCK_CACHE);
        renderSection(builder, params, overallStats,    Fields.OVERALL);
        renderSection(builder, params, blockLevelStats, Fields.BLOCK_LEVEL);
        builder.endObject();
        return builder;
    }

    private static void renderSection(
        XContentBuilder builder, Params params, BlockCacheStats s, String name
    ) throws IOException {
        builder.startObject(name);
        builder.field(Fields.HIT_COUNT,       s.hits());
        builder.humanReadableField(Fields.HIT_BYTES_IN_BYTES,       Fields.HIT_BYTES,       new ByteSizeValue(s.hitBytes()));
        builder.field(Fields.MISS_COUNT,      s.misses());
        builder.humanReadableField(Fields.MISS_BYTES_IN_BYTES,      Fields.MISS_BYTES,      new ByteSizeValue(s.missBytes()));
        builder.field(Fields.EVICTION_COUNT,  s.evictions());
        builder.humanReadableField(Fields.EVICTION_BYTES_IN_BYTES,  Fields.EVICTION_BYTES,  new ByteSizeValue(s.evictionBytes()));
        builder.humanReadableField(Fields.USED_BYTES_IN_BYTES,      Fields.USED_BYTES,      new ByteSizeValue(s.diskBytesUsed()));
        builder.humanReadableField(Fields.CAPACITY_BYTES_IN_BYTES,  Fields.CAPACITY_BYTES,  new ByteSizeValue(s.totalBytes()));
        builder.endObject();
    }

    static final class Fields {
        static final String FOYER_BLOCK_CACHE       = "foyer_block_cache";
        static final String OVERALL                 = "overall";
        static final String BLOCK_LEVEL             = "block_level";
        static final String HIT_COUNT               = "hit_count";
        static final String HIT_BYTES               = "hit_bytes";
        static final String HIT_BYTES_IN_BYTES      = "hit_bytes_in_bytes";
        static final String MISS_COUNT              = "miss_count";
        static final String MISS_BYTES              = "miss_bytes";
        static final String MISS_BYTES_IN_BYTES     = "miss_bytes_in_bytes";
        static final String EVICTION_COUNT          = "eviction_count";
        static final String EVICTION_BYTES          = "eviction_bytes";
        static final String EVICTION_BYTES_IN_BYTES = "eviction_bytes_in_bytes";
        static final String USED_BYTES              = "used_bytes";
        static final String USED_BYTES_IN_BYTES     = "used_bytes_in_bytes";
        static final String CAPACITY_BYTES          = "capacity_bytes";
        static final String CAPACITY_BYTES_IN_BYTES = "capacity_bytes_in_bytes";
    }
}
