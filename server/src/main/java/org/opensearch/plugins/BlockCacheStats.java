/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Point-in-time snapshot of {@link BlockCache} counters.
 *
 * <p>Emitted for node-stats reporting and merging into
 * {@link org.opensearch.index.store.remote.filecache.AggregateFileCacheStats}.
 * Every {@code BlockCache} implementation is expected to surface all counters;
 * implementations that do not track a particular metric should return zero for
 * that field rather than throwing.
 *
 * <p><b>Field semantics:</b></p>
 * <ul>
 *   <li>{@code hits}            — cumulative count of lookups served from the cache.</li>
 *   <li>{@code misses}          — cumulative count of lookups that did not find an entry.</li>
 *   <li>{@code hitBytes}        — cumulative bytes served from cache hits
 *       (bandwidth actually saved by the cache).</li>
 *   <li>{@code missBytes}       — cumulative bytes fetched on cache misses
 *       (bytes that had to be read from the backing store).</li>
 *   <li>{@code evictions}       — cumulative count of LRU / capacity-driven eviction events.</li>
 *   <li>{@code evictionBytes}   — cumulative bytes displaced by evictions.
 *       Used by {@code NodeCacheService.mergeStats()} to populate
 *       {@code evictions_in_bytes} in the REST output.</li>
 *   <li>{@code removed}         — cumulative count of explicit invalidations
 *       (e.g. delete-index, segment rename, merge completion).
 *       Mirrors the {@code removed} field in {@code FileCacheStats}.</li>
 *   <li>{@code removedBytes}    — cumulative bytes explicitly removed.
 *       Used by {@code NodeCacheService.mergeStats()} to populate
 *       {@code removed_in_bytes} in the REST output.</li>
 *   <li>{@code memoryBytesUsed} — current bytes occupied in the in-memory tier
 *       (zero for disk-only implementations such as Foyer).</li>
 *   <li>{@code diskBytesUsed}   — current bytes occupied in the on-disk tier
 *       (zero for memory-only implementations).</li>
 *   <li>{@code totalBytes}      — configured total capacity (memory + disk).
 *       Used by {@code NodeCacheService.mergeStats()} to populate
 *       {@code total_in_bytes} in the REST output.</li>
 *   <li>{@code activeInBytes}   — current bytes of reads in-flight (gauge, not cumulative).
 *       Represents the byte volume of cache lookups currently executing concurrently.
 * </ul>
 *
 * <p>Values are a snapshot at construction time and are not guaranteed to be
 * internally consistent across concurrent cache activity.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlockCacheStats(long hits, long misses, long hitBytes, long missBytes, long evictions, long evictionBytes, long removed,
    long removedBytes, long memoryBytesUsed, long diskBytesUsed, long totalBytes, long activeInBytes, BlockCacheTieredStats tieredStats)
    implements
        Writeable,
        ToXContentFragment {

    public BlockCacheStats(
        long hits,
        long misses,
        long hitBytes,
        long missBytes,
        long evictions,
        long evictionBytes,
        long removed,
        long removedBytes,
        long memoryBytesUsed,
        long diskBytesUsed,
        long totalBytes,
        long activeInBytes
    ) {
        this(
            hits,
            misses,
            hitBytes,
            missBytes,
            evictions,
            evictionBytes,
            removed,
            removedBytes,
            memoryBytesUsed,
            diskBytesUsed,
            totalBytes,
            activeInBytes,
            null
        );
    }

    public BlockCacheStats(StreamInput in) throws IOException {
        this(
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.getVersion().onOrAfter(org.opensearch.Version.V_3_8_0) ? in.readOptionalWriteable(BlockCacheTieredStats::new) : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hits);
        out.writeLong(misses);
        out.writeLong(hitBytes);
        out.writeLong(missBytes);
        out.writeLong(evictions);
        out.writeLong(evictionBytes);
        out.writeLong(removed);
        out.writeLong(removedBytes);
        out.writeLong(memoryBytesUsed);
        out.writeLong(diskBytesUsed);
        out.writeLong(totalBytes);
        out.writeLong(activeInBytes);
        if (out.getVersion().onOrAfter(org.opensearch.Version.V_3_8_0)) {
            out.writeOptionalWriteable(tieredStats);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("block_cache");
        renderBlockCacheSubStats(builder, "over_all_stats");
        renderFullFileSubStats(builder, "full_file_stats");
        renderBlockCacheSubStats(builder, "block_file_stats");
        renderPinnedSubStats(builder, "pinned_file_stats");
        if (tieredStats != null) {
            tieredStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    private void renderBlockCacheSubStats(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name);
        builder.field("active_in_bytes", activeInBytes);
        builder.field("used_in_bytes", diskBytesUsed + memoryBytesUsed);
        builder.field("pinned_in_bytes", 0);
        builder.field("evictions_in_bytes", evictionBytes);
        builder.field("removed_in_bytes", removedBytes);
        builder.field("active_percent", 0);
        builder.field("hit_count", hits);
        builder.field("miss_count", misses);
        builder.endObject();
    }

    private static void renderFullFileSubStats(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name);
        builder.field("active_in_bytes", 0);
        builder.field("used_in_bytes", 0);
        builder.field("pinned_in_bytes", 0);
        builder.field("evictions_in_bytes", 0);
        builder.field("removed_in_bytes", 0);
        builder.field("active_percent", 0);
        builder.field("hit_count", 0);
        builder.field("miss_count", 0);
        builder.endObject();
    }

    private static void renderPinnedSubStats(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name);
        builder.field("active_in_bytes", 0);
        builder.field("used_in_bytes", 0);
        builder.field("pinned_in_bytes", 0);
        builder.field("evictions_in_bytes", 0);
        builder.field("removed_in_bytes", 0);
        builder.field("active_percent", 0);
        builder.field("hit_count", 0);
        builder.field("miss_count", 0);
        builder.endObject();
    }
}
