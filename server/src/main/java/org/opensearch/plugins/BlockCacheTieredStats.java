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
 * Per-tier breakdown for tiered block caches (e.g. separate data and metadata SSDs).
 *
 * <p>Rendered as {@code data_cache_stats} and {@code metadata_cache_stats} sub-objects
 * within the {@code block_cache} section of the node stats response. Only present
 * when the block cache is configured in tiered mode.
 *
 * <p>The {@code removed} counters track explicit invalidations (e.g. shard deletion
 * via {@code evict_prefix}, prune via {@code clear()}) on each tier independently
 * from capacity-driven evictions.
 *
 * <p>Wire format is gated at {@link org.opensearch.Version#V_3_8_0} via the outer
 * {@link BlockCacheStats} reader; the inner field set defined here was finalized
 * before the V_3_8_0 release and does not require a separate gate.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlockCacheTieredStats(long dataHits, long dataMisses, long dataHitBytes, long dataMissBytes, long dataEvictions,
    long dataEvictionBytes, long dataRemoved, long dataRemovedBytes, long dataUsedBytes, long dataCapacityBytes, long dataActiveInBytes,
    long metadataHits, long metadataMisses, long metadataHitBytes, long metadataMissBytes, long metadataEvictions,
    long metadataEvictionBytes, long metadataRemoved, long metadataRemovedBytes, long metadataUsedBytes, long metadataCapacityBytes,
    long metadataActiveInBytes) implements Writeable, ToXContentFragment {

    public BlockCacheTieredStats(StreamInput in) throws IOException {
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
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(dataHits);
        out.writeLong(dataMisses);
        out.writeLong(dataHitBytes);
        out.writeLong(dataMissBytes);
        out.writeLong(dataEvictions);
        out.writeLong(dataEvictionBytes);
        out.writeLong(dataRemoved);
        out.writeLong(dataRemovedBytes);
        out.writeLong(dataUsedBytes);
        out.writeLong(dataCapacityBytes);
        out.writeLong(dataActiveInBytes);
        out.writeLong(metadataHits);
        out.writeLong(metadataMisses);
        out.writeLong(metadataHitBytes);
        out.writeLong(metadataMissBytes);
        out.writeLong(metadataEvictions);
        out.writeLong(metadataEvictionBytes);
        out.writeLong(metadataRemoved);
        out.writeLong(metadataRemovedBytes);
        out.writeLong(metadataUsedBytes);
        out.writeLong(metadataCapacityBytes);
        out.writeLong(metadataActiveInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("data_cache_stats");
        builder.field("active_in_bytes", dataActiveInBytes);
        builder.field("used_in_bytes", dataUsedBytes);
        builder.field("capacity_in_bytes", dataCapacityBytes);
        builder.field("evictions_in_bytes", dataEvictionBytes);
        builder.field("removed_in_bytes", dataRemovedBytes);
        builder.field("hit_count", dataHits);
        builder.field("miss_count", dataMisses);
        builder.field("hit_bytes", dataHitBytes);
        builder.field("miss_bytes", dataMissBytes);
        builder.field("eviction_count", dataEvictions);
        builder.field("removed_count", dataRemoved);
        builder.endObject();

        builder.startObject("metadata_cache_stats");
        builder.field("active_in_bytes", metadataActiveInBytes);
        builder.field("used_in_bytes", metadataUsedBytes);
        builder.field("capacity_in_bytes", metadataCapacityBytes);
        builder.field("evictions_in_bytes", metadataEvictionBytes);
        builder.field("removed_in_bytes", metadataRemovedBytes);
        builder.field("hit_count", metadataHits);
        builder.field("miss_count", metadataMisses);
        builder.field("hit_bytes", metadataHitBytes);
        builder.field("miss_bytes", metadataMissBytes);
        builder.field("eviction_count", metadataEvictions);
        builder.field("removed_count", metadataRemoved);
        builder.endObject();

        return builder;
    }
}
