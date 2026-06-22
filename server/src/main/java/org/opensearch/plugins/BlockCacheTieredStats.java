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
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlockCacheTieredStats(long dataHits, long dataMisses, long dataHitBytes, long dataMissBytes, long dataEvictions,
    long dataEvictionBytes, long dataUsedBytes, long dataCapacityBytes, long dataActiveInBytes, long metadataHits, long metadataMisses,
    long metadataHitBytes, long metadataMissBytes, long metadataEvictions, long metadataEvictionBytes, long metadataUsedBytes,
    long metadataCapacityBytes, long metadataActiveInBytes) implements Writeable, ToXContentFragment {

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
        out.writeLong(dataUsedBytes);
        out.writeLong(dataCapacityBytes);
        out.writeLong(dataActiveInBytes);
        out.writeLong(metadataHits);
        out.writeLong(metadataMisses);
        out.writeLong(metadataHitBytes);
        out.writeLong(metadataMissBytes);
        out.writeLong(metadataEvictions);
        out.writeLong(metadataEvictionBytes);
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
        builder.field("hit_count", dataHits);
        builder.field("miss_count", dataMisses);
        builder.field("hit_bytes", dataHitBytes);
        builder.field("miss_bytes", dataMissBytes);
        builder.field("eviction_count", dataEvictions);
        builder.endObject();

        builder.startObject("metadata_cache_stats");
        builder.field("active_in_bytes", metadataActiveInBytes);
        builder.field("used_in_bytes", metadataUsedBytes);
        builder.field("capacity_in_bytes", metadataCapacityBytes);
        builder.field("evictions_in_bytes", metadataEvictionBytes);
        builder.field("hit_count", metadataHits);
        builder.field("miss_count", metadataMisses);
        builder.field("hit_bytes", metadataHitBytes);
        builder.field("miss_bytes", metadataMissBytes);
        builder.field("eviction_count", metadataEvictions);
        builder.endObject();

        return builder;
    }
}
