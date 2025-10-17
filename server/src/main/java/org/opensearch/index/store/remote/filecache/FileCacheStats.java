/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;

import java.io.IOException;

import static org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.calculatePercentage;

/**
 * Statistics for the file cache system that tracks memory usage and performance metrics.
 * Aggregates statistics across all cache segments including:
 * - Memory usage: active and used bytes.
 * - Cache performance: hit counts and eviction counts.
 * - Utilization: active percentage of total used memory.
 * The statistics are exposed as part of {@link AggregateFileCacheStats} and via {@link org.opensearch.action.admin.cluster.node.stats.NodeStats}
 * to provide visibility into cache behavior and performance.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public class FileCacheStats implements Writeable, ToXContentFragment {

    private final long active;
    private final long total;
    private final long used;
    private final long pinned;
    private final long evicted;
    private final long hits;
    private final long misses;
    private final FileCacheStatsType statsType;

    @InternalApi
    public FileCacheStats(
        final long active,
        long total,
        final long used,
        final long pinned,
        final long evicted,
        final long hits,
        long misses,
        FileCacheStatsType statsType
    ) {
        this.active = active;
        this.total = total;
        this.used = used;
        this.pinned = pinned;
        this.evicted = evicted;
        this.hits = hits;
        this.misses = misses;
        this.statsType = statsType;
    }

    @InternalApi
    public FileCacheStats(final StreamInput in) throws IOException {
        this.statsType = FileCacheStatsType.fromString(in.readString());
        this.active = in.readLong();
        this.total = in.readLong();
        this.used = in.readLong();
        this.pinned = in.readLong();
        this.evicted = in.readLong();
        this.hits = in.readLong();
        this.misses = in.readLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(statsType.toString());
        out.writeLong(active);
        out.writeLong(total);
        out.writeLong(used);
        out.writeLong(pinned);
        out.writeLong(evicted);
        out.writeLong(hits);
        out.writeLong(misses);
    }

    public long getActive() {
        return active;
    }

    public long getUsed() {
        return used;
    }

    public long getEvicted() {
        return evicted;
    }

    public long getHits() {
        return hits;
    }

    public short getActivePercent() {
        return calculatePercentage(active, used);
    }

    public long getTotal() {
        return total;
    }

    public long getPinnedUsage() {
        return pinned;
    }

    public short getUsedPercent() {
        return calculatePercentage(getUsed(), total);
    }

    public long getCacheHits() {
        return hits;
    }

    public long getCacheMisses() {
        return misses;
    }

    static final class Fields {
        static final String ACTIVE = "active";
        static final String ACTIVE_IN_BYTES = "active_in_bytes";
        static final String USED = "used";
        static final String PINNED = "pinned";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String EVICTIONS_IN_BYTES = "evictions_in_bytes";
        static final String ACTIVE_PERCENT = "active_percent";
        static final String HIT_COUNT = "hit_count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(statsType.toString());
        builder.humanReadableField(FileCacheStats.Fields.ACTIVE_IN_BYTES, FileCacheStats.Fields.ACTIVE, new ByteSizeValue(getActive()));
        builder.humanReadableField(FileCacheStats.Fields.USED_IN_BYTES, FileCacheStats.Fields.USED, new ByteSizeValue(getUsed()));
        builder.humanReadableField(FileCacheStats.Fields.USED_IN_BYTES, Fields.PINNED, new ByteSizeValue(getPinnedUsage()));
        builder.humanReadableField(
            FileCacheStats.Fields.EVICTIONS_IN_BYTES,
            FileCacheStats.Fields.EVICTIONS,
            new ByteSizeValue(getEvicted())
        );
        builder.field(FileCacheStats.Fields.ACTIVE_PERCENT, getActivePercent());
        builder.field(FileCacheStats.Fields.HIT_COUNT, getHits());
        builder.endObject();
        return builder;
    }
}
