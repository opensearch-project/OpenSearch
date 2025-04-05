/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics for the file cache system that tracks memory usage and performance metrics.
 * {@link FileCache} internally uses a {@link org.opensearch.index.store.remote.utils.cache.SegmentedCache}
 * to manage cached file data in memory segments.
 * This class aggregates statistics across all cache segments including:
 * - Memory usage (total, active, used)
 * - Cache performance (hits, misses, evictions)
 * - Utilization percentages
 * The statistics are exposed via {@link org.opensearch.action.admin.cluster.node.stats.NodeStats}
 * to provide visibility into cache behavior and performance.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public class FileCacheStats implements Writeable, ToXContentFragment {

    private final long timestamp;
    private final long active;
    private final long total;
    private final long used;
    private final long evicted;
    private final long hits;
    private final long misses;
    private final FullFileCacheStats fullFileCacheStats;

    public FileCacheStats(
        final long timestamp,
        final long active,
        final long total,
        final long used,
        final long evicted,
        final long hits,
        final long misses,
        final FullFileCacheStats fullFileCacheStats
    ) {
        this.timestamp = timestamp;
        this.active = active;
        this.total = total;
        this.used = used;
        this.evicted = evicted;
        this.hits = hits;
        this.misses = misses;
        this.fullFileCacheStats = fullFileCacheStats;
    }

    public FileCacheStats(final StreamInput in) throws IOException {
        this.timestamp = in.readLong();
        this.active = in.readLong();
        this.total = in.readLong();
        this.used = in.readLong();
        this.evicted = in.readLong();
        this.hits = in.readLong();
        this.misses = in.readLong();
        this.fullFileCacheStats = new FullFileCacheStats(in);
    }

    public static short calculatePercentage(long used, long max) {
        return max <= 0 ? 0 : (short) (Math.round((100d * used) / max));
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(active);
        out.writeLong(total);
        out.writeLong(used);
        out.writeLong(evicted);
        out.writeLong(hits);
        out.writeLong(misses);
        if (fullFileCacheStats != null) fullFileCacheStats.writeTo(out);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteSizeValue getTotal() {
        return new ByteSizeValue(total);
    }

    public ByteSizeValue getActive() {
        return new ByteSizeValue(active);
    }

    public short getActivePercent() {
        return calculatePercentage(active, used);
    }

    public ByteSizeValue getUsed() {
        return new ByteSizeValue(used);
    }

    public short getUsedPercent() {
        return calculatePercentage(getUsed().getBytes(), total);
    }

    public ByteSizeValue getEvicted() {
        return new ByteSizeValue(evicted);
    }

    public long getCacheHits() {
        return hits;
    }

    public long getCacheMisses() {
        return misses;
    }

    public FullFileCacheStats fullFileCacheStats() {
        return fullFileCacheStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FILE_CACHE);
        builder.field(Fields.TIMESTAMP, getTimestamp());
        builder.humanReadableField(Fields.ACTIVE_IN_BYTES, Fields.ACTIVE, getActive());
        builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
        builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, getUsed());
        builder.humanReadableField(Fields.EVICTIONS_IN_BYTES, Fields.EVICTIONS, getEvicted());
        builder.field(Fields.ACTIVE_PERCENT, getActivePercent());
        builder.field(Fields.USED_PERCENT, getUsedPercent());
        builder.field(Fields.HIT_COUNT, getCacheHits());
        builder.field(Fields.MISS_COUNT, getCacheMisses());
        if (fullFileCacheStats != null) {
            fullFileCacheStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String FILE_CACHE = "file_cache";
        static final String TIMESTAMP = "timestamp";
        static final String ACTIVE = "active";
        static final String ACTIVE_IN_BYTES = "active_in_bytes";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String EVICTIONS_IN_BYTES = "evictions_in_bytes";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";

        static final String ACTIVE_PERCENT = "active_percent";
        static final String USED_PERCENT = "used_percent";

        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
    }
}
