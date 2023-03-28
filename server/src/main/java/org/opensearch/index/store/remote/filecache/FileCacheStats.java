/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics on file cache
 *
 * @opensearch.internal
 */
public class FileCacheStats implements Writeable, ToXContentFragment {

    private final long timestamp;
    private final long active;
    private final long total;
    private final long used;
    private final long evicted;
    private final long removed;
    private final long replaced;
    private final long hits;
    private final long miss;

    public FileCacheStats(
        final long timestamp,
        final long active,
        final long total,
        final long used,
        final long evicted,
        final long removed,
        final long replaced,
        final long hits,
        final long miss
    ) {
        this.timestamp = timestamp;
        this.active = active;
        this.total = total;
        this.used = used;
        this.evicted = evicted;
        this.removed = removed;
        this.replaced = replaced;
        this.hits = hits;
        this.miss = miss;
    }

    public FileCacheStats(final StreamInput in) throws IOException {
        this.timestamp = in.readLong();
        this.active = in.readLong();
        this.total = in.readLong();
        this.used = in.readLong();
        this.evicted = in.readLong();
        this.removed = in.readLong();
        this.replaced = in.readLong();
        this.hits = in.readLong();
        this.miss = in.readLong();
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
        out.writeLong(removed);
        out.writeLong(replaced);
        out.writeLong(hits);
        out.writeLong(miss);
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

    public ByteSizeValue getRemoved() {
        return new ByteSizeValue(removed);
    }

    public long getReplacedCount() {
        return replaced;
    }

    public long getCacheHits() {
        return hits;
    }

    public long getCacheMiss() {
        return miss;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FILE_CACHE);
        builder.field(Fields.TIMESTAMP, getTimestamp());
        builder.humanReadableField(Fields.ACTIVE_IN_BYTES, Fields.ACTIVE, getActive());
        builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
        builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, getUsed());
        builder.humanReadableField(Fields.EVICTED_IN_BYTES, Fields.EVICTED, getEvicted());
        builder.humanReadableField(Fields.REMOVED_IN_BYTES, Fields.REMOVED, getRemoved());
        builder.field(Fields.REPLACED_COUNT, getReplacedCount());
        builder.field(Fields.ACTIVE_PERCENT, getActivePercent());
        builder.field(Fields.USED_PERCENT, getUsedPercent());
        builder.field(Fields.CACHE_HITS, getCacheHits());
        builder.field(Fields.CACHE_MISS, getCacheMiss());
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
        static final String EVICTED = "evicted";
        static final String EVICTED_IN_BYTES = "evicted_in_bytes";
        static final String REMOVED = "removed";
        static final String REMOVED_IN_BYTES = "removed_in_bytes";
        static final String REPLACED_COUNT = "replaced_count";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";

        static final String ACTIVE_PERCENT = "active_percent";
        static final String USED_PERCENT = "used_percent";

        static final String CACHE_HITS = "cache_hits";
        static final String CACHE_MISS = "cache_miss";
    }
}
