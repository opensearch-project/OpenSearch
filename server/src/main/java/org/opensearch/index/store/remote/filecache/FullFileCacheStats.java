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
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics on full file cache.
 */
@PublicApi(since = "3.0.0")
public class FullFileCacheStats implements Writeable, ToXContentFragment {
    private final long activeFullFileBytes;
    private final long activeFullFileCount;
    private final long usedFullFileBytes;
    private final long usedFullFileCount;
    private final long evictedFullFileBytes;
    private final long evictedFullFileCount;
    private final long hitCount;
    private final long missCount;

    public FullFileCacheStats(
        final long activeFullFileBytes,
        final long activeFullFileCount,
        final long usedFullFileBytes,
        final long usedFullFileCount,
        final long evictedFullFileBytes,
        final long evictedFullFileCount,
        final long hitCount,
        final long missCount
    ) {
        this.activeFullFileBytes = activeFullFileBytes;
        this.activeFullFileCount = activeFullFileCount;
        this.usedFullFileBytes = usedFullFileBytes;
        this.usedFullFileCount = usedFullFileCount;
        this.evictedFullFileBytes = evictedFullFileBytes;
        this.evictedFullFileCount = evictedFullFileCount;
        this.hitCount = hitCount;
        this.missCount = missCount;
    }

    public FullFileCacheStats(final StreamInput in) throws IOException {
        this.activeFullFileBytes = in.readLong();
        this.activeFullFileCount = in.readLong();
        this.usedFullFileBytes = in.readLong();
        this.usedFullFileCount = in.readLong();
        this.evictedFullFileBytes = in.readLong();
        this.evictedFullFileCount = in.readLong();
        this.hitCount = in.readLong();
        this.missCount = in.readLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeLong(activeFullFileBytes);
        out.writeLong(activeFullFileCount);
        out.writeLong(usedFullFileBytes);
        out.writeLong(usedFullFileCount);
        out.writeLong(evictedFullFileBytes);
        out.writeLong(evictedFullFileCount);
        out.writeLong(hitCount);
        out.writeLong(missCount);
    }

    public long getActiveFullFileBytes() {
        return activeFullFileBytes;
    }

    public long getActiveFullFileCount() {
        return activeFullFileCount;
    }

    public long getUsedFullFileBytes() {
        return usedFullFileBytes;
    }

    public long getUsedFullFileCount() {
        return usedFullFileCount;
    }

    public long getEvictedFullFileBytes() {
        return evictedFullFileBytes;
    }

    public long getEvictedFullFileCount() {
        return evictedFullFileCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    static final class Fields {
        static final String FULL_FILE_STATS = "full_file_stats";
        static final String ACTIVE_FILE_BYTES = "active_file_in_bytes";
        static final String ACTIVE_FILE_COUNT = "active_file_count";
        static final String USED_FULL_FILE_COUNT = "used_full_file_count";
        static final String USED_FULL_FILE_BYTES = "used_full_file_bytes";
        static final String EVICTED_FULL_FILE_COUNT = "evicted_full_file_count";
        static final String EVICTED_FULL_FILE_BYTES = "evicted_full_file_bytes";
        static final String HITS = "cache_hits";
        static final String MISS = "cache_miss";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FULL_FILE_STATS);
        builder.field(Fields.ACTIVE_FILE_COUNT, getActiveFullFileCount());
        builder.field(Fields.ACTIVE_FILE_BYTES, getActiveFullFileBytes());
        builder.field(Fields.USED_FULL_FILE_COUNT, getUsedFullFileCount());
        builder.field(Fields.USED_FULL_FILE_BYTES, getUsedFullFileBytes());
        builder.field(Fields.EVICTED_FULL_FILE_COUNT, getEvictedFullFileCount());
        builder.field(Fields.EVICTED_FULL_FILE_BYTES, getEvictedFullFileBytes());
        builder.field(Fields.HITS, getHitCount());
        builder.field(Fields.MISS, getMissCount());
        builder.endObject();
        return builder;
    }
}
