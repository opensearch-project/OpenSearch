/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * An immutable snapshot of CacheStats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ImmutableCacheStats implements Writeable, ToXContent {
    private final long hits;
    private final long misses;
    private final long evictions;
    private final long sizeInBytes;
    private final long entries;

    public ImmutableCacheStats(long hits, long misses, long evictions, long sizeInBytes, long entries) {
        this.hits = hits;
        this.misses = misses;
        this.evictions = evictions;
        this.sizeInBytes = sizeInBytes;
        this.entries = entries;
    }

    public ImmutableCacheStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    public static ImmutableCacheStats addSnapshots(ImmutableCacheStats s1, ImmutableCacheStats s2) {
        return new ImmutableCacheStats(
            s1.hits + s2.hits,
            s1.misses + s2.misses,
            s1.evictions + s2.evictions,
            s1.sizeInBytes + s2.sizeInBytes,
            s1.entries + s2.entries
        );
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }

    public long getEvictions() {
        return evictions;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getEntries() {
        return entries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(hits);
        out.writeVLong(misses);
        out.writeVLong(evictions);
        out.writeVLong(sizeInBytes);
        out.writeVLong(entries);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != ImmutableCacheStats.class) {
            return false;
        }
        ImmutableCacheStats other = (ImmutableCacheStats) o;
        return (hits == other.hits)
            && (misses == other.misses)
            && (evictions == other.evictions)
            && (sizeInBytes == other.sizeInBytes)
            && (entries == other.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, misses, evictions, sizeInBytes, entries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // We don't write the header in CacheStatsResponse's toXContent, because it doesn't know the name of aggregation it's part of
        builder.humanReadableField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, new ByteSizeValue(sizeInBytes));
        builder.field(Fields.EVICTIONS, evictions);
        builder.field(Fields.HIT_COUNT, hits);
        builder.field(Fields.MISS_COUNT, misses);
        builder.field(Fields.ENTRIES, entries);
        return builder;
    }

    public static final class Fields {
        public static final String MEMORY_SIZE = "size";
        public static final String MEMORY_SIZE_IN_BYTES = "size_in_bytes";
        // TODO: This might not be memory as it could be partially on disk, so I've changed it, but should it be consistent with the earlier
        // field?
        public static final String EVICTIONS = "evictions";
        public static final String HIT_COUNT = "hit_count";
        public static final String MISS_COUNT = "miss_count";
        public static final String ENTRIES = "entries";
    }
}
