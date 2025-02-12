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
    private final long items;

    public ImmutableCacheStats(long hits, long misses, long evictions, long sizeInBytes, long items) {
        this.hits = hits;
        this.misses = misses;
        this.evictions = evictions;
        this.sizeInBytes = sizeInBytes;
        this.items = items;
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
            s1.items + s2.items
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

    public long getItems() {
        return items;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(hits);
        out.writeVLong(misses);
        out.writeVLong(evictions);
        out.writeVLong(sizeInBytes);
        out.writeVLong(items);
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
            && (items == other.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, misses, evictions, sizeInBytes, items);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // We don't write the header in CacheStatsResponse's toXContent, because it doesn't know the name of aggregation it's part of
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(sizeInBytes));
        builder.field(Fields.EVICTIONS, evictions);
        builder.field(Fields.HIT_COUNT, hits);
        builder.field(Fields.MISS_COUNT, misses);
        builder.field(Fields.ITEM_COUNT, items);
        return builder;
    }

    @Override
    public String toString() {
        return Fields.HIT_COUNT
            + "="
            + hits
            + ", "
            + Fields.MISS_COUNT
            + "="
            + misses
            + ", "
            + Fields.EVICTIONS
            + "="
            + evictions
            + ", "
            + Fields.SIZE_IN_BYTES
            + "="
            + sizeInBytes
            + ", "
            + Fields.ITEM_COUNT
            + "="
            + items;
    }

    /**
     * Field names used to write the values in this object to XContent.
     */
    public static final class Fields {
        public static final String SIZE = "size";
        public static final String SIZE_IN_BYTES = "size_in_bytes";
        public static final String EVICTIONS = "evictions";
        public static final String HIT_COUNT = "hit_count";
        public static final String MISS_COUNT = "miss_count";
        public static final String ITEM_COUNT = "item_count";
    }
}
