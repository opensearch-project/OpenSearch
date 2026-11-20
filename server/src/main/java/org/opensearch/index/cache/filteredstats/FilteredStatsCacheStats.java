/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Size and activity of a node's {@link IndicesFilteredStatsCache}, reported under {@code _nodes/stats/filtered_stats_cache}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FilteredStatsCacheStats implements Writeable, ToXContentFragment {

    private final long memorySizeInBytes;
    private final long entries;
    private final long hitCount;
    private final long missCount;
    private final long evictions;
    private final long refusals;

    /**
     * Creates the stats.
     *
     * @param memorySizeInBytes bytes currently charged for cached statistics
     * @param entries cached (segment, filter) entries
     * @param hitCount lookups served from the cache
     * @param missCount lookups that computed their value
     * @param evictions entries evicted to stay within the size bound
     * @param refusals values computed but not cached, because they were too large or the breaker refused them
     */
    public FilteredStatsCacheStats(long memorySizeInBytes, long entries, long hitCount, long missCount, long evictions, long refusals) {
        this.memorySizeInBytes = memorySizeInBytes;
        this.entries = entries;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.evictions = evictions;
        this.refusals = refusals;
    }

    /**
     * Reads the stats from a stream.
     *
     * @param in the stream
     */
    public FilteredStatsCacheStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySizeInBytes);
        out.writeVLong(entries);
        out.writeVLong(hitCount);
        out.writeVLong(missCount);
        out.writeVLong(evictions);
        out.writeVLong(refusals);
    }

    /** Returns the bytes currently charged for cached statistics. */
    public long getMemorySizeInBytes() {
        return memorySizeInBytes;
    }

    /** Returns the number of cached (segment, filter) entries. */
    public long getEntries() {
        return entries;
    }

    /** Returns the number of lookups served from the cache. */
    public long getHitCount() {
        return hitCount;
    }

    /** Returns the number of lookups that computed their value. */
    public long getMissCount() {
        return missCount;
    }

    /** Returns the number of entries evicted to stay within the size bound. */
    public long getEvictions() {
        return evictions;
    }

    /** Returns the number of values computed but not cached. */
    public long getRefusals() {
        return refusals;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FILTERED_STATS_CACHE);
        builder.humanReadableField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, new ByteSizeValue(memorySizeInBytes));
        builder.field(Fields.ENTRIES, entries);
        builder.field(Fields.HIT_COUNT, hitCount);
        builder.field(Fields.MISS_COUNT, missCount);
        builder.field(Fields.EVICTIONS, evictions);
        builder.field(Fields.REFUSALS, refusals);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FilteredStatsCacheStats other)) return false;
        return memorySizeInBytes == other.memorySizeInBytes
            && entries == other.entries
            && hitCount == other.hitCount
            && missCount == other.missCount
            && evictions == other.evictions
            && refusals == other.refusals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySizeInBytes, entries, hitCount, missCount, evictions, refusals);
    }

    static final class Fields {
        static final String FILTERED_STATS_CACHE = "filtered_stats_cache";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String ENTRIES = "entries";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String EVICTIONS = "evictions";
        static final String REFUSALS = "refusals";
    }
}
