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

import java.io.IOException;
import java.util.Objects;

/**
 * An immutable snapshot of CacheStats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ImmutableCacheStats implements Writeable { // TODO: Make this extend ToXContent (in API PR)
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
}
