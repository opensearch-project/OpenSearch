/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * A class containing the 5 metrics tracked by a CacheStats object.
 */
public class CacheStatsResponse implements Writeable { // TODO: Make this extend ToXContent.
    public CounterMetric hits;
    public CounterMetric misses;
    public CounterMetric evictions;
    public CounterMetric memorySize;
    public CounterMetric entries;

    public CacheStatsResponse(long hits, long misses, long evictions, long memorySize, long entries) {
        this.hits = new CounterMetric();
        this.hits.inc(hits);
        this.misses = new CounterMetric();
        this.misses.inc(misses);
        this.evictions = new CounterMetric();
        this.evictions.inc(evictions);
        this.memorySize = new CounterMetric();
        this.memorySize.inc(memorySize);
        this.entries = new CounterMetric();
        this.entries.inc(entries);
    }

    public CacheStatsResponse(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    public CacheStatsResponse() {
        this(0, 0, 0, 0, 0);
    }

    public synchronized void add(CacheStatsResponse other) {
        if (other == null) {
            return;
        }
        this.hits.inc(other.hits.count());
        this.misses.inc(other.misses.count());
        this.evictions.inc(other.evictions.count());
        this.memorySize.inc(other.memorySize.count());
        this.entries.inc(other.entries.count());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != CacheStatsResponse.class) {
            return false;
        }
        CacheStatsResponse other = (CacheStatsResponse) o;
        return (hits.count() == other.hits.count())
            && (misses.count() == other.misses.count())
            && (evictions.count() == other.evictions.count())
            && (memorySize.count() == other.memorySize.count())
            && (entries.count() == other.entries.count());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits.count(), misses.count(), evictions.count(), memorySize.count(), entries.count());
    }

    public long getHits() {
        return hits.count();
    }

    public long getMisses() {
        return misses.count();
    }

    public long getEvictions() {
        return evictions.count();
    }

    public long getMemorySize() {
        return memorySize.count();
    }

    public long getEntries() {
        return entries.count();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(hits.count());
        out.writeVLong(misses.count());
        out.writeVLong(evictions.count());
        out.writeVLong(memorySize.count());
        out.writeVLong(entries.count());
    }
}
