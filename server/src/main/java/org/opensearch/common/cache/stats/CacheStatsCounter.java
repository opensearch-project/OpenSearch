/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;

import java.util.Objects;

/**
 * A class containing the 5 live metrics tracked by a StatsHolder object. Mutable.
 */
public class CacheStatsCounter {
    public CounterMetric hits;
    public CounterMetric misses;
    public CounterMetric evictions;
    public CounterMetric sizeInBytes;
    public CounterMetric entries;

    public CacheStatsCounter(long hits, long misses, long evictions, long sizeInBytes, long entries) {
        this.hits = new CounterMetric();
        this.hits.inc(hits);
        this.misses = new CounterMetric();
        this.misses.inc(misses);
        this.evictions = new CounterMetric();
        this.evictions.inc(evictions);
        this.sizeInBytes = new CounterMetric();
        this.sizeInBytes.inc(sizeInBytes);
        this.entries = new CounterMetric();
        this.entries.inc(entries);
    }

    public CacheStatsCounter() {
        this(0, 0, 0, 0, 0);
    }

    private synchronized void internalAdd(long otherHits, long otherMisses, long otherEvictions, long otherSizeInBytes, long otherEntries) {
        this.hits.inc(otherHits);
        this.misses.inc(otherMisses);
        this.evictions.inc(otherEvictions);
        this.sizeInBytes.inc(otherSizeInBytes);
        this.entries.inc(otherEntries);
    }

    public void add(CacheStatsCounter other) {
        if (other == null) {
            return;
        }
        internalAdd(other.getHits(), other.getMisses(), other.getEvictions(), other.getSizeInBytes(), other.getEntries());
    }

    public void add(CounterSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        internalAdd(snapshot.getHits(), snapshot.getMisses(), snapshot.getEvictions(), snapshot.getSizeInBytes(), snapshot.getEntries());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != CacheStatsCounter.class) {
            return false;
        }
        CacheStatsCounter other = (CacheStatsCounter) o;
        return (hits.count() == other.hits.count())
            && (misses.count() == other.misses.count())
            && (evictions.count() == other.evictions.count())
            && (sizeInBytes.count() == other.sizeInBytes.count())
            && (entries.count() == other.entries.count());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits.count(), misses.count(), evictions.count(), sizeInBytes.count(), entries.count());
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

    public long getSizeInBytes() {
        return sizeInBytes.count();
    }

    public long getEntries() {
        return entries.count();
    }

    public CounterSnapshot snapshot() {
        return new CounterSnapshot(hits.count(), misses.count(), evictions.count(), sizeInBytes.count(), entries.count());
    }

}
