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
 * A mutable class containing the 5 live metrics tracked by a StatsHolder object.
 */
public class CacheStats {
    CounterMetric hits;
    CounterMetric misses;
    CounterMetric evictions;
    CounterMetric sizeInBytes;
    CounterMetric items;

    public CacheStats(long hits, long misses, long evictions, long sizeInBytes, long items) {
        this.hits = new CounterMetric();
        this.hits.inc(hits);
        this.misses = new CounterMetric();
        this.misses.inc(misses);
        this.evictions = new CounterMetric();
        this.evictions.inc(evictions);
        this.sizeInBytes = new CounterMetric();
        this.sizeInBytes.inc(sizeInBytes);
        this.items = new CounterMetric();
        this.items.inc(items);
    }

    public CacheStats() {
        this(0, 0, 0, 0, 0);
    }

    private void internalAdd(long otherHits, long otherMisses, long otherEvictions, long otherSizeInBytes, long otherEntries) {
        this.hits.inc(otherHits);
        this.misses.inc(otherMisses);
        this.evictions.inc(otherEvictions);
        this.sizeInBytes.inc(otherSizeInBytes);
        this.items.inc(otherEntries);
    }

    public void add(CacheStats other) {
        if (other == null) {
            return;
        }
        internalAdd(other.getHits(), other.getMisses(), other.getEvictions(), other.getSizeInBytes(), other.getItems());
    }

    public void add(ImmutableCacheStats snapshot) {
        if (snapshot == null) {
            return;
        }
        internalAdd(snapshot.getHits(), snapshot.getMisses(), snapshot.getEvictions(), snapshot.getSizeInBytes(), snapshot.getItems());
    }

    public void subtract(ImmutableCacheStats other) {
        if (other == null) {
            return;
        }
        internalAdd(-other.getHits(), -other.getMisses(), -other.getEvictions(), -other.getSizeInBytes(), -other.getItems());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits.count(), misses.count(), evictions.count(), sizeInBytes.count(), items.count());
    }

    public void incrementHits() {
        hits.inc();
    }

    public void incrementMisses() {
        misses.inc();
    }

    public void incrementEvictions() {
        evictions.inc();
    }

    public void incrementSizeInBytes(long amount) {
        sizeInBytes.inc(amount);
    }

    public void decrementSizeInBytes(long amount) {
        sizeInBytes.dec(amount);
    }

    public void incrementItems() {
        items.inc();
    }

    public void decrementItems() {
        items.dec();
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

    public long getItems() {
        return items.count();
    }

    public void resetSizeAndEntries() {
        sizeInBytes = new CounterMetric();
        items = new CounterMetric();
    }

    public ImmutableCacheStats immutableSnapshot() {
        return new ImmutableCacheStats(hits.count(), misses.count(), evictions.count(), sizeInBytes.count(), items.count());
    }
}
