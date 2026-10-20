/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Node-level, byte-bounded LRU store behind every index's {@link FilteredStatsCache}, holding one
 * {@link FilteredSegmentStats} per (segment, filter).
 * <p>
 * Values added to an entry are charged to the field data circuit breaker as they are added, and the entry is re-weighed
 * in the LRU as it grows, so the bound holds to within a quarter of each entry's size. A value the breaker refuses, or
 * that would take an entry past half the cache, is returned uncached, so either costs recomputation, never a failed
 * request.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class IndicesFilteredStatsCache implements Closeable {

    /** Maximum memory for cached visible-subset statistics on this node; zero turns caching off. */
    public static final Setting<ByteSizeValue> INDICES_FILTERED_STATS_CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "indices.filtered_stats.cache.size",
        "5%",
        Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(IndicesFilteredStatsCache.class);
    private static final long REFUSAL_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    // The entry object, its two maps, its cache entry and LRU links, and its node in keysByReader.
    private static final long ENTRY_OVERHEAD_BYTES = 512;
    private static final long KEY_SHALLOW_BYTES = RamUsageEstimator.shallowSizeOfInstance(Key.class);
    // An entry is re-weighed in the LRU once it has grown by a quarter, or by this much when it is small.
    private static final long REWEIGH_SLACK_BYTES = 4096;

    private final Cache<Key, Value> cache;
    private final CircuitBreaker breaker;
    private final boolean enabled;
    // A larger entry would evict most of the cache, and then itself.
    private final long maxEntryBytes;
    private final AtomicLong bytes = new AtomicLong();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder refusals = new LongAdder();
    private final AtomicLong lastRefusalLogNanos = new AtomicLong(System.nanoTime() - REFUSAL_LOG_INTERVAL_NANOS);
    // Keys cached per segment, so a closing segment invalidates exactly its own entries. A key lost to a race between
    // removal and reload is reclaimed by eviction instead.
    private final ConcurrentMap<IndexReader.CacheKey, Set<Key>> keysByReader = ConcurrentCollections.newConcurrentMap();

    /**
     * Creates the node-level cache.
     *
     * @param settings node settings carrying {@link #INDICES_FILTERED_STATS_CACHE_SIZE_SETTING}
     * @param breaker breaker that cached bytes are charged to
     */
    public IndicesFilteredStatsCache(Settings settings, CircuitBreaker breaker) {
        this.breaker = Objects.requireNonNull(breaker);
        final long sizeInBytes = INDICES_FILTERED_STATS_CACHE_SIZE_SETTING.get(settings).getBytes();
        this.enabled = sizeInBytes != 0;
        this.maxEntryBytes = sizeInBytes > 0 ? sizeInBytes / 2 : Long.MAX_VALUE;
        final CacheBuilder<Key, Value> builder = CacheBuilder.<Key, Value>builder()
            .removalListener(this::onRemoval)
            .weigher((key, value) -> value.weight);
        if (sizeInBytes > 0) {
            builder.setMaximumWeight(sizeInBytes);
        }
        this.cache = builder.build();
    }

    /**
     * Returns the statistics of a segment under a filter, building its visible bitset on a miss; concurrent misses
     * build it once.
     */
    FilteredSegmentStats segment(
        FilteredStatsCache owner,
        LeafReaderContext ctx,
        Query filter,
        FilteredStatsCache.Computation<BitSet> bitSet
    ) throws IOException {
        final IndexReader.CacheHelper cacheHelper = ctx.reader().getCoreCacheHelper();
        if (enabled == false || cacheHelper == null) {
            return FilteredSegmentStats.uncached(bitSet.compute());
        }
        final IndexReader.CacheKey readerKey = cacheHelper.getKey();
        final Key key = new Key(owner, readerKey, filter);
        final Value hit = cache.get(key);
        if (hit != null) {
            hits.increment();
            return hit.entry;
        }
        misses.increment();
        // A build another request started can fail for that request's own reasons, such as its cancellation or timeout.
        // Retry once as the builder, then build uncached, rather than fail with someone else's error.
        for (int attempt = 0; attempt < 2; attempt++) {
            final boolean[] builtHere = new boolean[1];
            try {
                return cache.computeIfAbsent(key, k -> {
                    builtHere[0] = true;
                    return build(owner, k, cacheHelper, bitSet);
                }).entry;
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof NotCached notCached) {
                    return notCached.entry;
                }
                if (builtHere[0]) {
                    throw rethrow(cause instanceof LoadError loadError ? loadError.error : cause);
                }
            }
        }
        return FilteredSegmentStats.uncached(bitSet.compute());
    }

    private Value build(
        FilteredStatsCache owner,
        Key key,
        IndexReader.CacheHelper cacheHelper,
        FilteredStatsCache.Computation<BitSet> bitSet
    ) throws IOException {
        final BitSet visibleDocs;
        try {
            visibleDocs = bitSet.compute();
        } catch (Error error) {
            // Cache completes a pending load only for an Exception; an Error would leave later callers of this key,
            // and the segment close that invalidates it, waiting forever.
            throw new LoadError(error);
        }
        final long weight = ENTRY_OVERHEAD_BYTES + key.ramBytesUsed() + (visibleDocs == null ? 0 : visibleDocs.ramBytesUsed());
        if (weight > maxEntryBytes || reserve(weight) == false) {
            onRefused(weight);
            throw new NotCached(FilteredSegmentStats.uncached(visibleDocs));
        }
        final FilteredSegmentStats entry = new FilteredSegmentStats(visibleDocs, this, owner);
        entry.key = key;
        entry.add(weight);
        entry.weighed = weight;
        account(owner, weight);
        // The caller holds the segment open, so onClose cannot run before the key is recorded.
        keysByReader.computeIfAbsent(key.readerKey, r -> {
            cacheHelper.addClosedListener(this::onClose);
            return ConcurrentCollections.newConcurrentSet();
        }).add(key);
        return new Value(entry, weight);
    }

    /** Charges a value about to be kept in an entry; false means it must not be kept. */
    boolean charge(FilteredSegmentStats entry, long weight) {
        if (entry.bytes() + weight > maxEntryBytes || reserve(weight) == false) {
            onRefused(weight);
            return false;
        }
        final long total = entry.add(weight);
        if (total < 0) {
            breaker.addWithoutBreaking(-weight);
            return false;
        }
        account(entry.owner, weight);
        if (total > entry.weighed + Math.max(entry.weighed / 4, REWEIGH_SLACK_BYTES) && entry.isRemoved() == false) {
            entry.weighed = total;
            // Outside the entry's lock: put can evict, and eviction takes other entries' locks.
            cache.put(entry.key, new Value(entry, total));
        }
        return true;
    }

    /** Returns a charge for a value that was not kept after all. */
    void release(FilteredSegmentStats entry, long weight) {
        if (entry.add(-weight) >= 0) {
            breaker.addWithoutBreaking(-weight);
            account(entry.owner, -weight);
        }
    }

    void onHit() {
        hits.increment();
    }

    void onMiss() {
        misses.increment();
    }

    private void account(FilteredStatsCache owner, long delta) {
        bytes.addAndGet(delta);
        owner.addBytes(delta);
    }

    private boolean reserve(long weight) {
        try {
            breaker.addEstimateBytesAndMaybeBreak(weight, "filtered_stats");
            return true;
        } catch (CircuitBreakingException e) {
            return false;
        }
    }

    // Computations throw only IOException or unchecked throwables.
    private static RuntimeException rethrow(Throwable cause) throws IOException {
        if (cause instanceof IOException io) throw io;
        if (cause instanceof RuntimeException re) throw re;
        if (cause instanceof Error error) throw error;
        throw new IllegalStateException(cause);
    }

    private void onRefused(long weight) {
        refusals.increment();
        final long now = System.nanoTime();
        final long last = lastRefusalLogNanos.get();
        if (now - last >= REFUSAL_LOG_INTERVAL_NANOS && lastRefusalLogNanos.compareAndSet(last, now)) {
            logger.warn(
                "not caching [{}] bytes of filtered statistics: over half of [{}] or refused by the [{}] breaker; "
                    + "[{}] values refused so far, each recomputed per request",
                weight,
                INDICES_FILTERED_STATS_CACHE_SIZE_SETTING.getKey(),
                breaker.getName(),
                refusals.sum()
            );
        }
    }

    private void onRemoval(RemovalNotification<Key, Value> notification) {
        if (notification.getRemovalReason() == RemovalReason.REPLACED) {
            // Re-weighed: the same entry stays in the cache.
            return;
        }
        if (notification.getRemovalReason() == RemovalReason.EVICTED) {
            evictions.increment();
        }
        final Key key = notification.getKey();
        final long charged = notification.getValue().entry.remove();
        breaker.addWithoutBreaking(-charged);
        account(key.owner, -charged);
        final Set<Key> keys = keysByReader.get(key.readerKey);
        if (keys != null) {
            keys.remove(key);
        }
    }

    private void onClose(IndexReader.CacheKey readerKey) {
        final Set<Key> keys = keysByReader.remove(readerKey);
        if (keys != null) {
            for (Key key : keys) {
                cache.invalidate(key);
            }
        }
    }

    /** Invalidates every entry cached for one index. */
    void clear(FilteredStatsCache owner) {
        // A snapshot, because other indices keep using the cache while this one is cleared.
        for (Key key : cache.keysSnapshot()) {
            if (key.owner == owner) {
                cache.invalidate(key);
            }
        }
    }

    /** Returns a point-in-time view of this cache's size and activity. */
    public FilteredStatsCacheStats stats() {
        return new FilteredStatsCacheStats(bytes.get(), cache.count(), hits.sum(), misses.sum(), evictions.sum(), refusals.sum());
    }

    long weight() {
        return cache.weight();
    }

    int count() {
        return cache.count();
    }

    long refusals() {
        return refusals.sum();
    }

    long evictions() {
        return evictions.sum();
    }

    int trackedKeys() {
        return keysByReader.values().stream().mapToInt(Set::size).sum();
    }

    @Override
    public void close() {
        cache.invalidateAll();
        keysByReader.clear();
    }

    /** Carries an Error out of a load as an Exception, so the pending load completes. */
    private static final class LoadError extends RuntimeException {
        private final transient Error error;

        LoadError(Error error) {
            super(null, null, false, false);
            this.error = error;
        }
    }

    /** Carries statistics that were not cached back to the caller. */
    private static final class NotCached extends RuntimeException {
        private final transient FilteredSegmentStats entry;

        NotCached(FilteredSegmentStats entry) {
            super(null, null, false, false);
            this.entry = entry;
        }
    }

    /**
     * Segment core and filter. The owning index takes no part in equality, because a segment belongs to one index.
     */
    static final class Key {
        final FilteredStatsCache owner;
        final IndexReader.CacheKey readerKey;
        final Query filter;
        private final int hash;

        Key(FilteredStatsCache owner, IndexReader.CacheKey readerKey, Query filter) {
            this.owner = owner;
            this.readerKey = Objects.requireNonNull(readerKey);
            this.filter = Objects.requireNonNull(filter);
            this.hash = 31 * System.identityHashCode(readerKey) + filter.hashCode();
        }

        // The filter is shared with the request that built it and is not counted.
        long ramBytesUsed() {
            return KEY_SHALLOW_BYTES;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key other)) return false;
            return readerKey == other.readerKey && hash == other.hash && filter.equals(other.filter);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /** An entry and the weight the LRU last saw for it. */
    static final class Value {
        final FilteredSegmentStats entry;
        final long weight;

        Value(FilteredSegmentStats entry, long weight) {
            this.entry = entry;
            this.weight = weight;
        }
    }
}
