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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
import java.util.function.ToLongFunction;

/**
 * Node-level, byte-bounded LRU store behind every index's {@link FilteredStatsCache}.
 * <p>
 * Cached bytes are charged to the field data circuit breaker. An entry the breaker refuses, or one larger than half the
 * cache, is returned to the caller uncached, so either costs recomputation, never a failed request.
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
    // The cache entry, its LRU links, its segment map node and its node in keysByReader.
    private static final long ENTRY_OVERHEAD_BYTES = 128;
    private static final long KEY_SHALLOW_BYTES = RamUsageEstimator.shallowSizeOfInstance(Key.class);

    private final Cache<Key, Value> cache;
    private final CircuitBreaker breaker;
    private final boolean enabled;
    // A larger entry would evict most of the cache, and then itself.
    private final long maxEntryBytes;
    private final AtomicLong refusals = new AtomicLong();
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

    /** Returns the cached value for a key, computing it on a miss; concurrent misses on one key compute it once. */
    <T> T getOrCompute(
        FilteredStatsCache owner,
        LeafReaderContext ctx,
        Query filter,
        String field,
        Term term,
        FilteredStatsCache.Computation<T> computation,
        ToLongFunction<T> weigher
    ) throws IOException {
        final IndexReader.CacheHelper cacheHelper = ctx.reader().getCoreCacheHelper();
        if (enabled == false || cacheHelper == null) {
            return computation.compute();
        }
        final IndexReader.CacheKey readerKey = cacheHelper.getKey();
        final Value hit = cache.get(new Key(owner, readerKey, filter, field, term));
        if (hit != null) {
            return cast(hit.payload);
        }
        // Copied only on a miss, so a cached key does not hold the caller's term buffer.
        final Key key = new Key(
            owner,
            readerKey,
            filter,
            field,
            term == null ? null : new Term(term.field(), BytesRef.deepCopyOf(term.bytes()))
        );
        // A load another request started can fail for that request's own reasons, such as its cancellation or timeout.
        // Retry once as the loader, then compute uncached, rather than fail with someone else's error.
        for (int attempt = 0; attempt < 2; attempt++) {
            final boolean[] loadedHere = new boolean[1];
            try {
                return cast(cache.computeIfAbsent(key, k -> {
                    loadedHere[0] = true;
                    return load(owner, k, readerKey, cacheHelper, computation, weigher);
                }).payload);
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof NotCached notCached) {
                    return cast(notCached.payload);
                }
                if (loadedHere[0]) {
                    throw rethrow(cause instanceof LoadError loadError ? loadError.error : cause);
                }
            }
        }
        return computation.compute();
    }

    private <T> Value load(
        FilteredStatsCache owner,
        Key key,
        IndexReader.CacheKey readerKey,
        IndexReader.CacheHelper cacheHelper,
        FilteredStatsCache.Computation<T> computation,
        ToLongFunction<T> weigher
    ) throws IOException {
        final T payload;
        try {
            payload = computation.compute();
        } catch (Error error) {
            // Cache completes a pending load only for an Exception; an Error would leave later callers of this key,
            // and the segment close that invalidates it, waiting forever.
            throw new LoadError(error);
        }
        final long weight = ENTRY_OVERHEAD_BYTES + key.ramBytesUsed() + weigher.applyAsLong(payload);
        if (weight > maxEntryBytes || reserve(weight) == false) {
            onRefused(weight);
            throw new NotCached(payload);
        }
        owner.addBytes(weight);
        // The caller holds the segment open, so onClose cannot run before the key is recorded.
        keysByReader.computeIfAbsent(readerKey, r -> {
            cacheHelper.addClosedListener(this::onClose);
            return ConcurrentCollections.newConcurrentSet();
        }).add(key);
        return new Value(payload, weight);
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

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object payload) {
        return (T) payload;
    }

    private void onRefused(long weight) {
        final long count = refusals.incrementAndGet();
        final long now = System.nanoTime();
        final long last = lastRefusalLogNanos.get();
        if (now - last >= REFUSAL_LOG_INTERVAL_NANOS && lastRefusalLogNanos.compareAndSet(last, now)) {
            logger.warn(
                "not caching a filtered statistics entry of [{}] bytes: larger than half of [{}] or refused by the [{}] breaker; "
                    + "[{}] entries refused so far, each recomputed per request",
                weight,
                INDICES_FILTERED_STATS_CACHE_SIZE_SETTING.getKey(),
                breaker.getName(),
                count
            );
        }
    }

    private void onRemoval(RemovalNotification<Key, Value> notification) {
        final Key key = notification.getKey();
        final Value value = notification.getValue();
        if (value != null) {
            breaker.addWithoutBreaking(-value.weight);
            key.owner.addBytes(-value.weight);
        }
        if (notification.getRemovalReason() != RemovalReason.REPLACED) {
            final Set<Key> keys = keysByReader.get(key.readerKey);
            if (keys != null) {
                keys.remove(key);
            }
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

    long weight() {
        return cache.weight();
    }

    int count() {
        return cache.count();
    }

    long refusals() {
        return refusals.get();
    }

    long evictions() {
        return cache.stats().getEvictions();
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

    /** Carries a computed value that was not cached back to the caller. */
    private static final class NotCached extends RuntimeException {
        private final transient Object payload;

        NotCached(Object payload) {
            super(null, null, false, false);
            this.payload = payload;
        }
    }

    /**
     * Segment core, filter, and the field or term an entry covers; both are null for the visible bitset. The owning
     * index takes no part in equality, because a segment belongs to one index.
     */
    static final class Key {
        final FilteredStatsCache owner;
        final IndexReader.CacheKey readerKey;
        final Query filter;
        final String field;
        final Term term;
        private final int hash;

        Key(FilteredStatsCache owner, IndexReader.CacheKey readerKey, Query filter, String field, Term term) {
            this.owner = owner;
            this.readerKey = Objects.requireNonNull(readerKey);
            this.filter = Objects.requireNonNull(filter);
            this.field = field;
            this.term = term;
            int h = System.identityHashCode(readerKey);
            h = 31 * h + filter.hashCode();
            h = 31 * h + Objects.hashCode(field);
            this.hash = 31 * h + Objects.hashCode(term);
        }

        // The filter is shared with the request that built it and is not counted.
        long ramBytesUsed() {
            return KEY_SHALLOW_BYTES + (field == null ? 0 : RamUsageEstimator.sizeOf(field)) + (term == null ? 0 : term.ramBytesUsed());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key other)) return false;
            return readerKey == other.readerKey
                && hash == other.hash
                && Objects.equals(field, other.field)
                && Objects.equals(term, other.term)
                && filter.equals(other.filter);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /** A cached value and the bytes charged for it, key included. */
    static final class Value {
        final Object payload;
        final long weight;

        Value(Object payload, long weight) {
            this.payload = payload;
            this.weight = weight;
        }
    }
}
