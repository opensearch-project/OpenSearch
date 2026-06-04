/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache.stats;

import org.opensearch.index.store.remote.utils.cache.RefCountedCache;

import java.util.function.BiFunction;

/**
 * Accumulates statistics during the operation of a {@link RefCountedCache} for presentation by
 * {@link RefCountedCache#stats}. This is solely intended for consumption by {@code Cache} implementors.
 *
 * @opensearch.internal
 */
public interface StatsCounter<K, V> {

    /**
     * Records cache hits. This should be called when a cache request returns a cached value.
     *
     * @param count the number of hits to record
     */
    void recordHits(K key, V value, boolean pinned, int count);

    /**
     * Records cache misses. This should be called when a cache request returns a value that was not
     * found in the cache. This method should be called by the loading thread, as well as by threads
     * blocking on the load. Multiple concurrent calls to {@link RefCountedCache} lookup methods with the same
     * key on an absent value should result in a single call to either {@code recordLoadSuccess} or
     * {@code recordLoadFailure} and multiple calls to this method, despite all being served by the
     * results of a single load operation.
     *
     * @param count  the number of misses to record
     */
    void recordMisses(K key, int count);

    /**
     * Records the explicit removal of an entry from the cache. This should only been called when an entry is
     * removed as a result of manual
     * {@link RefCountedCache#remove(Object)}
     * {@link RefCountedCache#compute(Object, BiFunction)}
     *
     * @param weight the weight of the removed entry
     */
    void recordRemoval(V value, boolean pinned, long weight);

    /**
     * Records the replacement of an entry from the cache. This should only been called when an entry is
     * replaced as a result of manual
     * {@link RefCountedCache#put(Object, Object)}
     * {@link RefCountedCache#compute(Object, BiFunction)}
     */
    void recordReplacement(V oldValue, V newValue, long oldWeight, long newWeight, boolean shouldUpdateActiveUsage, boolean isPinned);

    /**
     * Records the eviction of an entry from the cache. This should only been called when an entry is
     * evicted due to the cache's eviction strategy, and not as a result of manual
     * {@link RefCountedCache#remove(Object)}  removals}.
     *
     * @param weight the weight of the evicted entry
     */
    void recordEviction(V value, long weight);

    /**
     * Records the usage of the cache. This should be called when an entry is created/removed/replaced in the cache.
     *
     * @param value          Entry of the cache.
     * @param weight         Weight of the entry.
     * @param pinned
     * @param shouldDecrease Should the usage of the cache be decreased or not.
     */
    void recordUsage(V value, long weight, boolean pinned, boolean shouldDecrease);

    /**
     * Records the cache usage by entries which are active (being referenced).
     * This should be called when an active entry is created/removed/replaced in the cache.
     *
     * @param value          Entry of the cache.
     * @param weight         Weight of the entry.
     * @param pinned
     * @param shouldDecrease Should the active usage of the cache be decreased or not.
     */
    void recordActiveUsage(V value, long weight, boolean pinned, boolean shouldDecrease);

    /**
     * Records the cache usage by entries which are pinned.
     * This should be called when an entry is pinned/unpinned in the cache.
     * @param weight Weight of the entry.
     * @param shouldDecrease Should the pinned usage of the cache be decreased or not.
     */
    void recordPinnedUsage(V value, long weight, boolean shouldDecrease);

    /**
     * Resets the cache usage by entries which are active (being referenced).
     * This should be called when cache is cleared.
     */
    void resetActiveUsage();

    /**
     * Resets the cache usage by entries which are pinned.
     * This should be called when cache is cleared.
     */
    void resetPinnedUsage();

    /**
     * Resets the cache usage.
     * This should be called when cache is cleared.
     */
    void resetUsage();

    /**
     * Returns the active usage of the cache.
     * @return Active usage of the cache.
     */
    long activeUsage();

    /**
     * Returns the usage of the cache.
     * @return Usage of the cache.
     */
    long usage();

    /**
     * Returns the pinned usage of the cache.
     * @return Pinned usage of the cache.
     */
    long pinnedUsage();

    /**
     * Returns a snapshot of this counter's values. Note that this may be an inconsistent view, as it
     * may be interleaved with update operations.
     *
     * @return a snapshot of this counter's values
     */

    IRefCountedCacheStats snapshot();
}
