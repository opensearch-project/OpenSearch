/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import org.opensearch.index.store.remote.utils.cache.stats.CacheStats;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Custom Cache which support typical cache operations (put, get, ...) and it support reference counting per individual key which might
 * change eviction behavior
 * @param <K> type of the key
 * @param <V> type of th value
 *
 * @opensearch.internal
 */
public interface RefCountedCache<K, V> {

    /**
     * Returns the value associated with {@code key} in this cache, or {@code null} if there is no
     * cached value for {@code key}.
     */
    V get(K key);

    /**
     * Associates {@code value} with {@code key} in this cache. If the cache previously contained a
     * value associated with {@code key}, the old value is replaced by {@code value}.
     */
    V put(K key, V value);

    /**
     * Copies all the mappings from the specified map to the cache. The effect of this call is
     * equivalent to that of calling {@code put(k, v)} on this map once for each mapping from key
     * {@code k} to value {@code v} in the specified map. The behavior of this operation is undefined
     * if the specified map is modified while the operation is in progress.
     */
    void putAll(Map<? extends K, ? extends V> m);

    /**
     * If the specified key is already associated with a value, attempts to update its value using the given mapping
     * function and enters the new value into this map unless null.
     *
     * If the specified key is NOT already associated with a value, return null without applying the mapping function.
     *
     * The remappingFunction method for a given key will be invoked at most once.
     */
    V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * Discards any cached value for key {@code key}.
     */
    void remove(K key);

    /**
     * Discards any cached values for keys {@code keys}.
     */
    void removeAll(Iterable<? extends K> keys);

    /**
     * Discards all entries in the cache.
     */
    void clear();

    /**
     * Returns the approximate number of entries in this cache.
     */
    long size();

    /**
     * increment references count for key {@code key}.
     */
    void incRef(K key);

    /**
     * decrement references count for key {@code key}.
     */
    void decRef(K key);

    long prune();

    /**
     * Returns the weighted usage of this cache.
     *
     * @return the combined weight of the values in this cache
     */
    CacheUsage usage();

    /**
     * Returns a current snapshot of this cache's cumulative statistics. All statistics are
     * initialized to zero, and are monotonically increasing over the lifetime of the cache.
     * <p>
     * Due to the performance penalty of maintaining statistics, some implementations may not record
     * the usage history immediately or at all.
     *
     * @return the current snapshot of the statistics of this cache
     */
    CacheStats stats();
}
