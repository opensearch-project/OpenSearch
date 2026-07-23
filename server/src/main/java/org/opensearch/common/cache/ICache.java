/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.store.config.CacheConfig;

import java.io.Closeable;
import java.util.Map;

/**
 * Represents a cache interface.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ICache<K, V> extends Closeable {
    V get(ICacheKey<K> key);

    void put(ICacheKey<K> key, V value);

    V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception;

    /**
     * Invalidates the key. If a dimension in the key has dropStatsOnInvalidation set to true, the cache also completely
     * resets stats for that dimension value. It's the caller's responsibility to make sure all keys with that dimension value are
     * actually invalidated.
     */
    void invalidate(ICacheKey<K> key);

    void invalidateAll();

    /**
     * Returns a view of the keys in the cache that is safe to iterate under concurrent mutation. The view is weakly
     * consistent: concurrent insertions and removals may or may not be reflected. Iteration must never skip a key
     * that was present in the cache for the entire duration of the iteration. The returned iterator is not required
     * to support {@link java.util.Iterator#remove()}; use {@link #invalidate(ICacheKey)} to remove entries.
     */
    Iterable<ICacheKey<K>> keys();

    long count();

    void refresh();

    // Return total stats only
    default ImmutableCacheStatsHolder stats() {
        return stats(null);
    }

    // Return stats aggregated by the provided levels. If levels is null or an empty array, return total stats only.
    ImmutableCacheStatsHolder stats(String[] levels);

    /**
     * Factory to create objects.
     */
    @ExperimentalApi
    interface Factory {
        <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories);

        String getCacheName();
    }
}
