/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.stats.CacheStats;
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
     * Invalidates the key. If key.dropStatsForDimensions is true, the cache also resets stats for the combination
     * of dimensions this key holds. It's the caller's responsibility to make sure all keys with that combination are
     * actually invalidated.
     */
    void invalidate(ICacheKey<K> key);

    void invalidateAll();

    Iterable<ICacheKey<K>> keys();

    long count();

    void refresh();

    CacheStats stats();

    /**
     * Factory to create objects.
     */
    @ExperimentalApi
    interface Factory {
        <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories);

        String getCacheName();
    }
}
