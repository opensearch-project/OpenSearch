/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;
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
    V get(K key);

    void put(K key, V value);

    V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception;

    void invalidate(K key);

    void invalidateAll();

    Iterable<K> keys();

    long count();

    void refresh();

    /**
     * Factory to create objects.
     */
    @ExperimentalApi
    interface Factory {
        <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories);

        String getCacheName();
    }
}
