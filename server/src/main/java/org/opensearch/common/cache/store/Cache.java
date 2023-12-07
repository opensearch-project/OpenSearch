/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.LoadAwareCacheLoader;

/**
 * Represents a cache interface.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public interface Cache<K, V> {
    V get(K key);

    void put(K key, V value);

    V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception;

    void invalidate(K key);

    V compute(K key, LoadAwareCacheLoader<K, V> loader) throws Exception;

    void invalidateAll();

    Iterable<K> keys();

    long count();

    void refresh();
}
