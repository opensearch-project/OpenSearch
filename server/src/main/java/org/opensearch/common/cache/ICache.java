/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

/**
 * Represents a cache interface.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
public interface ICache<K, V> {
    V get(K key);

    void put(K key, V value);

    V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception;

    void invalidate(K key);

    void invalidateAll();

    Iterable<K> keys();

    long count();

    void refresh();
}
