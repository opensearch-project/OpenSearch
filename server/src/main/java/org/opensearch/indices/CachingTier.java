/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.cache.RemovalListener;

/**
 * asdsadssa
 * @param <K>
 * @param <V>
 */
public interface CachingTier<K, V> {

    V get(K key);

    void put(K key, V value);

    V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception;

    void invalidate(K key);

    V compute(K key, TieredCacheLoader<K, V> loader) throws Exception;

    void setRemovalListener(RemovalListener<K, V> removalListener);

    void invalidateAll();

    Iterable<K> keys();

    int count();

    TierType getTierType();
}
