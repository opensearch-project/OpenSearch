/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

public interface TieredCacheHandler<K, V> {

    V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception;

    V get(K key);

    void invalidate(K key);

    void invalidateAll();

    long count();

    CachingTier<K, V> getOnHeapCachingTier();
}
