/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import java.util.Optional;

/**
 * This service encapsulates all logic to write/fetch to/from appropriate tiers. Can be implemented with different
 * flavors like spillover etc.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface TieredCacheService<K, V> {

    V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception;

    V get(K key);

    void invalidate(K key);

    void invalidateAll();

    long count();

    OnHeapCachingTier<K, V> getOnHeapCachingTier();

    Optional<DiskCachingTier<K, V>> getDiskCachingTier();
}
