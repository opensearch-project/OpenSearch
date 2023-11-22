/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier.listeners;

import org.opensearch.common.cache.tier.enums.CacheStoreType;
import org.opensearch.common.cache.tier.TieredCacheRemovalNotification;

/**
 * This can be used to listen to tiered caching events
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface TieredCacheEventListener<K, V> {

    void onMiss(K key, CacheStoreType cacheStoreType);

    void onRemoval(TieredCacheRemovalNotification<K, V> notification);

    void onHit(K key, V value, CacheStoreType cacheStoreType);

    void onCached(K key, V value, CacheStoreType cacheStoreType);
}
