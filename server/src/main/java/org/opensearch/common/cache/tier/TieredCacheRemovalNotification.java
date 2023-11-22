/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.tier.enums.CacheStoreType;

/**
 * Notification when an element is removed from tiered cache.
 * @param <K> Type of key
 * @param <V> Type of value
 *
 * @opensearch.internal
 */
public class TieredCacheRemovalNotification<K, V> extends RemovalNotification<K, V> {
    private final CacheStoreType cacheStoreType;

    public TieredCacheRemovalNotification(K key, V value, RemovalReason removalReason) {
        super(key, value, removalReason);
        this.cacheStoreType = CacheStoreType.ON_HEAP;
    }

    public TieredCacheRemovalNotification(K key, V value, RemovalReason removalReason, CacheStoreType cacheStoreType) {
        super(key, value, removalReason);
        this.cacheStoreType = cacheStoreType;
    }

    public CacheStoreType getTierType() {
        return cacheStoreType;
    }
}
