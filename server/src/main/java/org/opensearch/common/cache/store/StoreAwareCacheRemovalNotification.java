/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.store.enums.CacheStoreType;

/**
 * Removal notification for store aware cache.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.internal
 */
public class StoreAwareCacheRemovalNotification<K, V> extends RemovalNotification<K, V> {
    private final CacheStoreType cacheStoreType;

    public StoreAwareCacheRemovalNotification(K key, V value, RemovalReason removalReason, CacheStoreType cacheStoreType) {
        super(key, value, removalReason);
        this.cacheStoreType = cacheStoreType;
    }

    public CacheStoreType getCacheStoreType() {
        return cacheStoreType;
    }
}
