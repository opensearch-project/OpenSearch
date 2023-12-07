/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.listeners.dispatchers;

import org.opensearch.common.Nullable;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.EventType;

/**
 * Encapsulates all logic to dispatch events for specific cache store type.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public interface StoreAwareCacheEventListenerDispatcher<K, V> {
    void dispatch(K key, @Nullable V value, CacheStoreType cacheStoreType, EventType eventType);

    void dispatchRemovalEvent(StoreAwareCacheRemovalNotification<K, V> notification);
}
