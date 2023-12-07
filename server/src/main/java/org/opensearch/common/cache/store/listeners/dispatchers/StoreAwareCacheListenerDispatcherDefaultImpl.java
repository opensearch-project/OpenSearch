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
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListenerConfiguration;

/**
 * Default implementation of event listener dispatcher.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public class StoreAwareCacheListenerDispatcherDefaultImpl<K, V> implements StoreAwareCacheEventListenerDispatcher<K, V> {

    private final StoreAwareCacheEventListenerConfiguration<K, V> listenerConfiguration;

    public StoreAwareCacheListenerDispatcherDefaultImpl(StoreAwareCacheEventListenerConfiguration<K, V> listenerConfiguration) {
        this.listenerConfiguration = listenerConfiguration;
    }

    @Override
    public void dispatch(K key, @Nullable V value, CacheStoreType cacheStoreType, EventType eventType) {
        switch (eventType) {
            case ON_CACHED:
                if (this.listenerConfiguration.getEventTypes().contains(EventType.ON_CACHED)) {
                    this.listenerConfiguration.getEventListener().onCached(key, value, cacheStoreType);
                }
                break;
            case ON_HIT:
                if (this.listenerConfiguration.getEventTypes().contains(EventType.ON_HIT)) {
                    this.listenerConfiguration.getEventListener().onHit(key, value, cacheStoreType);
                }
                break;
            case ON_MISS:
                if (this.listenerConfiguration.getEventTypes().contains(EventType.ON_MISS)) {
                    this.listenerConfiguration.getEventListener().onMiss(key, cacheStoreType);
                }
                break;
            case ON_REMOVAL:
                // Handled separately
                break;
            default:
                throw new IllegalArgumentException("Unsupported event type: " + eventType);
        }
    }

    @Override
    public void dispatchRemovalEvent(StoreAwareCacheRemovalNotification<K, V> notification) {
        if (this.listenerConfiguration.getEventTypes().contains(EventType.ON_REMOVAL)) {
            this.listenerConfiguration.getEventListener().onRemoval(notification);
        }
    }
}
