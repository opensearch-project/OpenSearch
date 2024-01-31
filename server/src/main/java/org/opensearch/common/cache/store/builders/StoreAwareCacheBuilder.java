/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.builders;

import org.opensearch.common.cache.cleaner.CacheCleaner;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.unit.TimeValue;

import java.util.function.ToLongBiFunction;

/**
 * Builder for store aware cache.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.internal
 */
public abstract class StoreAwareCacheBuilder<K, V> {

    private long maxWeightInBytes;

    private ToLongBiFunction<K, V> weigher;

    private TimeValue expireAfterAccess;

    private StoreAwareCacheEventListener<K, V> eventListener;
    private CacheCleaner.Builder<K, V> cacheCleanerBuilder;

    public StoreAwareCacheBuilder() {}

    public StoreAwareCacheBuilder<K, V> setMaximumWeightInBytes(long sizeInBytes) {
        this.maxWeightInBytes = sizeInBytes;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setWeigher(ToLongBiFunction<K, V> weigher) {
        this.weigher = weigher;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
        this.expireAfterAccess = expireAfterAccess;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setEventListener(StoreAwareCacheEventListener<K, V> eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setCacheCleanerBuilder(CacheCleaner.Builder<K, V> cacheCleaner) {
        this.cacheCleanerBuilder = cacheCleaner;
        return this;
    }

    public long getMaxWeightInBytes() {
        return maxWeightInBytes;
    }

    public TimeValue getExpireAfterAccess() {
        return expireAfterAccess;
    }

    public ToLongBiFunction<K, V> getWeigher() {
        return weigher;
    }

    public StoreAwareCacheEventListener<K, V> getEventListener() {
        return eventListener;
    }

    public CacheCleaner.Builder<K, V> getCacheCleanerBuilder() {
        return cacheCleanerBuilder;
    }

    public abstract StoreAwareCache<K, V> build();
}
