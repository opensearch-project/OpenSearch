/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.builders;

import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListenerConfiguration;
import org.opensearch.common.unit.TimeValue;

import java.util.function.ToLongBiFunction;

/**
 * Builder for store aware cache.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public abstract class StoreAwareCacheBuilder<K, V> {

    private long maxWeightInBytes;

    private ToLongBiFunction<K, V> weigher;

    private TimeValue expireAfterAcess;

    private StoreAwareCacheEventListenerConfiguration<K, V> listenerConfiguration;

    public StoreAwareCacheBuilder() {}

    public StoreAwareCacheBuilder<K, V> setMaximumWeightInBytes(long sizeInBytes) {
        this.maxWeightInBytes = sizeInBytes;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setWeigher(ToLongBiFunction<K, V> weigher) {
        this.weigher = weigher;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setExpireAfterAccess(TimeValue expireAfterAcess) {
        this.expireAfterAcess = expireAfterAcess;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setEventListenerConfiguration(
        StoreAwareCacheEventListenerConfiguration<K, V> listenerConfiguration
    ) {
        this.listenerConfiguration = listenerConfiguration;
        return this;
    }

    public long getMaxWeightInBytes() {
        return maxWeightInBytes;
    }

    public TimeValue getExpireAfterAcess() {
        return expireAfterAcess;
    }

    public ToLongBiFunction<K, V> getWeigher() {
        return weigher;
    }

    public StoreAwareCacheEventListenerConfiguration<K, V> getListenerConfiguration() {
        return listenerConfiguration;
    }

    public abstract StoreAwareCache<K, V> build();
}
