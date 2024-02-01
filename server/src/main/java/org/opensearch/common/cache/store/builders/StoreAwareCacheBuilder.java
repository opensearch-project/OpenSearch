/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.builders;

import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.settings.Settings;
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

    private TimeValue expireAfterAcess;

    private StoreAwareCacheEventListener<K, V> eventListener;

    private Settings settings;

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

    public StoreAwareCacheBuilder<K, V> setEventListener(StoreAwareCacheEventListener<K, V> eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public StoreAwareCacheBuilder<K, V> setSettings(Settings settings) {
        this.settings = settings;
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

    public StoreAwareCacheEventListener<K, V> getEventListener() {
        return this.eventListener;
    }

    public Settings getSettings() {
        return settings;
    }

    public abstract StoreAwareCache<K, V> build();
}
