/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.config;

import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.function.ToLongBiFunction;

/**
 * Configurations related to store aware caches
 *
 * @opensearch.internal
 */
public class StoreAwareCacheConfig<K, V> {

    private long maxWeightInBytes;

    private ToLongBiFunction<K, V> weigher;

    private TimeValue expireAfterAcess;

    private StoreAwareCacheEventListener<K, V> eventListener;

    private Settings settings;

    private Class<K> keyType;

    private Class<V> valueType;

    private String storagePath;

    private String threadPoolAlias;

    private String cacheAlias;

    private String settingPrefix;

    // Provides capability to make event listener to run in sync/async mode.
    private boolean isEventListenerModeSync;

    private StoreAwareCacheConfig(Builder<K, V> builder) {
        this.cacheAlias = builder.cacheAlias;
        this.keyType = builder.keyType;
    }

    public long getMaxWeightInBytes() {
        return maxWeightInBytes;
    }

    public ToLongBiFunction<K, V> getWeigher() {
        return weigher;
    }

    public TimeValue getExpireAfterAcess() {
        return expireAfterAcess;
    }

    public StoreAwareCacheEventListener<K, V> getEventListener() {
        return eventListener;
    }

    public boolean isEventListenerModeSync() {
        return isEventListenerModeSync;
    }

    public Class<K> getKeyType() {
        return keyType;
    }

    public Class<V> getValueType() {
        return valueType;
    }

    public String getCacheAlias() {
        return cacheAlias;
    }

    public Settings getSettings() {
        return settings;
    }

    public String getSettingPrefix() {
        return settingPrefix;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public String getThreadPoolAlias() {
        return threadPoolAlias;
    }

    /**
     * Builder class to build Cache config related parameters.
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public static class Builder<K, V> {

        private long maxWeightInBytes;

        private ToLongBiFunction<K, V> weigher;

        private TimeValue expireAfterAcess;

        private StoreAwareCacheEventListener<K, V> eventListener;

        private Settings settings;

        private Class<K> keyType;

        private Class<V> valueType;

        private String storagePath;

        private String threadPoolAlias;

        private String cacheAlias;

        private String settingPrefix;

        // Provides capability to make event listener to run in sync/async mode.
        private boolean isEventListenerModeSync;

        public Builder() {}

        public Builder<K, V> setMaxWeightInBytes(long maxWeightInBytes) {
            this.maxWeightInBytes = maxWeightInBytes;
            return this;
        }

        public Builder<K, V> setWeigher(ToLongBiFunction<K, V> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder<K, V> setExpireAfterAcess(TimeValue expireAfterAcess) {
            this.expireAfterAcess = expireAfterAcess;
            return this;
        }

        public Builder<K, V> setEventListener(StoreAwareCacheEventListener<K, V> listener) {
            this.eventListener = listener;
            return this;
        }

        public Builder<K, V> setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public Builder<K, V> setValueType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public Builder<K, V> setStoragePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public Builder<K, V> setThreadPoolAlias(String threadPoolAlias) {
            this.threadPoolAlias = threadPoolAlias;
            return this;
        }

        public Builder<K, V> setCacheAlias(String cacheAlias) {
            this.cacheAlias = cacheAlias;
            return this;
        }

        public Builder<K, V> setIsEventListenerModeSync(boolean isEventListenerModeSync) {
            this.isEventListenerModeSync = isEventListenerModeSync;
            return this;
        }

        public StoreAwareCacheConfig<K, V> build() {
            return new StoreAwareCacheConfig<>(this);
        }
    }
}
