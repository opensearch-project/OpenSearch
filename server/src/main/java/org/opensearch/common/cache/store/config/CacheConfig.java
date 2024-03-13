/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.config;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.function.ToLongBiFunction;

/**
 * Common configurations related to store aware caches.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CacheConfig<K, V> {

    private final Settings settings;

    /**
     * Defines the key type.
     */
    private final Class<K> keyType;

    /**
     * Defines the value type.
     */
    private final Class<V> valueType;

    /**
     * Represents a function that calculates the size or weight of a key-value pair.
     */
    private final ToLongBiFunction<K, V> weigher;

    private final RemovalListener<K, V> removalListener;

    /**
     * Max size in bytes for the cache. This is needed for backward compatibility.
     */
    private final long maxSizeInBytes;

    /**
     * Defines the expiration time for a cache entry. This is needed for backward compatibility.
     */
    private final TimeValue expireAfterAccess;

    private CacheConfig(Builder<K, V> builder) {
        this.keyType = builder.keyType;
        this.valueType = builder.valueType;
        this.settings = builder.settings;
        this.removalListener = builder.removalListener;
        this.weigher = builder.weigher;
        this.maxSizeInBytes = builder.maxSizeInBytes;
        this.expireAfterAccess = builder.expireAfterAccess;
    }

    public Class<K> getKeyType() {
        return keyType;
    }

    public Class<V> getValueType() {
        return valueType;
    }

    public Settings getSettings() {
        return settings;
    }

    public RemovalListener<K, V> getRemovalListener() {
        return removalListener;
    }

    public ToLongBiFunction<K, V> getWeigher() {
        return weigher;
    }

    public Long getMaxSizeInBytes() {
        return maxSizeInBytes;
    }

    public TimeValue getExpireAfterAccess() {
        return expireAfterAccess;
    }

    /**
     * Builder class to build Cache config related parameters.
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public static class Builder<K, V> {

        private Settings settings;

        private Class<K> keyType;

        private Class<V> valueType;

        private RemovalListener<K, V> removalListener;

        private ToLongBiFunction<K, V> weigher;

        private long maxSizeInBytes;

        private TimeValue expireAfterAccess;

        public Builder() {}

        public Builder<K, V> setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder<K, V> setKeyType(Class<K> keyType) {
            this.keyType = keyType;
            return this;
        }

        public Builder<K, V> setValueType(Class<V> valueType) {
            this.valueType = valueType;
            return this;
        }

        public Builder<K, V> setRemovalListener(RemovalListener<K, V> removalListener) {
            this.removalListener = removalListener;
            return this;
        }

        public Builder<K, V> setWeigher(ToLongBiFunction<K, V> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder<K, V> setMaxSizeInBytes(long sizeInBytes) {
            this.maxSizeInBytes = sizeInBytes;
            return this;
        }

        public Builder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
            this.expireAfterAccess = expireAfterAccess;
            return this;
        }

        public CacheConfig<K, V> build() {
            return new CacheConfig<>(this);
        }
    }
}
