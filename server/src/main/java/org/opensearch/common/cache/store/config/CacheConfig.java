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

    private CacheConfig(Builder<K, V> builder) {
        this.keyType = builder.keyType;
        this.valueType = builder.valueType;
        this.settings = builder.settings;
        this.removalListener = builder.removalListener;
        this.weigher = builder.weigher;
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

        public CacheConfig<K, V> build() {
            return new CacheConfig<>(this);
        }
    }
}
