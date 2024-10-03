/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.config;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.List;
import java.util.function.Function;
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
    private final ToLongBiFunction<ICacheKey<K>, V> weigher;

    private final RemovalListener<ICacheKey<K>, V> removalListener;

    private final List<String> dimensionNames;

    // Serializers for keys and values. Not required for all caches.
    private final Serializer<K, ?> keySerializer;
    private final Serializer<V, ?> valueSerializer;

    /** A function which extracts policy-relevant information, such as took time, from values, to allow inspection by policies if present. */
    private Function<V, CachedQueryResult.PolicyValues> cachedResultParser;
    /**
     * Max size in bytes for the cache. This is needed for backward compatibility.
     */
    private final long maxSizeInBytes;

    /**
     * Defines the expiration time for a cache entry. This is needed for backward compatibility.
     */
    private final TimeValue expireAfterAccess;

    private final ClusterSettings clusterSettings;

    private final boolean statsTrackingEnabled;

    private final String storagePath;

    private final int segmentCount;

    private final int segmentNumber;

    private CacheConfig(Builder<K, V> builder) {
        this.keyType = builder.keyType;
        this.valueType = builder.valueType;
        this.settings = builder.settings;
        this.removalListener = builder.removalListener;
        this.weigher = builder.weigher;
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.dimensionNames = builder.dimensionNames;
        this.cachedResultParser = builder.cachedResultParser;
        this.maxSizeInBytes = builder.maxSizeInBytes;
        this.expireAfterAccess = builder.expireAfterAccess;
        this.clusterSettings = builder.clusterSettings;
        this.statsTrackingEnabled = builder.statsTrackingEnabled;
        this.storagePath = builder.storagePath;
        this.segmentCount = builder.segmentCount;
        this.segmentNumber = builder.segmentNumber;
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

    public RemovalListener<ICacheKey<K>, V> getRemovalListener() {
        return removalListener;
    }

    public Serializer<K, ?> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V, ?> getValueSerializer() {
        return valueSerializer;
    }

    public ToLongBiFunction<ICacheKey<K>, V> getWeigher() {
        return weigher;
    }

    public Function<V, CachedQueryResult.PolicyValues> getCachedResultParser() {
        return cachedResultParser;
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public Long getMaxSizeInBytes() {
        return maxSizeInBytes;
    }

    public TimeValue getExpireAfterAccess() {
        return expireAfterAccess;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public boolean getStatsTrackingEnabled() {
        return statsTrackingEnabled;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public int getSegmentNumber() {
        return segmentNumber;
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

        private RemovalListener<ICacheKey<K>, V> removalListener;
        private List<String> dimensionNames;
        private Serializer<K, ?> keySerializer;
        private Serializer<V, ?> valueSerializer;
        private ToLongBiFunction<ICacheKey<K>, V> weigher;
        private Function<V, CachedQueryResult.PolicyValues> cachedResultParser;

        private long maxSizeInBytes;

        private TimeValue expireAfterAccess;
        private ClusterSettings clusterSettings;
        private boolean statsTrackingEnabled = true;
        private String storagePath;
        private int segmentCount;

        private int segmentNumber;

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

        public Builder<K, V> setRemovalListener(RemovalListener<ICacheKey<K>, V> removalListener) {
            this.removalListener = removalListener;
            return this;
        }

        public Builder<K, V> setWeigher(ToLongBiFunction<ICacheKey<K>, V> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder<K, V> setKeySerializer(Serializer<K, ?> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K, V> setValueSerializer(Serializer<V, ?> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        public Builder<K, V> setCachedResultParser(Function<V, CachedQueryResult.PolicyValues> function) {
            this.cachedResultParser = function;
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

        public Builder<K, V> setClusterSettings(ClusterSettings clusterSettings) {
            this.clusterSettings = clusterSettings;
            return this;
        }

        public Builder<K, V> setStatsTrackingEnabled(boolean statsTrackingEnabled) {
            this.statsTrackingEnabled = statsTrackingEnabled;
            return this;
        }

        public Builder<K, V> setStoragePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public Builder<K, V> setSegmentCount(int segmentCount) {
            this.segmentCount = segmentCount;
            return this;
        }

        public Builder<K, V> setSegmentNumber(int segmentNumber) {
            this.segmentNumber = segmentNumber;
            return this;
        }

        public CacheConfig<K, V> build() {
            return new CacheConfig<>(this);
        }
    }
}
