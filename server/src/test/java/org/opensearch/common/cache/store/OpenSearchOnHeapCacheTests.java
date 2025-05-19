/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

public class OpenSearchOnHeapCacheTests extends OpenSearchTestCase {
    private final static long keyValueSize = 50;
    private final static List<String> dimensionNames = List.of("dim1", "dim2", "dim3");

    public void testStats() throws Exception {
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        int maxKeys = between(10, 50);
        int numEvicted = between(10, 20);
        OpenSearchOnHeapCache<String, String> cache = getCache(maxKeys, listener, true);

        // When the pluggable caches setting is on, we should get stats as expected from cache.stats().

        List<ICacheKey<String>> keysAdded = new ArrayList<>();
        int numAdded = maxKeys + numEvicted;
        for (int i = 0; i < numAdded; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            keysAdded.add(key);
            cache.computeIfAbsent(key, getLoadAwareCacheLoader());

            assertEquals(i + 1, cache.stats().getTotalMisses());
            assertEquals(0, cache.stats().getTotalHits());
            assertEquals(Math.min(maxKeys, i + 1), cache.stats().getTotalItems());
            assertEquals(Math.min(maxKeys, i + 1) * keyValueSize, cache.stats().getTotalSizeInBytes());
            assertEquals(Math.max(0, i + 1 - maxKeys), cache.stats().getTotalEvictions());
        }
        // do gets from the last part of the list, which should be hits
        for (int i = numAdded - maxKeys; i < numAdded; i++) {
            cache.computeIfAbsent(keysAdded.get(i), getLoadAwareCacheLoader());
            int numHits = i + 1 - (numAdded - maxKeys);

            assertEquals(numAdded, cache.stats().getTotalMisses());
            assertEquals(numHits, cache.stats().getTotalHits());
            assertEquals(maxKeys, cache.stats().getTotalItems());
            assertEquals(maxKeys * keyValueSize, cache.stats().getTotalSizeInBytes());
            assertEquals(numEvicted, cache.stats().getTotalEvictions());
        }

        // invalidate keys
        for (int i = numAdded - maxKeys; i < numAdded; i++) {
            cache.invalidate(keysAdded.get(i));
            int numInvalidated = i + 1 - (numAdded - maxKeys);

            assertEquals(numAdded, cache.stats().getTotalMisses());
            assertEquals(maxKeys, cache.stats().getTotalHits());
            assertEquals(maxKeys - numInvalidated, cache.stats().getTotalItems());
            assertEquals((maxKeys - numInvalidated) * keyValueSize, cache.stats().getTotalSizeInBytes());
            assertEquals(numEvicted, cache.stats().getTotalEvictions());
        }
    }

    public void testWithoutStatsTracking() throws Exception {
        // When we manually set statsTrackingEnabled = false in the config,
        // we should get all-zero stats from cache.stats(), but count() should still work.
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        int maxKeys = between(10, 50);
        int numEvicted = between(10, 20);

        OpenSearchOnHeapCache<String, String> manuallySetNoopStatsCache = getCache(maxKeys, listener, false);
        int numAdded = maxKeys + numEvicted;
        for (int i = 0; i < numAdded; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            manuallySetNoopStatsCache.computeIfAbsent(key, getLoadAwareCacheLoader());

            assertEquals(Math.min(maxKeys, i + 1), manuallySetNoopStatsCache.count());
            ImmutableCacheStatsHolder stats = manuallySetNoopStatsCache.stats();
            assertZeroStats(manuallySetNoopStatsCache.stats());
        }
    }

    public void testWithCacheConfigSizeSettings_WhenStoreNameBlank() {
        // If the store name is blank, the cache should get its size from the config, not the setting.
        long maxSizeFromSetting = between(1000, 2000);
        long maxSizeFromConfig = between(3000, 4000);

        Settings.Builder storeNameBlankSettingsBuilder = Settings.builder();
        OpenSearchOnHeapCache<String, String> onHeapCache = setupMaxSizeTest(
            storeNameBlankSettingsBuilder,
            maxSizeFromSetting,
            maxSizeFromConfig,
            true
        );
        assertEquals(maxSizeFromConfig, onHeapCache.getMaximumWeight());
    }

    public void testWithCacheConfigSettings_WhenStoreNameNotBlank() {
        // When the store name is not blank, the cache should get its size from the config if present, and otherwise should get it from the
        // setting.
        Settings.Builder settingsBuilder = Settings.builder()
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            );
        long maxSizeFromSetting = between(1000, 2000);
        long maxSizeFromConfig = between(3000, 4000);
        OpenSearchOnHeapCache<String, String> onHeapCache = setupMaxSizeTest(settingsBuilder, maxSizeFromSetting, maxSizeFromConfig, false);
        assertEquals(maxSizeFromSetting, onHeapCache.getMaximumWeight());

        onHeapCache = setupMaxSizeTest(settingsBuilder, maxSizeFromSetting, maxSizeFromConfig, true);
        assertEquals(maxSizeFromConfig, onHeapCache.getMaximumWeight());
    }

    private OpenSearchOnHeapCache<String, String> setupMaxSizeTest(
        Settings.Builder settingsBuilder,
        long maxSizeFromSetting,
        long maxSizeFromConfig,
        boolean putSizeInConfig
    ) {
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        settingsBuilder.put(
            OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                .getKey(),
            maxSizeFromSetting + "b"
        );

        CacheConfig.Builder<String, String> cacheConfigBuilder = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setValueType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setRemovalListener(listener)
            .setSettings(settingsBuilder.build())
            .setDimensionNames(dimensionNames)
            .setStatsTrackingEnabled(true);
        if (putSizeInConfig) {
            cacheConfigBuilder.setMaxSizeInBytes(maxSizeFromConfig);
        }

        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        return (OpenSearchOnHeapCache<String, String>) onHeapCacheFactory.create(
            cacheConfigBuilder.build(),
            CacheType.INDICES_REQUEST_CACHE,
            null
        );
    }

    private void assertZeroStats(ImmutableCacheStatsHolder stats) {
        assertEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), stats.getTotalStats());
    }

    private OpenSearchOnHeapCache<String, String> getCache(
        int maxSizeKeys,
        MockRemovalListener<String, String> listener,
        boolean statsTrackingEnabled
    ) {
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        Settings settings = Settings.builder()
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                maxSizeKeys * keyValueSize + "b"
            )
            .build();

        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setValueType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setRemovalListener(listener)
            .setSettings(settings)
            .setDimensionNames(dimensionNames)
            .setMaxSizeInBytes(maxSizeKeys * keyValueSize)
            .setStatsTrackingEnabled(statsTrackingEnabled)
            .build();
        return (OpenSearchOnHeapCache<String, String>) onHeapCacheFactory.create(cacheConfig, CacheType.INDICES_REQUEST_CACHE, null);
    }

    public void testInvalidateWithDropDimensions() throws Exception {
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        int maxKeys = 50;
        OpenSearchOnHeapCache<String, String> cache = getCache(maxKeys, listener, true);

        List<ICacheKey<String>> keysAdded = new ArrayList<>();

        for (int i = 0; i < maxKeys - 5; i++) {
            ICacheKey<String> key = new ICacheKey<>(UUID.randomUUID().toString(), getRandomDimensions());
            keysAdded.add(key);
            cache.computeIfAbsent(key, getLoadAwareCacheLoader());
        }

        ICacheKey<String> keyToDrop = keysAdded.get(0);
        String[] levels = dimensionNames.toArray(new String[0]);
        ImmutableCacheStats snapshot = cache.stats(levels).getStatsForDimensionValues(keyToDrop.dimensions);
        assertNotNull(snapshot);

        keyToDrop.setDropStatsForDimensions(true);
        cache.invalidate(keyToDrop);

        // Now assert the stats are gone for any key that has this combination of dimensions, but still there otherwise
        for (ICacheKey<String> keyAdded : keysAdded) {
            snapshot = cache.stats(levels).getStatsForDimensionValues(keyAdded.dimensions);
            if (keyAdded.dimensions.equals(keyToDrop.dimensions)) {
                assertNull(snapshot);
            } else {
                assertNotNull(snapshot);
            }
        }
    }

    private List<String> getRandomDimensions() {
        Random rand = Randomness.get();
        int bound = 3;
        List<String> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            result.add(String.valueOf(rand.nextInt(bound)));
        }
        return result;
    }

    private static class MockRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {
        CounterMetric numRemovals;

        MockRemovalListener() {
            numRemovals = new CounterMetric();
        }

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            numRemovals.inc();
        }
    }

    private ICacheKey<String> getICacheKey(String key) {
        List<String> dims = new ArrayList<>();
        for (String dimName : dimensionNames) {
            dims.add("0");
        }
        return new ICacheKey<>(key, dims);
    }

    private LoadAwareCacheLoader<ICacheKey<String>, String> getLoadAwareCacheLoader() {
        return new LoadAwareCacheLoader<>() {
            boolean isLoaded = false;

            @Override
            public String load(ICacheKey<String> key) {
                isLoaded = true;
                return UUID.randomUUID().toString();
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }
}
