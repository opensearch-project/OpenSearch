/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_NAME;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_ON_HEAP;
import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

public class TieredSpilloverCacheTests extends OpenSearchTestCase {
    static final List<String> dimensionNames = List.of("dim1", "dim2", "dim3");

    private ClusterSettings clusterSettings;

    @Before
    public void setup() {
        Settings settings = Settings.EMPTY;
        clusterSettings = new ClusterSettings(settings, new HashSet<>());
        clusterSettings.registerSetting(TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE));
        clusterSettings.registerSetting(DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_REQUEST_CACHE));
    }

    public void testComputeIfAbsentWithoutAnyOnHeapCacheEviction() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            randomIntBetween(1, 4),
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );
        int numOfItems1 = randomIntBetween(1, onHeapCacheSize / 2 - 1);
        List<ICacheKey<String>> keys = new ArrayList<>();
        // Put values in cache.
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            keys.add(key);
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(0, removalListener.evictionsMetric.count());
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));

        // Try to hit cache again with some randomization.
        int numOfItems2 = randomIntBetween(1, onHeapCacheSize / 2 - 1);
        int cacheHit = 0;
        int cacheMiss = 0;
        for (int iter = 0; iter < numOfItems2; iter++) {
            if (randomBoolean()) {
                // Hit cache with stored key
                cacheHit++;
                int index = randomIntBetween(0, keys.size() - 1);
                tieredSpilloverCache.computeIfAbsent(keys.get(index), getLoadAwareCacheLoader());
            } else {
                // Hit cache with randomized key which is expected to miss cache always.
                tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), getLoadAwareCacheLoader());
                cacheMiss++;
            }
        }
        assertEquals(0, removalListener.evictionsMetric.count());
        assertEquals(cacheHit, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(numOfItems1 + cacheMiss, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));

        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(numOfItems1 + cacheMiss, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
    }

    public void testComputeIfAbsentWithFactoryBasedCacheCreation() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        // Set the desired settings needed to create a TieredSpilloverCache object with INDICES_REQUEST_CACHE cacheType.
        Settings settings = Settings.builder()
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                MockDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSize * keyValueSize + "b"
            )
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
            )
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();

        ICache<String, String> tieredSpilloverICache = new TieredSpilloverCache.TieredSpilloverCacheFactory().create(
            new CacheConfig.Builder<String, String>().setKeyType(String.class)
                .setKeyType(String.class)
                .setWeigher((k, v) -> keyValueSize)
                .setRemovalListener(removalListener)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setSettings(settings)
                .setDimensionNames(dimensionNames)
                .setCachedResultParser(s -> new CachedQueryResult.PolicyValues(20_000_000L)) // Values will always appear to have taken
                                                                                             // 20_000_000 ns = 20 ms to compute
                .setClusterSettings(clusterSettings)
                .build(),
            CacheType.INDICES_REQUEST_CACHE,
            Map.of(
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
                new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory(),
                MockDiskCache.MockDiskCacheFactory.NAME,
                new MockDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300), false)
            )
        );

        TieredSpilloverCache<String, String> tieredSpilloverCache = (TieredSpilloverCache<String, String>) tieredSpilloverICache;

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(getICacheKey(key), tieredCacheLoader);
        }

        int expectedDiskEntries = numOfItems1 - onHeapCacheSize;
        tieredSpilloverCache.getOnHeapCache().keys().forEach(onHeapKeys::add);
        tieredSpilloverCache.getDiskCache().keys().forEach(diskTierKeys::add);
        // Verify on heap cache stats.
        assertEquals(onHeapCacheSize, tieredSpilloverCache.getOnHeapCache().count());
        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(expectedDiskEntries, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));

        // Verify disk cache stats.
        assertEquals(expectedDiskEntries, tieredSpilloverCache.getDiskCache().count());
        assertEquals(expectedDiskEntries, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(expectedDiskEntries * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
    }

    public void testWithFactoryCreationWithOnHeapCacheNotPresent() {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int keyValueSize = 50;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        // Set the settings without onHeap cache settings.
        Settings settings = Settings.builder()
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                MockDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSize * keyValueSize + "b"
            )
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
            )
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new TieredSpilloverCache.TieredSpilloverCacheFactory().create(
                new CacheConfig.Builder<String, String>().setKeyType(String.class)
                    .setKeyType(String.class)
                    .setWeigher((k, v) -> keyValueSize)
                    .setRemovalListener(removalListener)
                    .setSettings(settings)
                    .build(),
                CacheType.INDICES_REQUEST_CACHE,
                Map.of(
                    OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
                    new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory(),
                    MockDiskCache.MockDiskCacheFactory.NAME,
                    new MockDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300), false)
                )
            )
        );
        assertEquals(
            ex.getMessage(),
            "No associated onHeapCache found for tieredSpilloverCache for " + "cacheType:" + CacheType.INDICES_REQUEST_CACHE
        );
    }

    public void testWithFactoryCreationWithDiskCacheNotPresent() {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int keyValueSize = 50;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        // Set the settings without onHeap cache settings.
        Settings settings = Settings.builder()
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSize * keyValueSize + "b"
            )
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new TieredSpilloverCache.TieredSpilloverCacheFactory().create(
                new CacheConfig.Builder<String, String>().setKeyType(String.class)
                    .setKeyType(String.class)
                    .setWeigher((k, v) -> keyValueSize)
                    .setRemovalListener(removalListener)
                    .setSettings(settings)
                    .build(),
                CacheType.INDICES_REQUEST_CACHE,
                Map.of(
                    OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
                    new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory(),
                    MockDiskCache.MockDiskCacheFactory.NAME,
                    new MockDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300), false)
                )
            )
        );
        assertEquals(
            ex.getMessage(),
            "No associated diskCache found for tieredSpilloverCache for " + "cacheType:" + CacheType.INDICES_REQUEST_CACHE
        );
    }

    public void testComputeIfAbsentWithEvictionsFromOnHeapCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int keyValueSize = 50;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setRemovalListener(removalListener)
            .setKeySerializer(new StringSerializer())
            .setValueSerializer(new StringSerializer())
            .setDimensionNames(dimensionNames)
            .setSettings(
                Settings.builder()
                    .put(
                        CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
                    )
                    .put(FeatureFlags.PLUGGABLE_CACHE, "true")
                    .put(
                        OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                            .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                            .getKey(),
                        onHeapCacheSize * keyValueSize + "b"
                    )
                    .build()
            )
            .setClusterSettings(clusterSettings)
            .build();

        ICache.Factory mockDiskCacheFactory = new MockDiskCache.MockDiskCacheFactory(0, diskCacheSize, false);

        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(mockDiskCacheFactory)
            .setCacheConfig(cacheConfig)
            .setRemovalListener(removalListener)
            .setCacheType(CacheType.INDICES_REQUEST_CACHE)
            .build();

        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }

        long actualDiskCacheSize = tieredSpilloverCache.getDiskCache().count();

        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(actualDiskCacheSize, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(actualDiskCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));

        tieredSpilloverCache.getOnHeapCache().keys().forEach(onHeapKeys::add);
        tieredSpilloverCache.getDiskCache().keys().forEach(diskTierKeys::add);

        // Try to hit cache again with some randomization.
        int numOfItems2 = randomIntBetween(50, 200);
        int onHeapCacheHit = 0;
        int diskCacheHit = 0;
        int cacheMiss = 0;
        for (int iter = 0; iter < numOfItems2; iter++) {
            if (randomBoolean()) { // Hit cache with key stored in onHeap cache.
                onHeapCacheHit++;
                int index = randomIntBetween(0, onHeapKeys.size() - 1);
                LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
                tieredSpilloverCache.computeIfAbsent(onHeapKeys.get(index), loadAwareCacheLoader);
                assertFalse(loadAwareCacheLoader.isLoaded());
            } else { // Hit cache with key stored in disk cache.
                diskCacheHit++;
                int index = randomIntBetween(0, diskTierKeys.size() - 1);
                LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
                tieredSpilloverCache.computeIfAbsent(diskTierKeys.get(index), loadAwareCacheLoader);
                assertFalse(loadAwareCacheLoader.isLoaded());
            }
        }
        int numRandom = randomIntBetween(50, 200);
        for (int iter = 0; iter < numRandom; iter++) {
            // Hit cache with randomized key which is expected to miss cache always.
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), tieredCacheLoader);
            cacheMiss++;
        }

        assertEquals(numOfItems1 + cacheMiss + diskCacheHit, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheHit, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(cacheMiss + numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(diskCacheHit, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
    }

    public void testComputeIfAbsentWithEvictionsFromTieredCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );
        int numOfItems = randomIntBetween(totalSize + 1, totalSize * 3);
        for (int iter = 0; iter < numOfItems; iter++) {
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), tieredCacheLoader);
        }

        int evictions = numOfItems - (totalSize); // Evictions from the cache as a whole
        assertEquals(evictions, removalListener.evictionsMetric.count());
        assertEquals(evictions, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(
            evictions + getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK),
            getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP)
        );
    }

    public void testGetAndCount() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            if (iter > (onHeapCacheSize - 1)) {
                // All these are bound to go to disk based cache.
                diskTierKeys.add(key);
            } else {
                onHeapKeys.add(key);
            }
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
        }

        for (int iter = 0; iter < numOfItems1; iter++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    int index = randomIntBetween(0, onHeapKeys.size() - 1);
                    assertNotNull(tieredSpilloverCache.get(onHeapKeys.get(index)));
                } else {
                    int index = randomIntBetween(0, diskTierKeys.size() - 1);
                    assertNotNull(tieredSpilloverCache.get(diskTierKeys.get(index)));
                }
            } else {
                assertNull(tieredSpilloverCache.get(getICacheKey(UUID.randomUUID().toString())));
            }
        }
        assertEquals(numOfItems1, tieredSpilloverCache.count());
    }

    public void testPut() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );
        ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
        String value = UUID.randomUUID().toString();
        tieredSpilloverCache.put(key, value);
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, tieredSpilloverCache.count());
    }

    public void testPutAndVerifyNewItemsArePresentOnHeapCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(200, 400);
        int diskCacheSize = randomIntBetween(450, 800);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                    TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
                )
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    (onHeapCacheSize * keyValueSize) + "b"
                )
                .build(),
            0
        );

        for (int i = 0; i < onHeapCacheSize; i++) {
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), getLoadAwareCacheLoader());
        }

        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));

        // Again try to put OnHeap cache capacity amount of new items.
        List<ICacheKey<String>> newKeyList = new ArrayList<>();
        for (int i = 0; i < onHeapCacheSize; i++) {
            newKeyList.add(getICacheKey(UUID.randomUUID().toString()));
        }

        for (int i = 0; i < newKeyList.size(); i++) {
            tieredSpilloverCache.computeIfAbsent(newKeyList.get(i), getLoadAwareCacheLoader());
        }

        // Verify that new items are part of onHeap cache.
        List<ICacheKey<String>> actualOnHeapCacheKeys = new ArrayList<>();
        tieredSpilloverCache.getOnHeapCache().keys().forEach(actualOnHeapCacheKeys::add);

        assertEquals(newKeyList.size(), actualOnHeapCacheKeys.size());
        for (int i = 0; i < actualOnHeapCacheKeys.size(); i++) {
            assertTrue(newKeyList.contains(actualOnHeapCacheKeys.get(i)));
        }
        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
    }

    public void testInvalidate() throws Exception {
        int onHeapCacheSize = 1;
        int diskCacheSize = 10;
        int keyValueSize = 20;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );
        ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
        String value = UUID.randomUUID().toString();
        // First try to invalidate without the key present in cache.
        tieredSpilloverCache.invalidate(key);
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));

        // Now try to invalidate with the key present in onHeap cache.
        tieredSpilloverCache.put(key, value);
        tieredSpilloverCache.invalidate(key);
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        // Evictions metric shouldn't increase for invalidations.
        assertEquals(0, tieredSpilloverCache.count());

        tieredSpilloverCache.put(key, value);
        // Put another key/value so that one of the item is evicted to disk cache.
        ICacheKey<String> key2 = getICacheKey(UUID.randomUUID().toString());
        tieredSpilloverCache.put(key2, UUID.randomUUID().toString());

        assertEquals(2, tieredSpilloverCache.count());
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));

        // Again invalidate older key, leaving one in heap tier and zero in disk tier
        tieredSpilloverCache.invalidate(key);
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, tieredSpilloverCache.count());
    }

    public void testCacheKeys() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        // During first round add onHeapCacheSize entries. Will go to onHeap cache initially.
        for (int i = 0; i < onHeapCacheSize; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            diskTierKeys.add(key);
            tieredSpilloverCache.computeIfAbsent(key, getLoadAwareCacheLoader());
        }
        // In another round, add another onHeapCacheSize entries. These will go to onHeap and above ones will be
        // evicted to onDisk cache.
        for (int i = 0; i < onHeapCacheSize; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            onHeapKeys.add(key);
            tieredSpilloverCache.computeIfAbsent(key, getLoadAwareCacheLoader());
        }

        List<ICacheKey<String>> actualOnHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> actualOnDiskKeys = new ArrayList<>();
        Iterable<ICacheKey<String>> onHeapiterable = tieredSpilloverCache.getOnHeapCache().keys();
        Iterable<ICacheKey<String>> onDiskiterable = tieredSpilloverCache.getDiskCache().keys();
        onHeapiterable.iterator().forEachRemaining(actualOnHeapKeys::add);
        onDiskiterable.iterator().forEachRemaining(actualOnDiskKeys::add);
        for (ICacheKey<String> onHeapKey : onHeapKeys) {
            assertTrue(actualOnHeapKeys.contains(onHeapKey));
        }
        for (ICacheKey<String> onDiskKey : actualOnDiskKeys) {
            assertTrue(actualOnDiskKeys.contains(onDiskKey));
        }

        // Testing keys() which returns all keys.
        List<ICacheKey<String>> actualMergedKeys = new ArrayList<>();
        List<ICacheKey<String>> expectedMergedKeys = new ArrayList<>();
        expectedMergedKeys.addAll(onHeapKeys);
        expectedMergedKeys.addAll(diskTierKeys);

        Iterable<ICacheKey<String>> mergedIterable = tieredSpilloverCache.keys();
        mergedIterable.iterator().forEachRemaining(actualMergedKeys::add);

        assertEquals(expectedMergedKeys.size(), actualMergedKeys.size());
        for (ICacheKey<String> key : expectedMergedKeys) {
            assertTrue(actualMergedKeys.contains(key));
        }
    }

    public void testRefresh() {
        int diskCacheSize = randomIntBetween(60, 100);

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            50,
            diskCacheSize,
            removalListener,
            Settings.EMPTY,
            0
        );
        tieredSpilloverCache.refresh();
    }

    public void testInvalidateAll() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int keyValueSize = 50;
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );
        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            if (iter > (onHeapCacheSize - 1)) {
                // All these are bound to go to disk based cache.
                diskTierKeys.add(key);
            } else {
                onHeapKeys.add(key);
            }
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(numOfItems1, tieredSpilloverCache.count());
        tieredSpilloverCache.invalidateAll();
        assertEquals(0, tieredSpilloverCache.count());
    }

    public void testComputeIfAbsentConcurrently() throws Exception {
        int onHeapCacheSize = randomIntBetween(100, 300);
        int diskCacheSize = randomIntBetween(200, 400);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        Settings settings = Settings.builder()
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSize * keyValueSize + "b"
            )
            .build();

        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            settings,
            0
        );

        int numberOfSameKeys = randomIntBetween(10, onHeapCacheSize - 1);
        ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
        String value = UUID.randomUUID().toString();

        Thread[] threads = new Thread[numberOfSameKeys];
        Phaser phaser = new Phaser(numberOfSameKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfSameKeys); // To wait for all threads to finish.

        List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numberOfSameKeys; i++) {
            threads[i] = new Thread(() -> {
                try {
                    LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded = false;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(ICacheKey<String> key) {
                            isLoaded = true;
                            return value;
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await(); // Wait for rest of tasks to be cancelled.
        int numberOfTimesKeyLoaded = 0;
        assertEquals(numberOfSameKeys, loadAwareCacheLoaderList.size());
        for (int i = 0; i < loadAwareCacheLoaderList.size(); i++) {
            LoadAwareCacheLoader<ICacheKey<String>, String> loader = loadAwareCacheLoaderList.get(i);
            if (loader.isLoaded()) {
                numberOfTimesKeyLoaded++;
            }
        }
        assertEquals(1, numberOfTimesKeyLoaded); // It should be loaded only once.
        // We should see only one heap miss, and the rest hits
        assertEquals(1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(numberOfSameKeys - 1, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
    }

    public void testConcurrencyForEvictionFlowFromOnHeapToDiskTier() throws Exception {
        int diskCacheSize = randomIntBetween(450, 800);

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        ICache.Factory diskCacheFactory = new MockDiskCache.MockDiskCacheFactory(500, diskCacheSize, false);
        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> 150)
            .setRemovalListener(removalListener)
            .setKeySerializer(new StringSerializer())
            .setValueSerializer(new StringSerializer())
            .setSettings(
                Settings.builder()
                    .put(
                        CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
                    )
                    .put(FeatureFlags.PLUGGABLE_CACHE, "true")
                    .put(
                        OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                            .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                            .getKey(),
                        200 + "b"
                    )
                    .build()
            )
            .setClusterSettings(clusterSettings)
            .setDimensionNames(dimensionNames)
            .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(diskCacheFactory)
            .setRemovalListener(removalListener)
            .setCacheConfig(cacheConfig)
            .setCacheType(CacheType.INDICES_REQUEST_CACHE)
            .build();

        ICacheKey<String> keyToBeEvicted = getICacheKey("key1");
        ICacheKey<String> secondKey = getICacheKey("key2");

        // Put first key on tiered cache. Will go into onHeap cache.
        tieredSpilloverCache.computeIfAbsent(keyToBeEvicted, getLoadAwareCacheLoader());
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountDownLatch countDownLatch1 = new CountDownLatch(1);
        // Put second key on tiered cache. Will cause eviction of first key from onHeap cache and should go into
        // disk cache.
        LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
        Thread thread = new Thread(() -> {
            try {
                tieredSpilloverCache.computeIfAbsent(secondKey, loadAwareCacheLoader);
                countDownLatch1.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        assertBusy(() -> { assertTrue(loadAwareCacheLoader.isLoaded()); }, 100, TimeUnit.MILLISECONDS); // We wait for new key to be loaded
        // after which it eviction flow is
        // guaranteed to occur.
        ICache<String, String> onDiskCache = tieredSpilloverCache.getDiskCache();

        // Now on a different thread, try to get key(above one which got evicted) from tiered cache. We expect this
        // should return not null value as it should be present on diskCache.
        AtomicReference<String> actualValue = new AtomicReference<>();
        Thread thread1 = new Thread(() -> {
            try {
                actualValue.set(tieredSpilloverCache.get(keyToBeEvicted));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            countDownLatch.countDown();
        });
        thread1.start();
        countDownLatch.await();
        assertNotNull(actualValue.get());
        countDownLatch1.await();

        assertEquals(1, tieredSpilloverCache.getOnHeapCache().count());
        assertEquals(1, onDiskCache.count());

        assertEquals(1, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertNotNull(onDiskCache.get(keyToBeEvicted));
    }

    public void testDiskTierPolicies() throws Exception {
        // For policy function, allow if what it receives starts with "a" and string is even length
        ArrayList<Predicate<String>> policies = new ArrayList<>();
        policies.add(new AllowFirstLetterA());
        policies.add(new AllowEvenLengths());

        int keyValueSize = 50;
        int onHeapCacheSize = 0;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            keyValueSize,
            keyValueSize * 100,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0,
            policies
        );

        Map<String, String> keyValuePairs = new HashMap<>();
        Map<String, Boolean> expectedOutputs = new HashMap<>();
        keyValuePairs.put("key1", "abcd");
        expectedOutputs.put("key1", true);
        keyValuePairs.put("key2", "abcde");
        expectedOutputs.put("key2", false);
        keyValuePairs.put("key3", "bbc");
        expectedOutputs.put("key3", false);
        keyValuePairs.put("key4", "ab");
        expectedOutputs.put("key4", true);
        keyValuePairs.put("key5", "");
        expectedOutputs.put("key5", false);

        LoadAwareCacheLoader<ICacheKey<String>, String> loader = getLoadAwareCacheLoader(keyValuePairs);

        int expectedEvictions = 0;
        for (String key : keyValuePairs.keySet()) {
            ICacheKey<String> iCacheKey = getICacheKey(key);
            Boolean expectedOutput = expectedOutputs.get(key);
            String value = tieredSpilloverCache.computeIfAbsent(iCacheKey, loader);
            assertEquals(keyValuePairs.get(key), value);
            String result = tieredSpilloverCache.get(iCacheKey);
            if (expectedOutput) {
                // Should retrieve from disk tier if it was accepted
                assertEquals(keyValuePairs.get(key), result);
            } else {
                // Should miss as heap tier size = 0 and the policy rejected it
                assertNull(result);
                expectedEvictions++;
            }
        }

        // We expect values that were evicted from the heap tier and not allowed into the disk tier by the policy
        // to count towards total evictions
        assertEquals(keyValuePairs.size(), getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK)); // Disk tier is large enough for no evictions
        assertEquals(expectedEvictions, getTotalStatsSnapshot(tieredSpilloverCache).getEvictions());
    }

    public void testTookTimePolicyFromFactory() throws Exception {
        // Mock took time by passing this map to the policy info wrapper fn
        // The policy inspects values, not keys, so this is a map from values -> took time
        Map<String, Long> tookTimeMap = new HashMap<>();
        tookTimeMap.put("a", 10_000_000L);
        tookTimeMap.put("b", 0L);
        tookTimeMap.put("c", 99_999_999L);
        tookTimeMap.put("d", null);
        tookTimeMap.put("e", -1L);
        tookTimeMap.put("f", 8_888_888L);
        long timeValueThresholdNanos = 10_000_000L;

        Map<String, String> keyValueMap = Map.of("A", "a", "B", "b", "C", "c", "D", "d", "E", "e", "F", "f");

        // Most of setup duplicated from testComputeIfAbsentWithFactoryBasedCacheCreation()
        int onHeapCacheSize = randomIntBetween(tookTimeMap.size() + 1, tookTimeMap.size() + 30);
        int diskCacheSize = tookTimeMap.size();
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        // Set the desired settings needed to create a TieredSpilloverCache object with INDICES_REQUEST_CACHE cacheType.
        Settings settings = Settings.builder()
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_DISK_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                MockDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSize * keyValueSize + "b"
            )
            .put(
                TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(),
                new TimeValue(timeValueThresholdNanos / 1_000_000)
            )
            .build();

        ICache<String, String> tieredSpilloverICache = new TieredSpilloverCache.TieredSpilloverCacheFactory().create(
            new CacheConfig.Builder<String, String>().setKeyType(String.class)
                .setKeyType(String.class)
                .setWeigher((k, v) -> keyValueSize)
                .setRemovalListener(removalListener)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setSettings(settings)
                .setMaxSizeInBytes(onHeapCacheSize * keyValueSize)
                .setDimensionNames(dimensionNames)
                .setCachedResultParser(new Function<String, CachedQueryResult.PolicyValues>() {
                    @Override
                    public CachedQueryResult.PolicyValues apply(String s) {
                        return new CachedQueryResult.PolicyValues(tookTimeMap.get(s));
                    }
                })
                .setClusterSettings(clusterSettings)
                .build(),
            CacheType.INDICES_REQUEST_CACHE,
            Map.of(
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
                new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory(),
                MockDiskCache.MockDiskCacheFactory.NAME,
                new MockDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300), false)
            )
        );

        TieredSpilloverCache<String, String> tieredSpilloverCache = (TieredSpilloverCache<String, String>) tieredSpilloverICache;

        // First add all our values to the on heap cache
        for (String key : tookTimeMap.keySet()) {
            tieredSpilloverCache.computeIfAbsent(getICacheKey(key), getLoadAwareCacheLoader(keyValueMap));
        }
        assertEquals(tookTimeMap.size(), tieredSpilloverCache.count());

        // Ensure all these keys get evicted from the on heap tier by adding > heap tier size worth of random keys
        for (int i = 0; i < onHeapCacheSize; i++) {
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), getLoadAwareCacheLoader(keyValueMap));
        }
        ICache<String, String> onHeapCache = tieredSpilloverCache.getOnHeapCache();
        for (String key : tookTimeMap.keySet()) {
            assertNull(onHeapCache.get(getICacheKey(key)));
        }

        // Now the original keys should be in the disk tier if the policy allows them, or misses if not
        for (String key : tookTimeMap.keySet()) {
            String computedValue = tieredSpilloverCache.get(getICacheKey(key));
            String mapValue = keyValueMap.get(key);
            Long tookTime = tookTimeMap.get(mapValue);
            if (tookTime != null && tookTime > timeValueThresholdNanos) {
                // expect a hit
                assertNotNull(computedValue);
            } else {
                // expect a miss
                assertNull(computedValue);
            }
        }
    }

    public void testMinimumThresholdSettingValue() throws Exception {
        // Confirm we can't set TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_DISK_TOOK_TIME_THRESHOLD to below
        // TimeValue.ZERO (for example, MINUS_ONE)
        Setting<TimeValue> concreteSetting = TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(
            CacheType.INDICES_REQUEST_CACHE
        );
        TimeValue validDuration = new TimeValue(0, TimeUnit.MILLISECONDS);
        Settings validSettings = Settings.builder().put(concreteSetting.getKey(), validDuration).build();

        Settings belowThresholdSettings = Settings.builder().put(concreteSetting.getKey(), TimeValue.MINUS_ONE).build();

        assertThrows(IllegalArgumentException.class, () -> concreteSetting.get(belowThresholdSettings));
        assertEquals(validDuration, concreteSetting.get(validSettings));
    }

    public void testPutWithDiskCacheDisabledSetting() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .put(DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(), false)
                .build(),
            0
        );

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize); // Create more items than onHeap cache.
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
        }
        ICache<String, String> onHeapCache = tieredSpilloverCache.getOnHeapCache();
        ICache<String, String> diskCache = tieredSpilloverCache.getDiskCache();
        assertEquals(onHeapCacheSize, onHeapCache.count());
        assertEquals(0, diskCache.count()); // Disk cache shouldn't have anything considering it is disabled.
        assertEquals(numOfItems1 - onHeapCacheSize, removalListener.evictionsMetric.count());
    }

    public void testGetPutAndInvalidateWithDiskCacheDisabled() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;
        int totalSize = onHeapCacheSize + diskCacheSize;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize - 1); // Create more items than onHeap
        // cache to cause spillover.
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
        }
        ICache<String, String> onHeapCache = tieredSpilloverCache.getOnHeapCache();
        ICache<String, String> diskCache = tieredSpilloverCache.getDiskCache();
        List<ICacheKey<String>> diskCacheKeys = new ArrayList<>();
        tieredSpilloverCache.getDiskCache().keys().forEach(diskCacheKeys::add);
        long actualDiskCacheCount = diskCache.count();
        long actualTieredCacheCount = tieredSpilloverCache.count();
        assertEquals(onHeapCacheSize, onHeapCache.count());
        assertEquals(numOfItems1 - onHeapCacheSize, actualDiskCacheCount);
        assertEquals(0, removalListener.evictionsMetric.count());
        assertEquals(numOfItems1, actualTieredCacheCount);
        for (ICacheKey<String> diskKey : diskCacheKeys) {
            assertNotNull(tieredSpilloverCache.get(diskKey));
        }

        tieredSpilloverCache.enableDisableDiskCache(false); // Disable disk cache now.
        int numOfItems2 = totalSize - numOfItems1;
        for (int iter = 0; iter < numOfItems2; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
        }
        for (ICacheKey<String> diskKey : diskCacheKeys) {
            assertNull(tieredSpilloverCache.get(diskKey)); // Considering disk cache is disabled, we shouldn't find
            // these keys.
        }
        assertEquals(onHeapCacheSize, onHeapCache.count()); // Should remain same.
        assertEquals(0, diskCache.count() - actualDiskCacheCount); // Considering it is disabled now, shouldn't cache
        // any more items.
        assertEquals(numOfItems2, removalListener.evictionsMetric.count()); // Considering onHeap cache was already
        // full, we should all existing onHeap entries being evicted.
        assertEquals(0, tieredSpilloverCache.count() - actualTieredCacheCount); // Count still returns disk cache
        // entries count as they haven't been cleared yet.
        long lastKnownTieredCacheEntriesCount = tieredSpilloverCache.count();

        // Clear up disk cache keys.
        for (ICacheKey<String> diskKey : diskCacheKeys) {
            tieredSpilloverCache.invalidate(diskKey);
        }
        assertEquals(0, diskCache.count());
        assertEquals(lastKnownTieredCacheEntriesCount - diskCacheKeys.size(), tieredSpilloverCache.count());

        tieredSpilloverCache.invalidateAll(); // Clear up all the keys.
        assertEquals(0, tieredSpilloverCache.count());
    }

    public void testTiersDoNotTrackStats() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );

        // do some gets to put entries in both tiers
        int numMisses = onHeapCacheSize + randomIntBetween(10, 20);
        for (int iter = 0; iter < numMisses; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertNotEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), tieredSpilloverCache.stats().getTotalStats());
        assertEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), tieredSpilloverCache.getOnHeapCache().stats().getTotalStats());
        ImmutableCacheStats diskStats = tieredSpilloverCache.getDiskCache().stats().getTotalStats();
        assertEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), diskStats);
    }

    public void testTierStatsAddCorrectly() throws Exception {
        /* We expect the total stats to be:
         * totalHits = heapHits + diskHits
         * totalMisses = diskMisses
         * totalEvictions = diskEvictions
         * totalSize = heapSize + diskSize
         * totalEntries = heapEntries + diskEntries
         */

        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            Settings.builder()
                .put(
                    OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                        .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                        .getKey(),
                    onHeapCacheSize * keyValueSize + "b"
                )
                .build(),
            0
        );

        List<ICacheKey<String>> usedKeys = new ArrayList<>();
        // Fill the cache, getting some entries + evictions for both tiers
        int numMisses = onHeapCacheSize + diskCacheSize + randomIntBetween(10, 20);
        for (int iter = 0; iter < numMisses; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            usedKeys.add(key);
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        // Also do some random hits
        Random rand = Randomness.get();
        int approxNumHits = 30;
        for (int i = 0; i < approxNumHits; i++) {
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            ICacheKey<String> key = usedKeys.get(rand.nextInt(usedKeys.size()));
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }

        ImmutableCacheStats totalStats = tieredSpilloverCache.stats().getTotalStats();
        ImmutableCacheStats heapStats = getStatsSnapshotForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP);
        ImmutableCacheStats diskStats = getStatsSnapshotForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK);

        assertEquals(totalStats.getHits(), heapStats.getHits() + diskStats.getHits());
        assertEquals(totalStats.getMisses(), diskStats.getMisses());
        assertEquals(totalStats.getEvictions(), diskStats.getEvictions());
        assertEquals(totalStats.getSizeInBytes(), heapStats.getSizeInBytes() + diskStats.getSizeInBytes());
        assertEquals(totalStats.getItems(), heapStats.getItems() + diskStats.getItems());

        // Also check the heap stats don't have zero misses or evictions
        assertNotEquals(0, heapStats.getMisses());
        assertNotEquals(0, heapStats.getEvictions());

        // Now turn off the disk tier and do more misses and evictions from the heap tier.
        // These should be added to the totals, as the disk tier is now absent
        long missesBeforeDisablingDiskCache = totalStats.getMisses();
        long evictionsBeforeDisablingDiskCache = totalStats.getEvictions();
        long heapTierEvictionsBeforeDisablingDiskCache = heapStats.getEvictions();

        clusterSettings.applySettings(
            Settings.builder().put(DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(), false).build()
        );

        int newMisses = randomIntBetween(10, 30);
        for (int i = 0; i < newMisses; i++) {
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), tieredCacheLoader);
        }

        totalStats = tieredSpilloverCache.stats().getTotalStats();
        heapStats = getStatsSnapshotForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP);
        assertEquals(missesBeforeDisablingDiskCache + newMisses, totalStats.getMisses());
        assertEquals(heapTierEvictionsBeforeDisablingDiskCache + newMisses, heapStats.getEvictions());
        assertEquals(evictionsBeforeDisablingDiskCache + newMisses, totalStats.getEvictions());

        // Turn the disk cache back on in cluster settings for other tests
        clusterSettings.applySettings(
            Settings.builder().put(DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(), true).build()
        );

    }

    private List<String> getMockDimensions() {
        List<String> dims = new ArrayList<>();
        for (String dimensionName : dimensionNames) {
            dims.add("0");
        }
        return dims;
    }

    private ICacheKey<String> getICacheKey(String key) {
        return new ICacheKey<>(key, getMockDimensions());
    }

    class MockCacheRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {
        final CounterMetric evictionsMetric = new CounterMetric();

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            evictionsMetric.inc();
        }
    }

    private static class AllowFirstLetterA implements Predicate<String> {
        @Override
        public boolean test(String data) {
            try {
                return (data.charAt(0) == 'a');
            } catch (StringIndexOutOfBoundsException e) {
                return false;
            }
        }
    }

    private static class AllowEvenLengths implements Predicate<String> {
        @Override
        public boolean test(String data) {
            return data.length() % 2 == 0;
        }
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

    private LoadAwareCacheLoader<ICacheKey<String>, String> getLoadAwareCacheLoader(Map<String, String> keyValueMap) {
        return new LoadAwareCacheLoader<>() {
            boolean isLoaded = false;

            @Override
            public String load(ICacheKey<String> key) {
                isLoaded = true;
                String mapValue = keyValueMap.get(key.key);
                if (mapValue == null) {
                    mapValue = UUID.randomUUID().toString();
                }
                return mapValue;
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }

    private TieredSpilloverCache<String, String> initializeTieredSpilloverCache(
        int keyValueSize,
        int diskCacheSize,
        RemovalListener<ICacheKey<String>, String> removalListener,
        Settings settings,
        long diskDeliberateDelay

    ) {
        return intializeTieredSpilloverCache(keyValueSize, diskCacheSize, removalListener, settings, diskDeliberateDelay, null);
    }

    private TieredSpilloverCache<String, String> intializeTieredSpilloverCache(
        int keyValueSize,
        int diskCacheSize,
        RemovalListener<ICacheKey<String>, String> removalListener,
        Settings settings,
        long diskDeliberateDelay,
        List<Predicate<String>> policies
    ) {
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setSettings(settings)
            .setDimensionNames(dimensionNames)
            .setRemovalListener(removalListener)
            .setKeySerializer(new StringSerializer())
            .setValueSerializer(new StringSerializer())
            .setSettings(
                Settings.builder()
                    .put(
                        CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                        TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
                    )
                    .put(FeatureFlags.PLUGGABLE_CACHE, "true")
                    .put(settings)
                    .build()
            )
            .setClusterSettings(clusterSettings)
            .build();
        ICache.Factory mockDiskCacheFactory = new MockDiskCache.MockDiskCacheFactory(diskDeliberateDelay, diskCacheSize, false);

        TieredSpilloverCache.Builder<String, String> builder = new TieredSpilloverCache.Builder<String, String>().setCacheType(
            CacheType.INDICES_REQUEST_CACHE
        )
            .setRemovalListener(removalListener)
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(mockDiskCacheFactory)
            .setCacheConfig(cacheConfig);
        if (policies != null) {
            builder.addPolicies(policies);
        }
        return builder.build();
    }

    // Helper functions for extracting tier aggregated stats.
    private long getHitsForTier(TieredSpilloverCache<?, ?> tsc, String tierValue) throws IOException {
        return getStatsSnapshotForTier(tsc, tierValue).getHits();
    }

    private long getMissesForTier(TieredSpilloverCache<?, ?> tsc, String tierValue) throws IOException {
        return getStatsSnapshotForTier(tsc, tierValue).getMisses();
    }

    private long getEvictionsForTier(TieredSpilloverCache<?, ?> tsc, String tierValue) throws IOException {
        return getStatsSnapshotForTier(tsc, tierValue).getEvictions();
    }

    private long getSizeInBytesForTier(TieredSpilloverCache<?, ?> tsc, String tierValue) throws IOException {
        return getStatsSnapshotForTier(tsc, tierValue).getSizeInBytes();
    }

    private long getItemsForTier(TieredSpilloverCache<?, ?> tsc, String tierValue) throws IOException {
        return getStatsSnapshotForTier(tsc, tierValue).getItems();
    }

    private ImmutableCacheStats getStatsSnapshotForTier(TieredSpilloverCache<?, ?> tsc, String tierValue) throws IOException {
        List<String> levelsList = new ArrayList<>(dimensionNames);
        levelsList.add(TIER_DIMENSION_NAME);
        String[] levels = levelsList.toArray(new String[0]);
        ImmutableCacheStatsHolder cacheStats = tsc.stats(levels);
        // Since we always use the same list of dimensions from getMockDimensions() in keys for these tests, we can get all the stats values
        // for a given tier with a single node in MDCS
        List<String> mockDimensions = getMockDimensions();
        mockDimensions.add(tierValue);
        ImmutableCacheStats snapshot = cacheStats.getStatsForDimensionValues(mockDimensions);
        if (snapshot == null) {
            return new ImmutableCacheStats(0, 0, 0, 0, 0); // This can happen if no cache actions have happened for this set of
            // dimensions yet
        }
        return snapshot;
    }

    // Duplicated here from EhcacheDiskCacheTests.java, we can't add a dependency on that plugin
    static class StringSerializer implements Serializer<String, byte[]> {
        private final Charset charset = StandardCharsets.UTF_8;

        @Override
        public byte[] serialize(String object) {
            return object.getBytes(charset);
        }

        @Override
        public String deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return new String(bytes, charset);
        }

        public boolean equals(String object, byte[] bytes) {
            return object.equals(deserialize(bytes));
        }
    }

    private ImmutableCacheStats getTotalStatsSnapshot(TieredSpilloverCache<?, ?> tsc) throws IOException {
        ImmutableCacheStatsHolder cacheStats = tsc.stats(new String[0]);
        return cacheStats.getStatsForDimensionValues(List.of());
    }
}
