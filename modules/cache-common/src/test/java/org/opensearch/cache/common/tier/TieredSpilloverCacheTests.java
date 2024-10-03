/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.OpenSearchException;
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
import org.opensearch.env.NodeEnvironment;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.cache.common.tier.TieredSpilloverCache.ZERO_SEGMENT_COUNT_EXCEPTION_MESSAGE;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.DISK_CACHE_ENABLED_SETTING_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TIERED_SPILLOVER_SEGMENTS;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_NAME;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_DISK;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStatsHolder.TIER_DIMENSION_VALUE_ON_HEAP;
import static org.opensearch.common.cache.settings.CacheSettings.INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE;
import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            0,
            1
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
            .put(TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()).getKey(), 1)
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();
        String storagePath = getStoragePath(settings);
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
                .setStoragePath(storagePath)
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
        tieredSpilloverCache.tieredSpilloverCacheSegments[0].getOnHeapCache().keys().forEach(onHeapKeys::add);
        tieredSpilloverCache.tieredSpilloverCacheSegments[0].getDiskCache().keys().forEach(diskTierKeys::add);
        // Verify on heap cache stats.
        assertEquals(onHeapCacheSize, tieredSpilloverCache.tieredSpilloverCacheSegments[0].getOnHeapCache().count());
        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(expectedDiskEntries, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));

        // Verify disk cache stats.
        assertEquals(expectedDiskEntries, tieredSpilloverCache.tieredSpilloverCacheSegments[0].getDiskCache().count());
        assertEquals(expectedDiskEntries, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(expectedDiskEntries * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
    }

    public void testComputeIfAbsentWithSegmentedCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(300, 600);
        int diskCacheSize = randomIntBetween(700, 1200);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int numberOfSegments = getNumberOfSegments();
        int keyValueSize = 11;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        int onHeapCacheSizeInBytes = onHeapCacheSize * keyValueSize;
        Map<Integer, Integer> expectedSegmentOnHeapCacheSize = getSegmentOnHeapCacheSize(
            numberOfSegments,
            onHeapCacheSizeInBytes,
            keyValueSize
        );
        int totalOnHeapEntries = 0;
        int totalOnDiskEntries = 0;
        // Set the desired settings needed to create a TieredSpilloverCache object with INDICES_REQUEST_CACHE cacheType.
        Settings settings = Settings.builder()
            .put(
                TieredSpilloverCacheSettings.TIERED_SPILLOVER_ONHEAP_STORE_NAME.getConcreteSettingForNamespace(
                    CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()
                ).getKey(),
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME
            )
            .put(
                TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()).getKey(),
                numberOfSegments
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
                onHeapCacheSizeInBytes + "b"
            )
            .put(
                CacheSettings.getConcreteStoreNameSettingForCacheType(CacheType.INDICES_REQUEST_CACHE).getKey(),
                TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
            )
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .build();
        String storagePath = getStoragePath(settings);
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
                .setStoragePath(storagePath)
                .setSegmentCount(numberOfSegments)
                .build(),
            CacheType.INDICES_REQUEST_CACHE,
            Map.of(
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
                new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory(),
                MockDiskCache.MockDiskCacheFactory.NAME,
                // Make disk cache big enough to hold all entries
                new MockDiskCache.MockDiskCacheFactory(0, diskCacheSize * 500, false)
            )
        );
        TieredSpilloverCache<String, String> tieredSpilloverCache = (TieredSpilloverCache<String, String>) tieredSpilloverICache;
        TieredSpilloverCache.TieredSpilloverCacheSegment<String, String>[] tieredSpilloverCacheSegments =
            tieredSpilloverCache.tieredSpilloverCacheSegments;
        assertEquals(numberOfSegments, tieredSpilloverCacheSegments.length);

        Map<ICacheKey<String>, TieredSpilloverCache.TieredSpilloverCacheSegment<String, String>> tieredSpilloverCacheSegmentMap =
            new HashMap<>();

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        Map<Integer, Integer> expectedNumberOfEntriesInSegment = new HashMap<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            ICacheKey<String> iCacheKey = getICacheKey(key);
            int keySegment = tieredSpilloverCache.getSegmentNumber(iCacheKey);
            if (expectedNumberOfEntriesInSegment.get(keySegment) == null) {
                expectedNumberOfEntriesInSegment.put(keySegment, Integer.valueOf(1));
            } else {
                Integer updatedValue = expectedNumberOfEntriesInSegment.get(keySegment) + 1;
                expectedNumberOfEntriesInSegment.put(keySegment, updatedValue);
            }
            tieredSpilloverCacheSegmentMap.put(iCacheKey, tieredSpilloverCache.getTieredCacheSegment(iCacheKey));
            tieredSpilloverCache.computeIfAbsent(iCacheKey, tieredCacheLoader);
        }

        // We now calculate expected onHeap cache entries and then verify it later.
        for (int i = 0; i < numberOfSegments; i++) {
            if (expectedNumberOfEntriesInSegment.get(i) == null) {
                continue;
            }
            if (expectedNumberOfEntriesInSegment.get(i) >= expectedSegmentOnHeapCacheSize.get(i)) {
                totalOnHeapEntries += expectedSegmentOnHeapCacheSize.get(i);
                totalOnDiskEntries += expectedNumberOfEntriesInSegment.get(i) - expectedSegmentOnHeapCacheSize.get(i);
            } else {
                // In this case onHeap cache wasn't utilized fully.
                totalOnHeapEntries += expectedNumberOfEntriesInSegment.get(i);
            }
        }

        tieredSpilloverCache.getOnHeapCacheKeys().forEach(onHeapKeys::add);
        tieredSpilloverCache.getDiskCacheKeys().forEach(diskTierKeys::add);
        // Verify on heap cache stats.
        assertEquals(totalOnHeapEntries, tieredSpilloverCache.onHeapCacheCount());
        assertEquals(totalOnHeapEntries, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(totalOnDiskEntries, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(totalOnHeapEntries * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));

        // Verify disk cache stats.
        assertEquals(totalOnDiskEntries, tieredSpilloverCache.diskCacheCount());
        assertEquals(totalOnDiskEntries, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(0, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(totalOnDiskEntries * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));

        // Verify the keys for onHeap and disk cache

        for (ICacheKey<String> key : onHeapKeys) {
            assertNotNull(tieredSpilloverCache.get(key));
        }
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapKeys.size(), getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        for (ICacheKey<String> key : diskTierKeys) {
            assertNotNull(tieredSpilloverCache.get(key));
        }
        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(diskTierKeys.size(), getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
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

        Settings settings = Settings.builder()
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
            .build();
        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setRemovalListener(removalListener)
            .setKeySerializer(new StringSerializer())
            .setValueSerializer(new StringSerializer())
            .setDimensionNames(dimensionNames)
            .setSettings(settings)
            .setStoragePath(getStoragePath(settings))
            .setClusterSettings(clusterSettings)
            .build();

        ICache.Factory mockDiskCacheFactory = new MockDiskCache.MockDiskCacheFactory(0, diskCacheSize, false);

        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(mockDiskCacheFactory)
            .setCacheConfig(cacheConfig)
            .setRemovalListener(removalListener)
            .setCacheType(CacheType.INDICES_REQUEST_CACHE)
            .setNumberOfSegments(1)
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

        long actualDiskCacheSize = tieredSpilloverCache.diskCacheCount();

        assertEquals(numOfItems1, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(actualDiskCacheSize, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(onHeapCacheSize * keyValueSize, getSizeInBytesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(actualDiskCacheSize, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));

        tieredSpilloverCache.getOnHeapCacheKeys().forEach(onHeapKeys::add);
        tieredSpilloverCache.getDiskCacheKeys().forEach(diskTierKeys::add);

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
        assertEquals(0, tieredSpilloverCache.completableFutureMap.size());
    }

    public void testComputeIfAbsentWithEvictionsFromTieredCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(300, 600);
        int diskCacheSize = randomIntBetween(700, 1200);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int numberOfSegments = getNumberOfSegments();
        int keyValueSize = 11;

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
            0,
            numberOfSegments
        );
        Map<Integer, Integer> onHeapCacheExpectedSize = getSegmentOnHeapCacheSize(
            numberOfSegments,
            onHeapCacheSize * keyValueSize,
            keyValueSize
        );
        Map<Integer, Integer> mockDiskCacheExpectedSize = getSegmentMockDiskCacheSize(numberOfSegments, diskCacheSize);
        Map<Integer, Integer> perSegmentEntryCapacity = new HashMap<>();
        for (int i = 0; i < numberOfSegments; i++) {
            int totalEntriesForSegment = onHeapCacheExpectedSize.get(i) + mockDiskCacheExpectedSize.get(i);
            perSegmentEntryCapacity.put(i, totalEntriesForSegment);
        }
        int numOfItems = randomIntBetween(totalSize + 1, totalSize * 3);
        Map<Integer, Integer> segmentSizeTracker = new HashMap<>();
        int expectedEvictions = 0;
        for (int iter = 0; iter < numOfItems; iter++) {
            ICacheKey<String> iCacheKey = getICacheKey(UUID.randomUUID().toString());
            int keySegment = tieredSpilloverCache.getSegmentNumber(iCacheKey);
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(iCacheKey, tieredCacheLoader);
            if (segmentSizeTracker.get(keySegment) == null) {
                segmentSizeTracker.put(keySegment, Integer.valueOf(1));
            } else {
                Integer updatedValue = segmentSizeTracker.get(keySegment) + 1;
                segmentSizeTracker.put(keySegment, updatedValue);
            }
        }
        for (int i = 0; i < numberOfSegments; i++) {
            if (segmentSizeTracker.get(i) == null) {
                continue;
            }
            if (segmentSizeTracker.get(i) > perSegmentEntryCapacity.get(i)) {
                expectedEvictions += segmentSizeTracker.get(i) - perSegmentEntryCapacity.get(i);
            }
        }
        assertEquals(expectedEvictions, removalListener.evictionsMetric.count());
        assertEquals(expectedEvictions, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertEquals(
            expectedEvictions + getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK),
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
            0,
            1
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
        int numberOfSegments = getNumberOfSegments();
        int onHeapCacheSize = randomIntBetween(10, 30) * numberOfSegments;
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, onHeapCacheSize * 2);
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
            0,
            numberOfSegments
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
        int numberOfSegments = 1;
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
            0,
            numberOfSegments
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
        tieredSpilloverCache.getOnHeapCacheKeys().forEach(actualOnHeapCacheKeys::add);

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
            0,
            1
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
            0,
            1
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
        Iterable<ICacheKey<String>> onHeapiterable = tieredSpilloverCache.getOnHeapCacheKeys();
        Iterable<ICacheKey<String>> onDiskiterable = tieredSpilloverCache.getDiskCacheKeys();
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
            0,
            1
        );
        tieredSpilloverCache.refresh();
    }

    public void testInvalidateAll() throws Exception {
        int onHeapCacheSize = randomIntBetween(300, 600);
        int diskCacheSize = randomIntBetween(700, 1200);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int numberOfSegments = getNumberOfSegments();
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
            0,
            numberOfSegments
        );
        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(
            getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP) + getItemsForTier(
                tieredSpilloverCache,
                TIER_DIMENSION_VALUE_DISK
            ),
            tieredSpilloverCache.count()
        );
        tieredSpilloverCache.invalidateAll();
        assertEquals(0, tieredSpilloverCache.count());
    }

    public void testComputeIfAbsentConcurrently() throws Exception {
        int onHeapCacheSize = randomIntBetween(500, 700);
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
            0,
            1
        );

        int numberOfSameKeys = randomIntBetween(400, onHeapCacheSize - 1);
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
                    assertEquals(value, tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
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
        assertEquals(0, tieredSpilloverCache.completableFutureMap.size());
    }

    public void testComputIfAbsentConcurrentlyWithMultipleKeys() throws Exception {
        int numberOfSegments = getNumberOfSegments();
        int onHeapCacheSize = randomIntBetween(300, 500) * numberOfSegments; // Able to support all keys in case of
        // skewness as well.
        int diskCacheSize = randomIntBetween(600, 700);
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
            0,
            numberOfSegments
        );

        int iterations = 10;
        int numberOfKeys = 20;
        List<ICacheKey<String>> iCacheKeyList = new ArrayList<>();
        for (int i = 0; i < numberOfKeys; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            iCacheKeyList.add(key);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        CountDownLatch countDownLatch = new CountDownLatch(iterations * numberOfKeys); // To wait for all threads to finish.

        List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();
        for (int j = 0; j < numberOfKeys; j++) {
            int finalJ = j;
            for (int i = 0; i < iterations; i++) {
                executorService.submit(() -> {
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
                                return iCacheKeyList.get(finalJ).key;
                            }
                        };
                        loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                        tieredSpilloverCache.computeIfAbsent(iCacheKeyList.get(finalJ), loadAwareCacheLoader);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        countDownLatch.await();
        int numberOfTimesKeyLoaded = 0;
        assertEquals(iterations * numberOfKeys, loadAwareCacheLoaderList.size());
        for (int i = 0; i < loadAwareCacheLoaderList.size(); i++) {
            LoadAwareCacheLoader<ICacheKey<String>, String> loader = loadAwareCacheLoaderList.get(i);
            if (loader.isLoaded()) {
                numberOfTimesKeyLoaded++;
            }
        }
        assertEquals(numberOfKeys, numberOfTimesKeyLoaded); // It should be loaded only once.
        // We should see only one heap miss, and the rest hits
        assertEquals(numberOfKeys, getMissesForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals((iterations * numberOfKeys) - numberOfKeys, getHitsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(0, tieredSpilloverCache.completableFutureMap.size());
        executorService.shutdownNow();
    }

    public void testComputeIfAbsentConcurrentlyAndThrowsException() throws Exception {
        LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
            boolean isLoaded = false;

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }

            @Override
            public String load(ICacheKey<String> key) {
                throw new RuntimeException("Testing");
            }
        };
        verifyComputeIfAbsentThrowsException(RuntimeException.class, loadAwareCacheLoader, "Testing");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testComputeIfAbsentWithOnHeapCacheThrowingExceptionOnPut() throws Exception {
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
        ICache.Factory onHeapCacheFactory = mock(OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.class);
        ICache mockOnHeapCache = mock(ICache.class);
        when(onHeapCacheFactory.create(any(), any(), any())).thenReturn(mockOnHeapCache);
        doThrow(new RuntimeException("Testing")).when(mockOnHeapCache).put(any(), any());
        CacheConfig<String, String> cacheConfig = getCacheConfig(keyValueSize, settings, removalListener, 1);
        ICache.Factory mockDiskCacheFactory = new MockDiskCache.MockDiskCacheFactory(0, diskCacheSize, false);

        TieredSpilloverCache<String, String> tieredSpilloverCache = getTieredSpilloverCache(
            onHeapCacheFactory,
            mockDiskCacheFactory,
            cacheConfig,
            null,
            removalListener,
            1
        );
        String value = "";
        value = tieredSpilloverCache.computeIfAbsent(getICacheKey("test"), new LoadAwareCacheLoader<>() {
            @Override
            public boolean isLoaded() {
                return false;
            }

            @Override
            public String load(ICacheKey<String> key) {
                return "test";
            }
        });
        assertEquals("test", value);
        assertEquals(0, tieredSpilloverCache.completableFutureMap.size());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testComputeIfAbsentWithDiskCacheThrowingExceptionOnPut() throws Exception {
        int onHeapCacheSize = 1;
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
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        CacheConfig<String, String> cacheConfig = getCacheConfig(keyValueSize, settings, removalListener, 1);
        ICache.Factory mockDiskCacheFactory = mock(MockDiskCache.MockDiskCacheFactory.class);
        ICache mockDiskCache = mock(ICache.class);
        when(mockDiskCacheFactory.create(any(), any(), any())).thenReturn(mockDiskCache);
        doThrow(new RuntimeException("Test")).when(mockDiskCache).put(any(), any());

        TieredSpilloverCache<String, String> tieredSpilloverCache = getTieredSpilloverCache(
            onHeapCacheFactory,
            mockDiskCacheFactory,
            cacheConfig,
            null,
            removalListener,
            1
        );

        String response = "";
        // This first computeIfAbsent ensures onHeap cache has 1 item present and rest will be evicted to disk.
        tieredSpilloverCache.computeIfAbsent(getICacheKey("test1"), new LoadAwareCacheLoader<>() {
            @Override
            public boolean isLoaded() {
                return false;
            }

            @Override
            public String load(ICacheKey<String> key) {
                return "test1";
            }
        });
        response = tieredSpilloverCache.computeIfAbsent(getICacheKey("test"), new LoadAwareCacheLoader<>() {
            @Override
            public boolean isLoaded() {
                return false;
            }

            @Override
            public String load(ICacheKey<String> key) {
                return "test";
            }
        });
        ImmutableCacheStats diskStats = getStatsSnapshotForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK);

        assertEquals(0, diskStats.getSizeInBytes());
        assertEquals(1, removalListener.evictionsMetric.count());
        assertEquals("test", response);
        assertEquals(0, tieredSpilloverCache.completableFutureMap.size());
    }

    public void testComputeIfAbsentConcurrentlyWithLoaderReturningNull() throws Exception {
        LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
            boolean isLoaded = false;

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }

            @Override
            public String load(ICacheKey<String> key) {
                return null;
            }
        };
        verifyComputeIfAbsentThrowsException(NullPointerException.class, loadAwareCacheLoader, "Loader returned a null value");
    }

    public void testConcurrencyForEvictionFlowFromOnHeapToDiskTier() throws Exception {
        int diskCacheSize = randomIntBetween(450, 800);

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        ICache.Factory diskCacheFactory = new MockDiskCache.MockDiskCacheFactory(500, diskCacheSize, false);

        Settings settings = Settings.builder()
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
            .build();
        String storagePath;
        try (NodeEnvironment environment = newNodeEnvironment(settings)) {
            storagePath = environment.nodePaths()[0].path + "/test";
        } catch (IOException e) {
            throw new OpenSearchException("Exception occurred", e);
        }

        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> 150)
            .setRemovalListener(removalListener)
            .setKeySerializer(new StringSerializer())
            .setValueSerializer(new StringSerializer())
            .setSettings(settings)
            .setClusterSettings(clusterSettings)
            .setStoragePath(storagePath)
            .setDimensionNames(dimensionNames)
            .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(diskCacheFactory)
            .setRemovalListener(removalListener)
            .setCacheConfig(cacheConfig)
            .setNumberOfSegments(1)
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
        // after which it eviction flow is guaranteed to occur.

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

        assertEquals(1, tieredSpilloverCache.onHeapCacheCount());
        assertEquals(1, tieredSpilloverCache.diskCacheCount());

        assertEquals(1, getEvictionsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP));
        assertEquals(1, getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK));
        assertNotNull(tieredSpilloverCache.getTieredCacheSegment(keyToBeEvicted).getDiskCache().get(keyToBeEvicted));
    }

    public void testDiskTierPolicies() throws Exception {
        // For policy function, allow if what it receives starts with "a" and string is even length
        ArrayList<Predicate<String>> policies = new ArrayList<>();
        policies.add(new AllowFirstLetterA());
        policies.add(new AllowEvenLengths());

        int keyValueSize = 50;
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
                    keyValueSize - 1 + "b"
                )
                .build(),
            0,
            policies,
            1
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
            .put(TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()).getKey(), 1)
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
                .setStoragePath(getStoragePath(settings))
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

        // Ensure all these keys get evicted from the on heap tier by adding > heap tier size worth of random keys (this works as we have 1
        // segment)
        for (int i = 0; i < onHeapCacheSize; i++) {
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), getLoadAwareCacheLoader(keyValueMap));
        }
        for (String key : tookTimeMap.keySet()) {
            ICacheKey<String> iCacheKey = getICacheKey(key);
            assertNull(tieredSpilloverCache.getTieredCacheSegment(iCacheKey).getOnHeapCache().get(iCacheKey));
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
        int diskCacheSize = randomIntBetween(300, 500);
        int keyValueSize = 50;
        int totalSize = onHeapCacheSize + diskCacheSize;
        int numberOfSegments = getNumberOfSegments();
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
            0,
            numberOfSegments
        );

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize); // Create more items than onHeap cache.
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
        }

        assertEquals(getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP), tieredSpilloverCache.onHeapCacheCount());
        assertEquals(0, tieredSpilloverCache.diskCacheCount()); // Disk cache shouldn't have anything considering it is
        // disabled.
        assertEquals(numOfItems1 - tieredSpilloverCache.onHeapCacheCount(), removalListener.evictionsMetric.count());
    }

    public void testGetPutAndInvalidateWithDiskCacheDisabled() throws Exception {
        int onHeapCacheSize = randomIntBetween(300, 400);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 500);
        int keyValueSize = 12;
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        int numberOfSegments = getNumberOfSegments();
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
            0,
            numberOfSegments
        );
        Map<Integer, Integer> onHeapCacheExpectedSize = getSegmentOnHeapCacheSize(
            numberOfSegments,
            onHeapCacheSize * keyValueSize,
            keyValueSize
        );
        Map<Integer, Integer> mockDiskCacheExpectedSize = getSegmentMockDiskCacheSize(numberOfSegments, diskCacheSize);
        Map<Integer, Integer> perSegmentEntryCapacity = new HashMap<>();
        for (int i = 0; i < numberOfSegments; i++) {
            int totalEntriesForSegment = onHeapCacheExpectedSize.get(i) + mockDiskCacheExpectedSize.get(i);
            perSegmentEntryCapacity.put(i, totalEntriesForSegment);
        }
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize - 1); // Create more items than onHeap
        // cache to cause spillover.
        Map<Integer, Integer> segmentSizeTracker = new HashMap<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            int keySegment = tieredSpilloverCache.getSegmentNumber(key);
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
            if (segmentSizeTracker.get(keySegment) == null) {
                segmentSizeTracker.put(keySegment, Integer.valueOf(1));
            } else {
                Integer updatedValue = segmentSizeTracker.get(keySegment) + 1;
                segmentSizeTracker.put(keySegment, updatedValue);
            }
        }
        int expectedEvictions = 0;
        for (int i = 0; i < numberOfSegments; i++) {
            if (segmentSizeTracker.get(i) == null) {
                continue;
            }
            if (segmentSizeTracker.get(i) > perSegmentEntryCapacity.get(i)) {
                expectedEvictions += segmentSizeTracker.get(i) - perSegmentEntryCapacity.get(i);
            }
        }
        List<ICacheKey<String>> diskCacheKeys = new ArrayList<>();
        tieredSpilloverCache.getDiskCacheKeys().forEach(diskCacheKeys::add);
        long actualDiskCacheCount = tieredSpilloverCache.diskCacheCount();
        long actualTieredCacheCount = tieredSpilloverCache.count();
        assertEquals(getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP), tieredSpilloverCache.onHeapCacheCount());
        assertEquals(getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_DISK), actualDiskCacheCount);
        assertEquals(expectedEvictions, removalListener.evictionsMetric.count());
        assertEquals(
            getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP) + getItemsForTier(
                tieredSpilloverCache,
                TIER_DIMENSION_VALUE_DISK
            ),
            actualTieredCacheCount
        );
        for (ICacheKey<String> diskKey : diskCacheKeys) {
            assertNotNull(tieredSpilloverCache.get(diskKey));
        }
        tieredSpilloverCache.enableDisableDiskCache(false); // Disable disk cache now.
        int numOfItems2 = totalSize - numOfItems1;
        for (int iter = 0; iter < numOfItems2; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            int keySegment = tieredSpilloverCache.getSegmentNumber(key);
            TieredSpilloverCache.TieredSpilloverCacheSegment<String, String> segment =
                tieredSpilloverCache.tieredSpilloverCacheSegments[keySegment];
            LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, loadAwareCacheLoader);
        }
        for (ICacheKey<String> diskKey : diskCacheKeys) {
            assertNull(tieredSpilloverCache.get(diskKey)); // Considering disk cache is disabled, we shouldn't find
            // these keys.
        }
        assertEquals(getItemsForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP), tieredSpilloverCache.onHeapCacheCount()); // Should
                                                                                                                                    // remain
                                                                                                                                    // same.
        assertEquals(0, tieredSpilloverCache.diskCacheCount() - actualDiskCacheCount); // Considering it is disabled now, shouldn't
        // cache
        // any more items.
        assertTrue(removalListener.evictionsMetric.count() > 0);
        // Considering onHeap cache was already full, we should have some onHeap entries being evicted.
        long lastKnownTieredCacheEntriesCount = tieredSpilloverCache.count();

        // Clear up disk cache keys.
        for (ICacheKey<String> diskKey : diskCacheKeys) {
            tieredSpilloverCache.invalidate(diskKey);
        }
        assertEquals(0, tieredSpilloverCache.diskCacheCount());
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
            0,
            1
        );

        // do some gets to put entries in both tiers
        int numMisses = onHeapCacheSize + randomIntBetween(10, 20);
        for (int iter = 0; iter < numMisses; iter++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertNotEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), tieredSpilloverCache.stats().getTotalStats());
        assertEquals(
            new ImmutableCacheStats(0, 0, 0, 0, 0),
            tieredSpilloverCache.tieredSpilloverCacheSegments[0].getOnHeapCache().stats().getTotalStats()
        );
        ImmutableCacheStats diskStats = tieredSpilloverCache.tieredSpilloverCacheSegments[0].getDiskCache().stats().getTotalStats();
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

        int onHeapCacheSize = randomIntBetween(300, 600);
        int diskCacheSize = randomIntBetween(700, 1200);
        int numberOfSegments = getNumberOfSegments();
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
            0,
            numberOfSegments
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
        Map<Integer, Integer> onHeapExpectedSize = getSegmentOnHeapCacheSize(
            numberOfSegments,
            onHeapCacheSize * keyValueSize,
            keyValueSize
        );
        int numOfItems = randomIntBetween(10, 30);
        int newMisses = 0;
        for (int i = 0; i < numOfItems; i++) {
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            ICacheKey<String> iCacheKey = getICacheKey(UUID.randomUUID().toString());
            int keySegment = tieredSpilloverCache.getSegmentNumber(iCacheKey);
            if (tieredSpilloverCache.tieredSpilloverCacheSegments[keySegment].getOnHeapCache().count() >= onHeapExpectedSize.get(
                keySegment
            )) {
                newMisses++;
            }
            tieredSpilloverCache.computeIfAbsent(iCacheKey, tieredCacheLoader);
        }

        totalStats = tieredSpilloverCache.stats().getTotalStats();
        heapStats = getStatsSnapshotForTier(tieredSpilloverCache, TIER_DIMENSION_VALUE_ON_HEAP);
        assertEquals(missesBeforeDisablingDiskCache + numOfItems, totalStats.getMisses());
        assertEquals(heapTierEvictionsBeforeDisablingDiskCache + newMisses, heapStats.getEvictions());
        assertEquals(evictionsBeforeDisablingDiskCache + newMisses, totalStats.getEvictions());

        // Turn the disk cache back on in cluster settings for other tests
        clusterSettings.applySettings(
            Settings.builder().put(DISK_CACHE_ENABLED_SETTING_MAP.get(CacheType.INDICES_REQUEST_CACHE).getKey(), true).build()
        );
    }

    public void testPutForAKeyWhichAlreadyExists() {
        int onHeapCacheSize = 1;
        int diskCacheSize = 3;
        int keyValueSize = 1;
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
            0,
            1
        );
        ICacheKey<String> key1 = getICacheKey("key1");
        ICacheKey<String> key2 = getICacheKey("key2");
        tieredSpilloverCache.put(key1, "key1"); // Goes to onHeap cache.
        tieredSpilloverCache.put(key2, "key2"); // Goes to onHeap cache. And key1 evicted to disk cache.
        List<ICacheKey<String>> diskKeys = new ArrayList<>();
        List<ICacheKey<String>> onHeapCacheKeys = new ArrayList<>();
        tieredSpilloverCache.tieredSpilloverCacheSegments[0].getDiskCache().keys().forEach(diskKeys::add);
        tieredSpilloverCache.tieredSpilloverCacheSegments[0].getOnHeapCache().keys().forEach(onHeapCacheKeys::add);
        assertEquals(1, onHeapCacheKeys.size());
        assertEquals(1, diskKeys.size());
        assertTrue(onHeapCacheKeys.contains(key2));
        assertTrue(diskKeys.contains(key1));
        assertEquals("key1", tieredSpilloverCache.get(key1));

        // Now try to put key1 again onto tiered cache with new value.
        tieredSpilloverCache.put(key1, "dummy");
        diskKeys.clear();
        onHeapCacheKeys.clear();
        tieredSpilloverCache.tieredSpilloverCacheSegments[0].getDiskCache().keys().forEach(diskKeys::add);
        tieredSpilloverCache.tieredSpilloverCacheSegments[0].getOnHeapCache().keys().forEach(onHeapCacheKeys::add);
        assertEquals(1, onHeapCacheKeys.size());
        assertEquals(1, diskKeys.size());
        assertTrue(onHeapCacheKeys.contains(key2));
        assertTrue(diskKeys.contains(key1));
        assertEquals("dummy", tieredSpilloverCache.get(key1));
    }

    public void testTieredCacheThrowingExceptionOnPerSegmentSizeBeingZero() {
        int onHeapCacheSize = 10;
        int diskCacheSize = randomIntBetween(700, 1200);
        int keyValueSize = 1;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        assertThrows(
            IllegalArgumentException.class,
            () -> initializeTieredSpilloverCache(
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
                0,
                256
            )
        );
    }

    public void testTieredCacheWithZeroNumberOfSegments() {
        int onHeapCacheSize = 10;
        int diskCacheSize = randomIntBetween(700, 1200);
        int keyValueSize = 1;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        assertThrows(
            ZERO_SEGMENT_COUNT_EXCEPTION_MESSAGE,
            IllegalArgumentException.class,
            () -> initializeTieredSpilloverCache(
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
                0,
                0
            )
        );
    }

    public void testWithInvalidSegmentNumber() throws Exception {
        int onHeapCacheSize = 10;
        int diskCacheSize = randomIntBetween(700, 1200);
        int keyValueSize = 1;
        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
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
            .put(TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()).getKey(), 1)
            .put(FeatureFlags.PLUGGABLE_CACHE, "true")
            .put(TIERED_SPILLOVER_SEGMENTS.getConcreteSettingForNamespace(CacheType.INDICES_REQUEST_CACHE.getSettingPrefix()).getKey(), 3)
            .build();
        String storagePath = getStoragePath(settings);
        assertThrows(
            INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE,
            IllegalArgumentException.class,
            () -> new TieredSpilloverCache.TieredSpilloverCacheFactory().create(
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
                    .setStoragePath(storagePath)
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

    private TieredSpilloverCache<String, String> getTieredSpilloverCache(
        ICache.Factory onHeapCacheFactory,
        ICache.Factory mockDiskCacheFactory,
        CacheConfig<String, String> cacheConfig,
        List<Predicate<String>> policies,
        RemovalListener<ICacheKey<String>, String> removalListener,
        int numberOfSegments
    ) {
        TieredSpilloverCache.Builder<String, String> builder = new TieredSpilloverCache.Builder<String, String>().setCacheType(
            CacheType.INDICES_REQUEST_CACHE
        )
            .setRemovalListener(removalListener)
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(mockDiskCacheFactory)
            .setNumberOfSegments(numberOfSegments)
            .setCacheConfig(cacheConfig);
        if (policies != null) {
            builder.addPolicies(policies);
        }
        return builder.build();
    }

    private TieredSpilloverCache<String, String> initializeTieredSpilloverCache(
        int keyValueSize,
        int diskCacheSize,
        RemovalListener<ICacheKey<String>, String> removalListener,
        Settings settings,
        long diskDeliberateDelay

    ) {
        return intializeTieredSpilloverCache(keyValueSize, diskCacheSize, removalListener, settings, diskDeliberateDelay, null, 256);
    }

    private TieredSpilloverCache<String, String> initializeTieredSpilloverCache(
        int keyValueSize,
        int diskCacheSize,
        RemovalListener<ICacheKey<String>, String> removalListener,
        Settings settings,
        long diskDeliberateDelay,
        int numberOfSegments

    ) {
        return intializeTieredSpilloverCache(
            keyValueSize,
            diskCacheSize,
            removalListener,
            settings,
            diskDeliberateDelay,
            null,
            numberOfSegments
        );
    }

    private TieredSpilloverCache<String, String> intializeTieredSpilloverCache(
        int keyValueSize,
        int diskCacheSize,
        RemovalListener<ICacheKey<String>, String> removalListener,
        Settings settings,
        long diskDeliberateDelay,
        List<Predicate<String>> policies,
        int numberOfSegments
    ) {
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        String storagePath;
        try (NodeEnvironment environment = newNodeEnvironment(settings)) {
            storagePath = environment.nodePaths()[0].path + "/test";
        } catch (IOException e) {
            throw new OpenSearchException("Exception occurred", e);
        }
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
            .setStoragePath(storagePath)
            .build();
        ICache.Factory mockDiskCacheFactory = new MockDiskCache.MockDiskCacheFactory(diskDeliberateDelay, diskCacheSize, false);

        return getTieredSpilloverCache(onHeapCacheFactory, mockDiskCacheFactory, cacheConfig, policies, removalListener, numberOfSegments);
    }

    private CacheConfig<String, String> getCacheConfig(
        int keyValueSize,
        Settings settings,
        RemovalListener<ICacheKey<String>, String> removalListener,
        int numberOfSegments
    ) {
        String storagePath;
        try (NodeEnvironment environment = newNodeEnvironment(settings)) {
            storagePath = environment.nodePaths()[0].path + "/test";
        } catch (IOException e) {
            throw new OpenSearchException("Exception occurred", e);
        }
        return new CacheConfig.Builder<String, String>().setKeyType(String.class)
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
            .setStoragePath(storagePath)
            .setSegmentCount(numberOfSegments)
            .build();
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

    private String getStoragePath(Settings settings) {
        String storagePath;
        try (NodeEnvironment environment = newNodeEnvironment(settings)) {
            storagePath = environment.nodePaths()[0].path + "/test";
        } catch (IOException e) {
            throw new OpenSearchException("Exception occurred", e);
        }
        return storagePath;
    }

    private void verifyComputeIfAbsentThrowsException(
        Class<? extends Exception> expectedException,
        LoadAwareCacheLoader<ICacheKey<String>, String> loader,
        String expectedExceptionMessage
    ) throws InterruptedException {
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
            0,
            1
        );

        int numberOfSameKeys = randomIntBetween(10, onHeapCacheSize - 1);
        ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
        String value = UUID.randomUUID().toString();
        AtomicInteger exceptionCount = new AtomicInteger();

        Thread[] threads = new Thread[numberOfSameKeys];
        Phaser phaser = new Phaser(numberOfSameKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfSameKeys); // To wait for all threads to finish.

        for (int i = 0; i < numberOfSameKeys; i++) {
            threads[i] = new Thread(() -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    tieredSpilloverCache.computeIfAbsent(key, loader);
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    assertEquals(ExecutionException.class, e.getClass());
                    assertEquals(expectedException, e.getCause().getClass());
                    assertEquals(expectedExceptionMessage, e.getCause().getMessage());
                } finally {
                    countDownLatch.countDown();
                }
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await(); // Wait for rest of tasks to be cancelled.

        // Verify exception count was equal to number of requests
        assertEquals(numberOfSameKeys, exceptionCount.get());
        assertEquals(0, tieredSpilloverCache.completableFutureMap.size());
    }

    private int getNumberOfSegments() {
        return randomFrom(2, 4, 8, 16, 32, 64, 128, 256);
    }

    private Map<Integer, Integer> getSegmentOnHeapCacheSize(int numberOfSegments, int onHeapCacheSizeInBytes, int keyValueSize) {
        Map<Integer, Integer> expectedSegmentOnHeapCacheSize = new HashMap<>();
        for (int i = 0; i < numberOfSegments; i++) {
            int perSegmentOnHeapCacheSizeBytes = onHeapCacheSizeInBytes / numberOfSegments;
            int perSegmentOnHeapCacheEntries = perSegmentOnHeapCacheSizeBytes / keyValueSize;
            expectedSegmentOnHeapCacheSize.put(i, perSegmentOnHeapCacheEntries);
        }
        return expectedSegmentOnHeapCacheSize;
    }

    private Map<Integer, Integer> getSegmentMockDiskCacheSize(int numberOfSegments, int diskCacheSize) {
        Map<Integer, Integer> expectedSegmentDiskCacheSize = new HashMap<>();
        for (int i = 0; i < numberOfSegments; i++) {
            int perSegmentDiskCacheEntries = diskCacheSize / numberOfSegments;
            expectedSegmentDiskCacheSize.put(i, perSegmentDiskCacheEntries);
        }
        return expectedSegmentDiskCacheSize;
    }

    private ImmutableCacheStats getTotalStatsSnapshot(TieredSpilloverCache<?, ?> tsc) throws IOException {
        ImmutableCacheStatsHolder cacheStats = tsc.stats(new String[0]);
        return cacheStats.getStatsForDimensionValues(List.of());
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
}
