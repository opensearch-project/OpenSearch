/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

public class TieredSpilloverCacheTests extends OpenSearchTestCase {
    // TODO: TSC stats impl is in a future PR. Parts of tests which use stats values are commented out for now.
    static final List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
    public void testComputeIfAbsentWithoutAnyOnHeapCacheEviction() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            onHeapCacheSize,
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
        /*assertEquals(numOfItems1, tieredSpilloverCache.stats().getMissesByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getHitsByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));*/

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
        /*assertEquals(cacheHit, tieredSpilloverCache.stats().getHitsByDimensions(HEAP_DIMS));
        assertEquals(numOfItems1 + cacheMiss, tieredSpilloverCache.stats().getMissesByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));*/
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
                MockOnDiskCache.MockDiskCacheFactory.NAME
            )
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                onHeapCacheSize * keyValueSize + "b"
            )
            .build();

        ICache<String, String> tieredSpilloverICache = new TieredSpilloverCache.TieredSpilloverCacheFactory().create(
            new CacheConfig.Builder<String, String>().setKeyType(String.class)
                .setKeyType(String.class)
                .setWeigher((k, v) -> keyValueSize)
                .setRemovalListener(removalListener)
                .setSettings(settings)
                .setDimensionNames(dimensionNames)
                .build(),
            CacheType.INDICES_REQUEST_CACHE,
            Map.of(
                OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory.NAME,
                new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory(),
                MockOnDiskCache.MockDiskCacheFactory.NAME,
                new MockOnDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300))
            )
        );

        TieredSpilloverCache<String, String> tieredSpilloverCache = (TieredSpilloverCache<String, String>) tieredSpilloverICache;

        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<ICacheKey<String>> onHeapKeys = new ArrayList<>();
        List<ICacheKey<String>> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(getICacheKey(key), tieredCacheLoader);
        }
        long actualDiskCacheSize = tieredSpilloverCache.getDiskCache().count();
        // Evictions from onHeap equal to disk cache size.
        /*assertEquals(numOfItems1, tieredSpilloverCache.stats().getMissesByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getHitsByDimensions(HEAP_DIMS));
        assertEquals(actualDiskCacheSize, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));*/

        tieredSpilloverCache.getOnHeapCache().keys().forEach(onHeapKeys::add);
        tieredSpilloverCache.getDiskCache().keys().forEach(diskTierKeys::add);

        /*assertEquals(onHeapKeys.size(), tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(diskTierKeys.size(), tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));
        assertEquals(onHeapKeys.size() * keyValueSize, tieredSpilloverCache.stats().getMemorySizeByDimensions(HEAP_DIMS));
        assertEquals(diskTierKeys.size() * keyValueSize, tieredSpilloverCache.stats().getMemorySizeByDimensions(DISK_DIMS));*/
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
                MockOnDiskCache.MockDiskCacheFactory.NAME
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
                    MockOnDiskCache.MockDiskCacheFactory.NAME,
                    new MockOnDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300))
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
                    MockOnDiskCache.MockDiskCacheFactory.NAME,
                    new MockOnDiskCache.MockDiskCacheFactory(0, randomIntBetween(100, 300))
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
            .setDimensionNames(dimensionNames)
            .setSettings(
                Settings.builder()
                    .put(
                        OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                            .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                            .getKey(),
                        onHeapCacheSize * keyValueSize + "b"
                    )
                    .build()
            )
            .build();

        ICache.Factory mockDiskCacheFactory = new MockOnDiskCache.MockDiskCacheFactory(0, diskCacheSize);

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

        /*assertEquals(numOfItems1, tieredSpilloverCache.stats().getMissesByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getHitsByDimensions(HEAP_DIMS));
        assertEquals(actualDiskCacheSize, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));
        assertEquals(tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS), tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));*/

        tieredSpilloverCache.getOnHeapCache().keys().forEach(onHeapKeys::add);
        tieredSpilloverCache.getDiskCache().keys().forEach(diskTierKeys::add);

        /*assertEquals(tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS), onHeapKeys.size());
        assertEquals(tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS), diskTierKeys.size());*/

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
        for (int iter = 0; iter < randomIntBetween(50, 200); iter++) {
            // Hit cache with randomized key which is expected to miss cache always.
            LoadAwareCacheLoader<ICacheKey<String>, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(getICacheKey(UUID.randomUUID().toString()), tieredCacheLoader);
            cacheMiss++;
        }
        /*assertEquals(numOfItems1 + cacheMiss + diskCacheHit, tieredSpilloverCache.stats().getMissesByDimensions(HEAP_DIMS));
        assertEquals(onHeapCacheHit, tieredSpilloverCache.stats().getHitsByDimensions(HEAP_DIMS));
        assertEquals(cacheMiss + numOfItems1, tieredSpilloverCache.stats().getMissesByDimensions(DISK_DIMS));
        assertEquals(diskCacheHit, tieredSpilloverCache.stats().getHitsByDimensions(DISK_DIMS));*/
    }

    public void testComputeIfAbsentWithEvictionsFromBothTier() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            onHeapCacheSize,
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
        /*long diskSize = tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS);
        assertTrue(removalListener.evictionsMetric.count() > 0); // Removal listener captures anything that totally left the cache; in this case disk evictions
        assertEquals(removalListener.evictionsMetric.count(), tieredSpilloverCache.stats().getEvictionsByDimensions(DISK_DIMS));
        assertEquals(removalListener.evictionsMetric.count() + diskSize, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));*/
    }

    public void testGetAndCount() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            onHeapCacheSize,
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

    public void testPut() {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int keyValueSize = 50;

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = initializeTieredSpilloverCache(
            onHeapCacheSize,
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
        /*assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(1, tieredSpilloverCache.count());*/
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

        /*assertEquals(onHeapCacheSize, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));*/

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
        /*assertEquals(onHeapCacheSize, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(onHeapCacheSize, tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));*/
    }

    public void testInvalidate() {
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
        //assertEquals(0, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));

        // Now try to invalidate with the key present in onHeap cache.
        tieredSpilloverCache.put(key, value);
        tieredSpilloverCache.invalidate(key);
        //assertEquals(0, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));
        // Evictions metric shouldn't increase for invalidations.
        assertEquals(0, tieredSpilloverCache.count());

        tieredSpilloverCache.put(key, value);
        // Put another key/value so that one of the item is evicted to disk cache.
        ICacheKey<String> key2 = getICacheKey(UUID.randomUUID().toString());
        tieredSpilloverCache.put(key2, UUID.randomUUID().toString());

        assertEquals(2, tieredSpilloverCache.count());
        /*assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));*/

        // Again invalidate older key, leaving one in heap tier and zero in disk tier
        tieredSpilloverCache.invalidate(key);
        /*assertEquals(0, tieredSpilloverCache.stats().getEvictionsByDimensions(DISK_DIMS));
        assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(0, tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));
        assertEquals(1, tieredSpilloverCache.count());*/
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
    }

    public void testConcurrencyForEvictionFlow() throws Exception {
        int diskCacheSize = randomIntBetween(450, 800);

        MockCacheRemovalListener<String, String> removalListener = new MockCacheRemovalListener<>();

        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        ICache.Factory diskCacheFactory = new MockOnDiskCache.MockDiskCacheFactory(500, diskCacheSize);
        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> 150)
            .setRemovalListener(removalListener)
            .setSettings(
                Settings.builder()
                    .put(
                        OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                            .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                            .getKey(),
                        200 + "b"
                    )
                    .build()
            )
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
        //assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
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
        /*assertEquals(1, tieredSpilloverCache.stats().getEvictionsByDimensions(HEAP_DIMS));
        assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(HEAP_DIMS));
        assertEquals(1, tieredSpilloverCache.stats().getEntriesByDimensions(DISK_DIMS));*/
        assertNotNull(onDiskCache.get(keyToBeEvicted));
    }

    private List<CacheStatsDimension> getMockDimensions() {
        List<CacheStatsDimension> dims = new ArrayList<>();
        for (String dimensionName : dimensionNames) {
            dims.add(new CacheStatsDimension(dimensionName, "0"));
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

    private TieredSpilloverCache<String, String> initializeTieredSpilloverCache(
        int keyValueSize,
        int diskCacheSize,
        RemovalListener<ICacheKey<String>, String> removalListener,
        Settings settings,
        long diskDeliberateDelay
    ) {
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setKeyType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setSettings(settings)
            .setDimensionNames(dimensionNames)
            .build();

        ICache.Factory mockDiskCacheFactory = new MockOnDiskCache.MockDiskCacheFactory(diskDeliberateDelay, diskCacheSize);

        return new TieredSpilloverCache.Builder<String, String>().setCacheType(CacheType.INDICES_REQUEST_CACHE)
            .setRemovalListener(removalListener)
            .setOnHeapCacheFactory(onHeapCacheFactory)
            .setDiskCacheFactory(mockDiskCacheFactory)
            .setCacheConfig(cacheConfig)
            .build();
    }
}

class MockOnDiskCache<K, V> implements ICache<K, V> {

    Map<ICacheKey<K>, V> cache;
    int maxSize;
    long delay;
    CacheStats stats = null; // Not needed - TSC tracks its own stats

    RemovalListener<ICacheKey<K>, V> removalListener;

    MockOnDiskCache(int maxSize, long delay, RemovalListener<ICacheKey<K>, V> listener) {
        this.maxSize = maxSize;
        this.delay = delay;
        this.cache = new ConcurrentHashMap<ICacheKey<K>, V>();
        this.removalListener = listener;
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = cache.get(key);
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            removalListener.onRemoval(new RemovalNotification<>(key, null, RemovalReason.EVICTED));
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.cache.put(key, value);
        // eventListener.onCached(key, value, CacheStoreType.DISK);
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        if (this.cache.containsKey(key)) {
            removalListener.onRemoval(new RemovalNotification<>(key, null, RemovalReason.INVALIDATED));
        }
        this.cache.remove(key);
    }

    @Override
    public void invalidateAll() {
        this.cache.clear();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return this.cache.keySet();
    }

    @Override
    public long count() {
        return this.cache.size();
    }

    @Override
    public void refresh() {}

    @Override
    public CacheStats stats() {
        return stats;
    }

    @Override
    public void close() {

    }

    public static class MockDiskCacheFactory implements Factory {

        static final String NAME = "mockDiskCache";
        final long delay;
        final int maxSize;

        MockDiskCacheFactory(long delay, int maxSize) {
            this.delay = delay;
            this.maxSize = maxSize;
        }

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            return new Builder<K, V>().setMaxSize(maxSize).setDeliberateDelay(delay).setRemovalListener(config.getRemovalListener()).build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }
    }

    public static class Builder<K, V> extends ICacheBuilder<K, V> {

        int maxSize;
        long delay;

        @Override
        public ICache<K, V> build() {
            return new MockOnDiskCache<K, V>(this.maxSize, this.delay, this.getRemovalListener());
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder<K, V> setDeliberateDelay(long millis) {
            this.delay = millis;
            return this;
        }
    }
}
