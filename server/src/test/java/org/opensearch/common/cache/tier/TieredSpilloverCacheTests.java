/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.builders.StoreAwareCacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

public class TieredSpilloverCacheTests extends OpenSearchTestCase {

    public void testComputeIfAbsentWithoutAnyOnHeapCacheEviction() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            randomIntBetween(1, 4),
            eventListener,
            0
        );
        int numOfItems1 = randomIntBetween(1, onHeapCacheSize / 2 - 1);
        List<String> keys = new ArrayList<>();
        // Put values in cache.
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            keys.add(key);
            LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(numOfItems1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.ON_HEAP).hitCount.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count());

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
                tieredSpilloverCache.computeIfAbsent(UUID.randomUUID().toString(), getLoadAwareCacheLoader());
                cacheMiss++;
            }
        }
        assertEquals(cacheHit, eventListener.enumMap.get(CacheStoreType.ON_HEAP).hitCount.count());
        assertEquals(numOfItems1 + cacheMiss, eventListener.enumMap.get(CacheStoreType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count());
    }

    public void testComputeIfAbsentWithEvictionsFromOnHeapCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;
        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();
        StoreAwareCacheBuilder<String, String> cacheBuilder = new OpenSearchOnHeapCache.Builder<String, String>().setMaximumWeightInBytes(
            onHeapCacheSize * 50
        ).setWeigher((k, v) -> 50); // Will support onHeapCacheSize entries.

        StoreAwareCacheBuilder<String, String> diskCacheBuilder = new MockOnDiskCache.Builder<String, String>().setMaxSize(diskCacheSize)
            .setDeliberateDelay(0);

        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheBuilder(cacheBuilder)
            .setOnDiskCacheBuilder(diskCacheBuilder)
            .setListener(eventListener)
            .build();

        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<String> onHeapKeys = new ArrayList<>();
        List<String> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        long actualDiskCacheSize = tieredSpilloverCache.getOnDiskCache().get().count();
        assertEquals(numOfItems1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.ON_HEAP).hitCount.count());
        assertEquals(actualDiskCacheSize, eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count());

        assertEquals(
            eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count(),
            eventListener.enumMap.get(CacheStoreType.DISK).cachedCount.count()
        );
        assertEquals(actualDiskCacheSize, eventListener.enumMap.get(CacheStoreType.DISK).cachedCount.count());

        tieredSpilloverCache.cacheKeys(CacheStoreType.ON_HEAP).forEach(onHeapKeys::add);
        tieredSpilloverCache.cacheKeys(CacheStoreType.DISK).forEach(diskTierKeys::add);

        assertEquals(tieredSpilloverCache.getOnHeapCache().count(), onHeapKeys.size());
        assertEquals(tieredSpilloverCache.getOnDiskCache().get().count(), diskTierKeys.size());

        // Try to hit cache again with some randomization.
        int numOfItems2 = randomIntBetween(50, 200);
        int onHeapCacheHit = 0;
        int diskCacheHit = 0;
        int cacheMiss = 0;
        for (int iter = 0; iter < numOfItems2; iter++) {
            if (randomBoolean()) { // Hit cache with key stored in onHeap cache.
                onHeapCacheHit++;
                int index = randomIntBetween(0, onHeapKeys.size() - 1);
                LoadAwareCacheLoader<String, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
                tieredSpilloverCache.computeIfAbsent(onHeapKeys.get(index), loadAwareCacheLoader);
                assertFalse(loadAwareCacheLoader.isLoaded());
            } else { // Hit cache with key stored in disk cache.
                diskCacheHit++;
                int index = randomIntBetween(0, diskTierKeys.size() - 1);
                LoadAwareCacheLoader<String, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
                tieredSpilloverCache.computeIfAbsent(diskTierKeys.get(index), loadAwareCacheLoader);
                assertFalse(loadAwareCacheLoader.isLoaded());
            }
        }
        for (int iter = 0; iter < randomIntBetween(50, 200); iter++) {
            // Hit cache with randomized key which is expected to miss cache always.
            LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(UUID.randomUUID().toString(), tieredCacheLoader);
            cacheMiss++;
        }
        // On heap cache misses would also include diskCacheHits as it means it missed onHeap cache.
        assertEquals(numOfItems1 + cacheMiss + diskCacheHit, eventListener.enumMap.get(CacheStoreType.ON_HEAP).missCount.count());
        assertEquals(onHeapCacheHit, eventListener.enumMap.get(CacheStoreType.ON_HEAP).hitCount.count());
        assertEquals(cacheMiss + numOfItems1, eventListener.enumMap.get(CacheStoreType.DISK).missCount.count());
        assertEquals(diskCacheHit, eventListener.enumMap.get(CacheStoreType.DISK).hitCount.count());
    }

    public void testComputeIfAbsentWithEvictionsFromBothTier() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );

        int numOfItems = randomIntBetween(totalSize + 1, totalSize * 3);
        for (int iter = 0; iter < numOfItems; iter++) {
            LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(UUID.randomUUID().toString(), tieredCacheLoader);
        }
        assertTrue(eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count() > 0);
        assertTrue(eventListener.enumMap.get(CacheStoreType.DISK).evictionsMetric.count() > 0);
    }

    public void testGetAndCount() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<String> onHeapKeys = new ArrayList<>();
        List<String> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            if (iter > (onHeapCacheSize - 1)) {
                // All these are bound to go to disk based cache.
                diskTierKeys.add(key);
            } else {
                onHeapKeys.add(key);
            }
            LoadAwareCacheLoader<String, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
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
                assertNull(tieredSpilloverCache.get(UUID.randomUUID().toString()));
            }
        }
        assertEquals(numOfItems1, tieredSpilloverCache.count());
    }

    public void testWithDiskTierNull() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();

        StoreAwareCacheBuilder<String, String> onHeapCacheBuilder = new OpenSearchOnHeapCache.Builder<String, String>()
            .setMaximumWeightInBytes(onHeapCacheSize * 20)
            .setWeigher((k, v) -> 20); // Will support upto onHeapCacheSize entries
        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheBuilder(onHeapCacheBuilder)
            .setListener(eventListener)
            .build();

        int numOfItems = randomIntBetween(onHeapCacheSize + 1, onHeapCacheSize * 3);
        for (int iter = 0; iter < numOfItems; iter++) {
            LoadAwareCacheLoader<String, String> loadAwareCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(UUID.randomUUID().toString(), loadAwareCacheLoader);
        }
        assertTrue(eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count() > 0);
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.DISK).cachedCount.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.DISK).evictionsMetric.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.DISK).missCount.count());
    }

    public void testPut() {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        tieredSpilloverCache.put(key, value);
        assertEquals(1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).cachedCount.count());
        assertEquals(1, tieredSpilloverCache.count());
    }

    public void testPutAndVerifyNewItemsArePresentOnHeapCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(200, 400);
        int diskCacheSize = randomIntBetween(450, 800);

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();

        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );

        for (int i = 0; i < onHeapCacheSize; i++) {
            tieredSpilloverCache.computeIfAbsent(UUID.randomUUID().toString(), new LoadAwareCacheLoader<>() {
                @Override
                public boolean isLoaded() {
                    return false;
                }

                @Override
                public String load(String key) throws Exception {
                    return UUID.randomUUID().toString();
                }
            });
        }

        assertEquals(onHeapCacheSize, tieredSpilloverCache.getOnHeapCache().count());
        assertEquals(0, tieredSpilloverCache.getOnDiskCache().get().count());

        // Again try to put OnHeap cache capacity amount of new items.
        List<String> newKeyList = new ArrayList<>();
        for (int i = 0; i < onHeapCacheSize; i++) {
            newKeyList.add(UUID.randomUUID().toString());
        }

        for (int i = 0; i < newKeyList.size(); i++) {
            tieredSpilloverCache.computeIfAbsent(newKeyList.get(i), new LoadAwareCacheLoader<>() {
                @Override
                public boolean isLoaded() {
                    return false;
                }

                @Override
                public String load(String key) {
                    return UUID.randomUUID().toString();
                }
            });
        }

        // Verify that new items are part of onHeap cache.
        List<String> actualOnHeapCacheKeys = new ArrayList<>();
        tieredSpilloverCache.cacheKeys(CacheStoreType.ON_HEAP).forEach(actualOnHeapCacheKeys::add);

        assertEquals(newKeyList.size(), actualOnHeapCacheKeys.size());
        for (int i = 0; i < actualOnHeapCacheKeys.size(); i++) {
            assertTrue(newKeyList.contains(actualOnHeapCacheKeys.get(i)));
        }

        assertEquals(onHeapCacheSize, tieredSpilloverCache.getOnHeapCache().count());
        assertEquals(onHeapCacheSize, tieredSpilloverCache.getOnDiskCache().get().count());
    }

    public void testInvalidate() {
        int onHeapCacheSize = 1;
        int diskCacheSize = 10;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        // First try to invalidate without the key present in cache.
        tieredSpilloverCache.invalidate(key);
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.ON_HEAP).invalidationMetric.count());

        // Now try to invalidate with the key present in onHeap cache.
        tieredSpilloverCache.put(key, value);
        tieredSpilloverCache.invalidate(key);
        assertEquals(1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).invalidationMetric.count());
        assertEquals(0, tieredSpilloverCache.count());

        tieredSpilloverCache.put(key, value);
        // Put another key/value so that one of the item is evicted to disk cache.
        String key2 = UUID.randomUUID().toString();
        tieredSpilloverCache.put(key2, UUID.randomUUID().toString());
        assertEquals(2, tieredSpilloverCache.count());
        // Again invalidate older key
        tieredSpilloverCache.invalidate(key);
        assertEquals(1, eventListener.enumMap.get(CacheStoreType.DISK).invalidationMetric.count());
        assertEquals(1, tieredSpilloverCache.count());
    }

    public void testCacheKeys() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );
        List<String> onHeapKeys = new ArrayList<>();
        List<String> diskTierKeys = new ArrayList<>();
        // During first round add onHeapCacheSize entries. Will go to onHeap cache initially.
        for (int i = 0; i < onHeapCacheSize; i++) {
            String key = UUID.randomUUID().toString();
            diskTierKeys.add(key);
            tieredSpilloverCache.computeIfAbsent(key, getLoadAwareCacheLoader());
        }
        // In another round, add another onHeapCacheSize entries. These will go to onHeap and above ones will be
        // evicted to onDisk cache.
        for (int i = 0; i < onHeapCacheSize; i++) {
            String key = UUID.randomUUID().toString();
            onHeapKeys.add(key);
            tieredSpilloverCache.computeIfAbsent(key, getLoadAwareCacheLoader());
        }

        List<String> actualOnHeapKeys = new ArrayList<>();
        List<String> actualOnDiskKeys = new ArrayList<>();
        Iterable<String> onHeapiterable = tieredSpilloverCache.cacheKeys(CacheStoreType.ON_HEAP);
        Iterable<String> onDiskiterable = tieredSpilloverCache.cacheKeys(CacheStoreType.DISK);
        onHeapiterable.iterator().forEachRemaining(actualOnHeapKeys::add);
        onDiskiterable.iterator().forEachRemaining(actualOnDiskKeys::add);
        for (String onHeapKey : onHeapKeys) {
            assertTrue(actualOnHeapKeys.contains(onHeapKey));
        }
        for (String onDiskKey : actualOnDiskKeys) {
            assertTrue(actualOnDiskKeys.contains(onDiskKey));
        }

        // Testing keys() which returns all keys.
        List<String> actualMergedKeys = new ArrayList<>();
        List<String> expectedMergedKeys = new ArrayList<>();
        expectedMergedKeys.addAll(onHeapKeys);
        expectedMergedKeys.addAll(diskTierKeys);

        Iterable<String> mergedIterable = tieredSpilloverCache.keys();
        mergedIterable.iterator().forEachRemaining(actualMergedKeys::add);

        assertEquals(expectedMergedKeys.size(), actualMergedKeys.size());
        for (String key : expectedMergedKeys) {
            assertTrue(actualMergedKeys.contains(key));
        }
    }

    public void testRefresh() {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );
        tieredSpilloverCache.refresh(CacheStoreType.ON_HEAP);
        tieredSpilloverCache.refresh(CacheStoreType.DISK);
        tieredSpilloverCache.refresh();
    }

    public void testInvalidateAll() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );
        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<String> onHeapKeys = new ArrayList<>();
        List<String> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            if (iter > (onHeapCacheSize - 1)) {
                // All these are bound to go to disk based cache.
                diskTierKeys.add(key);
            } else {
                onHeapKeys.add(key);
            }
            LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
            tieredSpilloverCache.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(numOfItems1, tieredSpilloverCache.count());
        tieredSpilloverCache.invalidateAll();
        assertEquals(0, tieredSpilloverCache.count());
    }

    public void testComputeIfAbsentConcurrently() throws Exception {
        int onHeapCacheSize = randomIntBetween(100, 300);
        int diskCacheSize = randomIntBetween(200, 400);

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();

        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            0
        );

        int numberOfSameKeys = randomIntBetween(10, onHeapCacheSize - 1);
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();

        Thread[] threads = new Thread[numberOfSameKeys];
        Phaser phaser = new Phaser(numberOfSameKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfSameKeys); // To wait for all threads to finish.

        List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numberOfSameKeys; i++) {
            threads[i] = new Thread(() -> {
                try {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader() {
                        boolean isLoaded = false;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public Object load(Object key) throws Exception {
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
            LoadAwareCacheLoader<String, String> loader = loadAwareCacheLoaderList.get(i);
            if (loader.isLoaded()) {
                numberOfTimesKeyLoaded++;
            }
        }
        assertEquals(1, numberOfTimesKeyLoaded); // It should be loaded only once.
    }

    public void testConcurrencyForEvictionFlow() throws Exception {
        int diskCacheSize = randomIntBetween(450, 800);

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();

        StoreAwareCacheBuilder<String, String> cacheBuilder = new OpenSearchOnHeapCache.Builder<String, String>().setMaximumWeightInBytes(
            200
        ).setWeigher((k, v) -> 150);

        StoreAwareCacheBuilder<String, String> diskCacheBuilder = new MockOnDiskCache.Builder<String, String>().setMaxSize(diskCacheSize)
            .setDeliberateDelay(500);

        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheBuilder(cacheBuilder)
            .setOnDiskCacheBuilder(diskCacheBuilder)
            .setListener(eventListener)
            .build();

        String keyToBeEvicted = "key1";
        String secondKey = "key2";

        // Put first key on tiered cache. Will go into onHeap cache.
        tieredSpilloverCache.computeIfAbsent(keyToBeEvicted, new LoadAwareCacheLoader<>() {
            @Override
            public boolean isLoaded() {
                return false;
            }

            @Override
            public String load(String key) throws Exception {
                return UUID.randomUUID().toString();
            }
        });
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountDownLatch countDownLatch1 = new CountDownLatch(1);
        // Put second key on tiered cache. Will cause eviction of first key from onHeap cache and should go into
        // disk cache.
        Thread thread = new Thread(() -> {
            try {
                tieredSpilloverCache.computeIfAbsent(secondKey, new LoadAwareCacheLoader<String, String>() {
                    @Override
                    public boolean isLoaded() {
                        return false;
                    }

                    @Override
                    public String load(String key) throws Exception {
                        return UUID.randomUUID().toString();
                    }
                });
                countDownLatch1.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        Thread.sleep(100); // Delay for cache for eviction to occur.
        StoreAwareCache<String, String> onDiskCache = tieredSpilloverCache.getOnDiskCache().get();

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
        assertEquals(1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count());
        assertEquals(1, tieredSpilloverCache.getOnHeapCache().count());
        assertEquals(1, onDiskCache.count());
        assertNotNull(onDiskCache.get(keyToBeEvicted));
    }

    class MockCacheEventListener<K, V> implements StoreAwareCacheEventListener<K, V> {

        EnumMap<CacheStoreType, TestStatsHolder> enumMap = new EnumMap<>(CacheStoreType.class);

        MockCacheEventListener() {
            for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
                enumMap.put(cacheStoreType, new TestStatsHolder());
            }
        }

        @Override
        public void onMiss(K key, CacheStoreType cacheStoreType) {
            enumMap.get(cacheStoreType).missCount.inc();
        }

        @Override
        public void onRemoval(StoreAwareCacheRemovalNotification<K, V> notification) {
            if (notification.getRemovalReason().equals(RemovalReason.EVICTED)) {
                enumMap.get(notification.getCacheStoreType()).evictionsMetric.inc();
            } else if (notification.getRemovalReason().equals(RemovalReason.INVALIDATED)) {
                enumMap.get(notification.getCacheStoreType()).invalidationMetric.inc();
            }
        }

        @Override
        public void onHit(K key, V value, CacheStoreType cacheStoreType) {
            enumMap.get(cacheStoreType).hitCount.inc();
        }

        @Override
        public void onCached(K key, V value, CacheStoreType cacheStoreType) {
            enumMap.get(cacheStoreType).cachedCount.inc();
        }

        class TestStatsHolder {
            final CounterMetric evictionsMetric = new CounterMetric();
            final CounterMetric hitCount = new CounterMetric();
            final CounterMetric missCount = new CounterMetric();
            final CounterMetric cachedCount = new CounterMetric();
            final CounterMetric invalidationMetric = new CounterMetric();
        }
    }

    private LoadAwareCacheLoader<String, String> getLoadAwareCacheLoader() {
        return new LoadAwareCacheLoader<String, String>() {
            boolean isLoaded = false;

            @Override
            public String load(String key) {
                isLoaded = true;
                return UUID.randomUUID().toString();
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }

    private TieredSpilloverCache<String, String> intializeTieredSpilloverCache(
        int onHeapCacheSize,
        int diksCacheSize,
        StoreAwareCacheEventListener<String, String> eventListener,
        long diskDeliberateDelay
    ) {
        StoreAwareCacheBuilder<String, String> diskCacheBuilder = new MockOnDiskCache.Builder<String, String>().setMaxSize(diksCacheSize)
            .setDeliberateDelay(diskDeliberateDelay);
        StoreAwareCacheBuilder<String, String> onHeapCacheBuilder = new OpenSearchOnHeapCache.Builder<String, String>()
            .setMaximumWeightInBytes(onHeapCacheSize * 20)
            .setWeigher((k, v) -> 20); // Will support upto onHeapCacheSize entries
        return new TieredSpilloverCache.Builder<String, String>().setOnHeapCacheBuilder(onHeapCacheBuilder)
            .setOnDiskCacheBuilder(diskCacheBuilder)
            .setListener(eventListener)
            .build();
    }
}

class MockOnDiskCache<K, V> implements StoreAwareCache<K, V> {

    Map<K, V> cache;
    int maxSize;

    long delay;
    StoreAwareCacheEventListener<K, V> eventListener;

    MockOnDiskCache(int maxSize, StoreAwareCacheEventListener<K, V> eventListener, long delay) {
        this.maxSize = maxSize;
        this.eventListener = eventListener;
        this.delay = delay;
        this.cache = new ConcurrentHashMap<K, V>();
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            eventListener.onHit(key, value, CacheStoreType.DISK);
        } else {
            eventListener.onMiss(key, CacheStoreType.DISK);
        }
        return value;
    }

    @Override
    public void put(K key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            eventListener.onRemoval(new StoreAwareCacheRemovalNotification<>(key, value, RemovalReason.EVICTED, CacheStoreType.DISK));
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.cache.put(key, value);
        eventListener.onCached(key, value, CacheStoreType.DISK);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        if (!loader.isLoaded()) {
            eventListener.onHit(key, value, CacheStoreType.DISK);
        } else {
            eventListener.onMiss(key, CacheStoreType.DISK);
            eventListener.onCached(key, value, CacheStoreType.DISK);
        }
        return value;
    }

    @Override
    public void invalidate(K key) {
        if (this.cache.containsKey(key)) {
            eventListener.onRemoval(new StoreAwareCacheRemovalNotification<>(key, null, RemovalReason.INVALIDATED, CacheStoreType.DISK));
        }
        this.cache.remove(key);
    }

    @Override
    public void invalidateAll() {
        this.cache.clear();
    }

    @Override
    public Iterable<K> keys() {
        return this.cache.keySet();
    }

    @Override
    public long count() {
        return this.cache.size();
    }

    @Override
    public void refresh() {}

    @Override
    public CacheStoreType getTierType() {
        return CacheStoreType.DISK;
    }

    public static class Builder<K, V> extends StoreAwareCacheBuilder<K, V> {

        int maxSize;
        long delay;

        @Override
        public StoreAwareCache<K, V> build() {
            return new MockOnDiskCache<K, V>(maxSize, this.getEventListener(), delay);
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
