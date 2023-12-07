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
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.StoreAwareCacheRemovalNotification;
import org.opensearch.common.cache.store.builders.StoreAwareCacheBuilder;
import org.opensearch.common.cache.store.enums.CacheStoreType;
import org.opensearch.common.cache.store.listeners.EventType;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListener;
import org.opensearch.common.cache.store.listeners.StoreAwareCacheEventListenerConfiguration;
import org.opensearch.common.cache.store.listeners.dispatchers.StoreAwareCacheEventListenerDispatcher;
import org.opensearch.common.cache.store.listeners.dispatchers.StoreAwareCacheListenerDispatcherDefaultImpl;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TieredSpilloverCacheTests extends OpenSearchTestCase {

    public void testComputeIfAbsentWithoutAnyOnHeapCacheEviction() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            randomIntBetween(1, 4),
            eventListenerConfiguration
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        assertEquals(numOfItems1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(CacheStoreType.ON_HEAP).hitCount.count());
        assertTrue(eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count() > 0);

        assertEquals(
            eventListener.enumMap.get(CacheStoreType.ON_HEAP).evictionsMetric.count(),
            eventListener.enumMap.get(CacheStoreType.DISK).cachedCount.count()
        );
        assertEquals(diskTierKeys.size(), eventListener.enumMap.get(CacheStoreType.DISK).cachedCount.count());

        // Try to hit cache again with some randomization.
        int numOfItems2 = randomIntBetween(50, 200);
        int onHeapCacheHit = 0;
        int diskCacheHit = 0;
        int cacheMiss = 0;
        for (int iter = 0; iter < numOfItems2; iter++) {
            if (randomBoolean()) {
                if (randomBoolean()) { // Hit cache with key stored in onHeap cache.
                    onHeapCacheHit++;
                    int index = randomIntBetween(0, onHeapKeys.size() - 1);
                    tieredSpilloverCache.computeIfAbsent(onHeapKeys.get(index), getLoadAwareCacheLoader());
                } else { // Hit cache with key stored in disk cache.
                    diskCacheHit++;
                    int index = randomIntBetween(0, diskTierKeys.size() - 1);
                    tieredSpilloverCache.computeIfAbsent(diskTierKeys.get(index), getLoadAwareCacheLoader());
                }
            } else {
                // Hit cache with randomized key which is expected to miss cache always.
                LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
                tieredSpilloverCache.computeIfAbsent(UUID.randomUUID().toString(), tieredCacheLoader);
                cacheMiss++;
            }
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();

        StoreAwareCacheBuilder<String, String> onHeapCacheBuilder = new MockOnHeapCache.Builder<String, String>().setMaxSize(
            onHeapCacheSize
        );
        TieredSpilloverCache<String, String> tieredSpilloverCache = new TieredSpilloverCache.Builder<String, String>()
            .setOnHeapCacheBuilder(onHeapCacheBuilder)
            .setListenerConfiguration(eventListenerConfiguration)
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
        );
        String key = UUID.randomUUID().toString();
        String value = UUID.randomUUID().toString();
        tieredSpilloverCache.put(key, value);
        assertEquals(1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).cachedCount.count());
        assertEquals(1, tieredSpilloverCache.count());
    }

    public void testInvalidate() {
        int onHeapCacheSize = 1;
        int diskCacheSize = 10;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        tieredSpilloverCache.invalidate(key2);
        assertEquals(1, eventListener.enumMap.get(CacheStoreType.DISK).invalidationMetric.count());
        assertEquals(1, tieredSpilloverCache.count());
    }

    public void testComputeWithoutAnyOnHeapEvictions() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<String, String>();
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> spilloverStrategyService = intializeTieredSpilloverCache(
            onHeapCacheSize,
            randomIntBetween(1, 4),
            eventListenerConfiguration
        );
        int numOfItems1 = randomIntBetween(1, onHeapCacheSize / 2 - 1);
        List<String> keys = new ArrayList<>();
        // Put values in cache.
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            keys.add(key);
            LoadAwareCacheLoader<String, String> tieredCacheLoader = getLoadAwareCacheLoader();
            spilloverStrategyService.compute(key, tieredCacheLoader);
        }
        assertEquals(numOfItems1, eventListener.enumMap.get(CacheStoreType.ON_HEAP).cachedCount.count());
    }

    public void testCacheKeys() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockCacheEventListener<String, String> eventListener = new MockCacheEventListener<>();
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        StoreAwareCacheEventListenerConfiguration<String, String> eventListenerConfiguration =
            new StoreAwareCacheEventListenerConfiguration.Builder<String, String>().setEventListener(eventListener)
                .setEventTypes(EnumSet.of(EventType.ON_CACHED, EventType.ON_HIT, EventType.ON_MISS, EventType.ON_REMOVAL))
                .build();
        TieredSpilloverCache<String, String> tieredSpilloverCache = intializeTieredSpilloverCache(
            onHeapCacheSize,
            diskCacheSize,
            eventListenerConfiguration
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
        StoreAwareCacheEventListenerConfiguration<String, String> listenerConfiguration
    ) {
        StoreAwareCacheBuilder<String, String> diskCacheBuilder = new MockOnDiskCache.Builder<String, String>().setMaxSize(diksCacheSize);
        StoreAwareCacheBuilder<String, String> onHeapCacheBuilder = new MockOnHeapCache.Builder<String, String>().setMaxSize(
            onHeapCacheSize
        );
        return new TieredSpilloverCache.Builder<String, String>().setOnHeapCacheBuilder(onHeapCacheBuilder)
            .setOnDiskCacheBuilder(diskCacheBuilder)
            .setListenerConfiguration(listenerConfiguration)
            .build();
    }
}

class MockOnDiskCache<K, V> implements StoreAwareCache<K, V> {

    Map<K, V> cache;
    int maxSize;
    StoreAwareCacheEventListenerDispatcher<K, V> eventDispatcher;

    MockOnDiskCache(int maxSize, StoreAwareCacheEventListenerConfiguration<K, V> eventListenerConfiguration) {
        this.maxSize = maxSize;
        this.eventDispatcher = new StoreAwareCacheListenerDispatcherDefaultImpl<>(eventListenerConfiguration);
        this.cache = new ConcurrentHashMap<K, V>();
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            eventDispatcher.dispatch(key, value, CacheStoreType.DISK, EventType.ON_HIT);
        } else {
            eventDispatcher.dispatch(key, null, CacheStoreType.DISK, EventType.ON_MISS);
        }
        return value;
    }

    @Override
    public void put(K key, V value) {
        if (this.cache.size() >= maxSize) { // For simplification
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, value, RemovalReason.EVICTED, CacheStoreType.DISK)
            );
            return;
        }
        this.cache.put(key, value);
        eventDispatcher.dispatch(key, value, CacheStoreType.DISK, EventType.ON_CACHED);
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
            eventDispatcher.dispatch(key, value, CacheStoreType.DISK, EventType.ON_HIT);
        } else {
            eventDispatcher.dispatch(key, value, CacheStoreType.DISK, EventType.ON_MISS);
            eventDispatcher.dispatch(key, value, CacheStoreType.DISK, EventType.ON_CACHED);
        }
        return value;
    }

    @Override
    public void invalidate(K key) {
        if (this.cache.containsKey(key)) {
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, null, RemovalReason.INVALIDATED, CacheStoreType.DISK)
            );
        }
        this.cache.remove(key);
    }

    @Override
    public V compute(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        if (this.cache.size() >= maxSize) { // If it exceeds, just notify for evict.
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, loader.load(key), RemovalReason.EVICTED, CacheStoreType.DISK)
            );
            return loader.load(key);
        }
        V value = this.cache.compute(key, ((k, v) -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
        eventDispatcher.dispatch(key, value, CacheStoreType.DISK, EventType.ON_CACHED);
        return value;
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

        @Override
        public StoreAwareCache<K, V> build() {
            return new MockOnDiskCache<K, V>(maxSize, this.getListenerConfiguration());
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }
    }
}

class MockOnHeapCache<K, V> implements StoreAwareCache<K, V> {

    Map<K, V> cache;
    int maxSize;
    StoreAwareCacheEventListenerDispatcher<K, V> eventDispatcher;

    MockOnHeapCache(int size, StoreAwareCacheEventListenerConfiguration<K, V> eventListenerConfiguration) {
        maxSize = size;
        this.cache = new ConcurrentHashMap<K, V>();
        this.eventDispatcher = new StoreAwareCacheListenerDispatcherDefaultImpl<>(eventListenerConfiguration);
    }

    @Override
    public V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_HIT);
        } else {
            eventDispatcher.dispatch(key, null, CacheStoreType.ON_HEAP, EventType.ON_MISS);
        }
        return value;
    }

    @Override
    public void put(K key, V value) {
        if (this.cache.size() >= maxSize) {
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, value, RemovalReason.EVICTED, CacheStoreType.ON_HEAP)
            );
            return;
        }
        this.cache.put(key, value);
        eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_CACHED);
    }

    @Override
    public V computeIfAbsent(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        if (this.cache.size() >= maxSize) { // If it exceeds, just notify for evict.
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, loader.load(key), RemovalReason.EVICTED, CacheStoreType.ON_HEAP)
            );
            return loader.load(key);
        }
        V value = cache.computeIfAbsent(key, key1 -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        if (!loader.isLoaded()) {
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_HIT);
        } else {
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_MISS);
            eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_CACHED);
        }
        return value;
    }

    @Override
    public void invalidate(K key) {
        if (this.cache.containsKey(key)) {
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, null, RemovalReason.INVALIDATED, CacheStoreType.ON_HEAP)
            );
        }
        this.cache.remove(key);

    }

    @Override
    public V compute(K key, LoadAwareCacheLoader<K, V> loader) throws Exception {
        if (this.cache.size() >= maxSize) { // If it exceeds, just notify for evict.
            eventDispatcher.dispatchRemovalEvent(
                new StoreAwareCacheRemovalNotification<>(key, loader.load(key), RemovalReason.EVICTED, CacheStoreType.ON_HEAP)
            );
            return loader.load(key);
        }
        V value = this.cache.compute(key, ((k, v) -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
        eventDispatcher.dispatch(key, value, CacheStoreType.ON_HEAP, EventType.ON_CACHED);
        return value;
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
    public void refresh() {

    }

    @Override
    public CacheStoreType getTierType() {
        return CacheStoreType.ON_HEAP;
    }

    public static class Builder<K, V> extends StoreAwareCacheBuilder<K, V> {

        int maxSize;

        @Override
        public StoreAwareCache<K, V> build() {
            return new MockOnHeapCache<>(maxSize, this.getListenerConfiguration());
        }

        public Builder<K, V> setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }
    }
}
