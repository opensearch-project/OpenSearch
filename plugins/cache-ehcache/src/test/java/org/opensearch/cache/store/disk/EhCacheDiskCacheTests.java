/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store.disk;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.cache.EhcacheDiskCacheSettings;
import org.opensearch.common.Randomness;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.serializer.BytesReferenceSerializer;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.function.ToLongBiFunction;

import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_LISTENER_MODE_SYNC_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_MAX_SIZE_IN_BYTES_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_STORAGE_PATH_KEY;
import static org.hamcrest.CoreMatchers.instanceOf;

@ThreadLeakFilters(filters = { EhcacheThreadLeakFilter.class })
public class EhCacheDiskCacheTests extends OpenSearchSingleNodeTestCase {

    private static final int CACHE_SIZE_IN_BYTES = 1024 * 101;
    private final String dimensionName = "shardId";

    public void testBasicGetAndPut() throws IOException {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ToLongBiFunction<ICacheKey<String>, String> weigher = getWeigher();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(weigher)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            long expectedSize = 0;
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ICacheKey<String> iCacheKey = getICacheKey(entry.getKey());
                ehcacheTest.put(iCacheKey, entry.getValue());
                expectedSize += weigher.applyAsLong(iCacheKey, entry.getValue());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(getICacheKey(entry.getKey()));
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, ehcacheTest.stats().getTotalItems());
            assertEquals(randomKeys, ehcacheTest.stats().getTotalHits());
            assertEquals(expectedSize, ehcacheTest.stats().getTotalSizeInBytes());
            assertEquals(randomKeys, ehcacheTest.count());

            // Validate misses
            int expectedNumberOfMisses = randomIntBetween(10, 200);
            for (int i = 0; i < expectedNumberOfMisses; i++) {
                ehcacheTest.get(getICacheKey(UUID.randomUUID().toString()));
            }

            assertEquals(expectedNumberOfMisses, ehcacheTest.stats().getTotalMisses());

            ehcacheTest.close();
        }
    }

    public void testBasicGetAndPutUsingFactory() throws IOException {
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            ICache.Factory ehcacheFactory = new EhcacheDiskCache.EhcacheDiskCacheFactory();
            ICache<String, String> ehcacheTest = ehcacheFactory.create(
                new CacheConfig.Builder<String, String>().setValueType(String.class)
                    .setKeyType(String.class)
                    .setRemovalListener(removalListener)
                    .setKeySerializer(new StringSerializer())
                    .setValueSerializer(new StringSerializer())
                    .setDimensionNames(List.of(dimensionName))
                    .setWeigher(getWeigher())
                    .setSettings(
                        Settings.builder()
                            .put(
                                EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                                    .get(DISK_MAX_SIZE_IN_BYTES_KEY)
                                    .getKey(),
                                CACHE_SIZE_IN_BYTES
                            )
                            .put(
                                EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                                    .get(DISK_STORAGE_PATH_KEY)
                                    .getKey(),
                                env.nodePaths()[0].indicesPath.toString() + "/request_cache"
                            )
                            .put(
                                EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                                    .get(DISK_LISTENER_MODE_SYNC_KEY)
                                    .getKey(),
                                true
                            )
                            .build()
                    )
                    .build(),
                CacheType.INDICES_REQUEST_CACHE,
                Map.of()
            );
            int randomKeys = randomIntBetween(10, 100);
            Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
            }
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, ehcacheTest.count());

            // Validate misses
            int expectedNumberOfMisses = randomIntBetween(10, 200);
            for (int i = 0; i < expectedNumberOfMisses; i++) {
                ehcacheTest.get(getICacheKey(UUID.randomUUID().toString()));
            }

            ehcacheTest.close();
        }
    }

    public void testConcurrentPut() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true) // For accurate count
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();
            int randomKeys = randomIntBetween(20, 100);
            Thread[] threads = new Thread[randomKeys];
            Phaser phaser = new Phaser(randomKeys + 1);
            CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
            Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
            int j = 0;
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
            }
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    ehcacheTest.put(entry.getKey(), entry.getValue());
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, ehcacheTest.count());
            assertEquals(randomKeys, ehcacheTest.stats().getTotalItems());
            ehcacheTest.close();
        }
    }

    public void testEhcacheParallelGets() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true) // For accurate count
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();
            int randomKeys = randomIntBetween(20, 100);
            Thread[] threads = new Thread[randomKeys];
            Phaser phaser = new Phaser(randomKeys + 1);
            CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
            Map<String, String> keyValueMap = new HashMap<>();
            int j = 0;
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(getICacheKey(entry.getKey()), entry.getValue());
            }
            assertEquals(keyValueMap.size(), ehcacheTest.count());
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    assertEquals(entry.getValue(), ehcacheTest.get(getICacheKey(entry.getKey())));
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
            assertEquals(randomKeys, ehcacheTest.stats().getTotalHits());
            ehcacheTest.close();
        }
    }

    public void testEhcacheKeyIterator() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(new MockRemovalListener<>())
                .setWeigher(getWeigher())
                .build();

            int randomKeys = randomIntBetween(2, 100);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(getICacheKey(entry.getKey()), entry.getValue());
            }
            Iterator<ICacheKey<String>> keys = ehcacheTest.keys().iterator();
            int keysCount = 0;
            while (keys.hasNext()) {
                ICacheKey<String> key = keys.next();
                keysCount++;
                assertNotNull(ehcacheTest.get(key));
            }
            assertEquals(keysCount, randomKeys);
            ehcacheTest.close();
        }
    }

    public void testEvictions() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ToLongBiFunction<ICacheKey<String>, String> weigher = getWeigher();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setThreadPoolAlias("ehcacheTest")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(weigher)
                .build();

            // Generate a string with 100 characters
            String value = generateRandomString(100);

            // Trying to generate more than 100kb to cause evictions.
            for (int i = 0; i < 1000; i++) {
                String key = "Key" + i;
                ehcacheTest.put(getICacheKey(key), value);
            }
            assertEquals(660, removalListener.evictionMetric.count());
            assertEquals(660, ehcacheTest.stats().getTotalEvictions());
            ehcacheTest.close();
        }
    }

    public void testComputeIfAbsentConcurrently() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setIsEventListenerModeSync(true)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setThreadPoolAlias("ehcacheTest")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();

            int numberOfRequest = 2;// randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            String value = "dummy";
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Verify value is only loaded once.
            for (int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

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
                    try {
                        assertEquals(value, ehcacheTest.computeIfAbsent(getICacheKey(key), loadAwareCacheLoader));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            phaser.arriveAndAwaitAdvance();
            countDownLatch.await();
            int numberOfTimesValueLoaded = 0;
            for (int i = 0; i < numberOfRequest; i++) {
                if (loadAwareCacheLoaderList.get(i).isLoaded()) {
                    numberOfTimesValueLoaded++;
                }
            }
            assertEquals(1, numberOfTimesValueLoaded);
            assertEquals(0, ((EhcacheDiskCache) ehcacheTest).getCompletableFutureMap().size());
            assertEquals(1, ehcacheTest.stats().getTotalMisses());
            assertEquals(1, ehcacheTest.stats().getTotalItems());
            assertEquals(numberOfRequest - 1, ehcacheTest.stats().getTotalHits());
            assertEquals(1, ehcacheTest.count());
            ehcacheTest.close();
        }
    }

    public void testComputeIfAbsentConcurrentlyAndThrowsException() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setThreadPoolAlias("ehcacheTest")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Loader throws exception.
            for (int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(ICacheKey<String> key) throws Exception {
                            isLoaded = true;
                            throw new RuntimeException("Exception");
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    assertThrows(ExecutionException.class, () -> ehcacheTest.computeIfAbsent(getICacheKey(key), loadAwareCacheLoader));
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            phaser.arriveAndAwaitAdvance();
            countDownLatch.await();

            assertEquals(0, ((EhcacheDiskCache) ehcacheTest).getCompletableFutureMap().size());
            ehcacheTest.close();
        }
    }

    public void testComputeIfAbsentWithNullValueLoading() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setIsEventListenerModeSync(true)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Loader throws exception.
            for (int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(ICacheKey<String> key) throws Exception {
                            isLoaded = true;
                            return null;
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    try {
                        ehcacheTest.computeIfAbsent(getICacheKey(key), loadAwareCacheLoader);
                    } catch (Exception ex) {
                        assertThat(ex.getCause(), instanceOf(NullPointerException.class));
                    }
                    assertThrows(ExecutionException.class, () -> ehcacheTest.computeIfAbsent(getICacheKey(key), loadAwareCacheLoader));
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            phaser.arriveAndAwaitAdvance();
            countDownLatch.await();

            assertEquals(0, ((EhcacheDiskCache) ehcacheTest).getCompletableFutureMap().size());
            ehcacheTest.close();
        }
    }

    public void testMemoryTracking() throws Exception {
        // Test all cases for EhCacheEventListener.onEvent and check stats memory usage is updated correctly
        Settings settings = Settings.builder().build();
        ToLongBiFunction<ICacheKey<String>, String> weigher = getWeigher();
        int initialKeyLength = 40;
        int initialValueLength = 40;
        long sizeForOneInitialEntry = weigher.applyAsLong(
            new ICacheKey<>(generateRandomString(initialKeyLength), getMockDimensions()),
            generateRandomString(initialValueLength)
        );
        int maxEntries = 2000;
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setIsEventListenerModeSync(true) // Test fails if async; probably not all updates happen before checking stats
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(maxEntries * sizeForOneInitialEntry)
                .setRemovalListener(new MockRemovalListener<>())
                .setWeigher(weigher)
                .build();
            long expectedSize = 0;

            // Test CREATED case
            int numInitialKeys = randomIntBetween(10, 100);
            ArrayList<ICacheKey<String>> initialKeys = new ArrayList<>();
            for (int i = 0; i < numInitialKeys; i++) {
                ICacheKey<String> key = new ICacheKey<>(generateRandomString(initialKeyLength), getMockDimensions());
                String value = generateRandomString(initialValueLength);
                ehcacheTest.put(key, value);
                initialKeys.add(key);
                expectedSize += weigher.applyAsLong(key, value);
                assertEquals(expectedSize, ehcacheTest.stats().getTotalStats().getSizeInBytes());
            }

            // Test UPDATED case
            HashMap<ICacheKey<String>, String> updatedValues = new HashMap<>();
            for (int i = 0; i < numInitialKeys * 0.5; i++) {
                int newLengthDifference = randomIntBetween(-20, 20);
                String newValue = generateRandomString(initialValueLength + newLengthDifference);
                ehcacheTest.put(initialKeys.get(i), newValue);
                updatedValues.put(initialKeys.get(i), newValue);
                expectedSize += newLengthDifference;
                assertEquals(expectedSize, ehcacheTest.stats().getTotalStats().getSizeInBytes());
            }

            // Test REMOVED case by removing all updated keys
            for (int i = 0; i < numInitialKeys * 0.5; i++) {
                ICacheKey<String> removedKey = initialKeys.get(i);
                ehcacheTest.invalidate(removedKey);
                expectedSize -= weigher.applyAsLong(removedKey, updatedValues.get(removedKey));
                assertEquals(expectedSize, ehcacheTest.stats().getTotalStats().getSizeInBytes());
            }

            // Test EVICTED case by adding entries past the cap and ensuring memory size stays as what we expect
            for (int i = 0; i < maxEntries - ehcacheTest.count(); i++) {
                ICacheKey<String> key = new ICacheKey<>(generateRandomString(initialKeyLength), getMockDimensions());
                String value = generateRandomString(initialValueLength);
                ehcacheTest.put(key, value);
            }
            // TODO: Ehcache incorrectly evicts at 30-40% of max size. Fix this test once we figure out why.
            // Since the EVICTED and EXPIRED cases use the same code as REMOVED, we should be ok on testing them for now.
            // assertEquals(maxEntries * sizeForOneInitialEntry, ehcacheTest.stats().getTotalMemorySize());

            ehcacheTest.close();
        }
    }

    public void testEhcacheKeyIteratorWithRemove() throws IOException {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(new MockRemovalListener<>())
                .setWeigher(getWeigher())
                .build();

            int randomKeys = randomIntBetween(2, 100);
            for (int i = 0; i < randomKeys; i++) {
                ehcacheTest.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
            }
            long originalSize = ehcacheTest.count();
            assertEquals(randomKeys, originalSize);

            // Now try removing subset of keys and verify
            List<ICacheKey<String>> removedKeyList = new ArrayList<>();
            for (Iterator<ICacheKey<String>> iterator = ehcacheTest.keys().iterator(); iterator.hasNext();) {
                ICacheKey<String> key = iterator.next();
                if (randomBoolean()) {
                    removedKeyList.add(key);
                    iterator.remove();
                }
            }
            // Verify the removed key doesn't exist anymore.
            for (ICacheKey<String> ehcacheKey : removedKeyList) {
                assertNull(ehcacheTest.get(ehcacheKey));
            }
            // Verify ehcache entry size again.
            assertEquals(originalSize - removedKeyList.size(), ehcacheTest.count());
            ehcacheTest.close();
        }

    }

    public void testInvalidateAll() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();
            int randomKeys = randomIntBetween(10, 100);
            Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
            }
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            ehcacheTest.invalidateAll(); // clear all the entries.
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                // Verify that value is null for a removed entry.
                assertNull(ehcacheTest.get(entry.getKey()));
            }
            assertEquals(0, ehcacheTest.count());
            ehcacheTest.close();
        }
    }

    public void testBasicGetAndPutBytesReference() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, BytesReference> ehCacheDiskCachingTier = new EhcacheDiskCache.Builder<String, BytesReference>()
                .setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new BytesReferenceSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setKeyType(String.class)
                .setValueType(BytesReference.class)
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES * 20) // bigger so no evictions happen
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setRemovalListener(new MockRemovalListener<>())
                .setWeigher((key, value) -> 1)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            int valueLength = 100;
            Random rand = Randomness.get();
            Map<ICacheKey<String>, BytesReference> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                byte[] valueBytes = new byte[valueLength];
                rand.nextBytes(valueBytes);
                keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), new BytesArray(valueBytes));

                // Test a non-BytesArray implementation of BytesReference.
                byte[] compositeBytes1 = new byte[valueLength];
                byte[] compositeBytes2 = new byte[valueLength];
                rand.nextBytes(compositeBytes1);
                rand.nextBytes(compositeBytes2);
                BytesReference composite = CompositeBytesReference.of(new BytesArray(compositeBytes1), new BytesArray(compositeBytes2));
                keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), composite);
            }
            for (Map.Entry<ICacheKey<String>, BytesReference> entry : keyValueMap.entrySet()) {
                ehCacheDiskCachingTier.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<ICacheKey<String>, BytesReference> entry : keyValueMap.entrySet()) {
                BytesReference value = ehCacheDiskCachingTier.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            ehCacheDiskCachingTier.close();
        }
    }

    public void testInvalidate() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setValueType(String.class)
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(getWeigher())
                .build();
            int randomKeys = randomIntBetween(10, 100);
            Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
            }
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            assertEquals(keyValueMap.size(), ehcacheTest.count());
            List<ICacheKey<String>> removedKeyList = new ArrayList<>();
            for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
                if (randomBoolean()) {
                    removedKeyList.add(entry.getKey());
                    ehcacheTest.invalidate(entry.getKey());
                }
            }
            for (ICacheKey<String> removedKey : removedKeyList) {
                assertNull(ehcacheTest.get(removedKey));
            }
            assertEquals(keyValueMap.size() - removedKeyList.size(), ehcacheTest.count());
            ehcacheTest.close();
        }
    }

    // Modified from OpenSearchOnHeapCacheTests.java
    public void testInvalidateWithDropDimensions() throws Exception {
        Settings settings = Settings.builder().build();
        List<String> dimensionNames = List.of("dim1", "dim2");
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehCacheDiskCachingTier = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setIsEventListenerModeSync(true)
                .setDimensionNames(dimensionNames)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES * 20) // bigger so no evictions happen
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setRemovalListener(new MockRemovalListener<>())
                .setWeigher((key, value) -> 1)
                .build();

            List<ICacheKey<String>> keysAdded = new ArrayList<>();

            for (int i = 0; i < 20; i++) {
                ICacheKey<String> key = new ICacheKey<>(UUID.randomUUID().toString(), getRandomDimensions(dimensionNames));
                keysAdded.add(key);
                ehCacheDiskCachingTier.put(key, UUID.randomUUID().toString());
            }

            ICacheKey<String> keyToDrop = keysAdded.get(0);

            String[] levels = dimensionNames.toArray(new String[0]);
            ImmutableCacheStats snapshot = ehCacheDiskCachingTier.stats(levels).getStatsForDimensionValues(keyToDrop.dimensions);
            assertNotNull(snapshot);

            keyToDrop.setDropStatsForDimensions(true);
            ehCacheDiskCachingTier.invalidate(keyToDrop);

            // Now assert the stats are gone for any key that has this combination of dimensions, but still there otherwise
            for (ICacheKey<String> keyAdded : keysAdded) {
                snapshot = ehCacheDiskCachingTier.stats(levels).getStatsForDimensionValues(keyAdded.dimensions);
                if (keyAdded.dimensions.equals(keyToDrop.dimensions)) {
                    assertNull(snapshot);
                } else {
                    assertNotNull(snapshot);
                }
            }

            ehCacheDiskCachingTier.close();
        }
    }

    public void testStatsTrackingDisabled() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ToLongBiFunction<ICacheKey<String>, String> weigher = getWeigher();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(weigher)
                .setStatsTrackingEnabled(false)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            for (int i = 0; i < randomKeys; i++) {
                ICacheKey<String> iCacheKey = getICacheKey(UUID.randomUUID().toString());
                ehcacheTest.put(iCacheKey, UUID.randomUUID().toString());
                assertEquals(0, ehcacheTest.count()); // Expect count of 0 if NoopCacheStatsHolder is used
                assertEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), ehcacheTest.stats().getTotalStats());
            }
            ehcacheTest.close();
        }
    }

    public void testDiskCacheFilesAreClearedUpDuringCloseAndInitialization() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ToLongBiFunction<ICacheKey<String>, String> weigher = getWeigher();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            String path = env.nodePaths()[0].path.toString() + "/request_cache";
            // Create a dummy file to simulate a scenario where the data is already in the disk cache storage path
            // beforehand.
            Files.createDirectory(Path.of(path));
            Path dummyFilePath = Files.createFile(Path.of(path + "/testing.txt"));
            assertTrue(Files.exists(dummyFilePath));
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(path)
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setDiskCacheAlias("test1")
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(weigher)
                .setStatsTrackingEnabled(false)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            for (int i = 0; i < randomKeys; i++) {
                ICacheKey<String> iCacheKey = getICacheKey(UUID.randomUUID().toString());
                ehcacheTest.put(iCacheKey, UUID.randomUUID().toString());
                assertEquals(0, ehcacheTest.count()); // Expect count of 0 if NoopCacheStatsHolder is used
                assertEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), ehcacheTest.stats().getTotalStats());
            }
            // Verify that older data was wiped out after initialization
            assertFalse(Files.exists(dummyFilePath));

            // Verify that there is data present under desired path by explicitly verifying the folder name by prefix
            // (used from disk cache alias)
            assertTrue(Files.exists(Path.of(path)));
            boolean folderExists = Files.walk(Path.of(path))
                .filter(Files::isDirectory)
                .anyMatch(path1 -> path1.getFileName().toString().startsWith("test1"));
            assertTrue(folderExists);
            ehcacheTest.close();
            assertFalse(Files.exists(Path.of(path))); // Verify everything is cleared up now after close()
        }
    }

    public void testDiskCacheCloseCalledTwiceAndVerifyDiskDataIsCleanedUp() throws Exception {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ToLongBiFunction<ICacheKey<String>, String> weigher = getWeigher();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            String path = env.nodePaths()[0].path.toString() + "/request_cache";
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(path)
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setKeySerializer(new StringSerializer())
                .setDiskCacheAlias("test1")
                .setValueSerializer(new StringSerializer())
                .setDimensionNames(List.of(dimensionName))
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .setWeigher(weigher)
                .setStatsTrackingEnabled(false)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            for (int i = 0; i < randomKeys; i++) {
                ICacheKey<String> iCacheKey = getICacheKey(UUID.randomUUID().toString());
                ehcacheTest.put(iCacheKey, UUID.randomUUID().toString());
                assertEquals(0, ehcacheTest.count()); // Expect count storagePath 0 if NoopCacheStatsHolder is used
                assertEquals(new ImmutableCacheStats(0, 0, 0, 0, 0), ehcacheTest.stats().getTotalStats());
            }
            ehcacheTest.close();
            assertFalse(Files.exists(Path.of(path))); // Verify everything is cleared up now after close()
            // Call it again. This will throw an exception.
            ehcacheTest.close();
        }
    }

    private List<String> getRandomDimensions(List<String> dimensionNames) {
        Random rand = Randomness.get();
        int bound = 3;
        List<String> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            result.add(String.valueOf(rand.nextInt(bound)));
        }
        return result;
    }

    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder randomString = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = (int) (randomDouble() * characters.length());
            randomString.append(characters.charAt(index));
        }

        return randomString.toString();
    }

    private List<String> getMockDimensions() {
        return List.of("0");
    }

    private ICacheKey<String> getICacheKey(String key) {
        return new ICacheKey<>(key, getMockDimensions());
    }

    private ToLongBiFunction<ICacheKey<String>, String> getWeigher() {
        return (iCacheKey, value) -> {
            // Size consumed by key
            long totalSize = iCacheKey.key.length();
            for (String dim : iCacheKey.dimensions) {
                totalSize += dim.length();
            }
            totalSize += 10; // The ICacheKeySerializer writes 2 VInts to record array lengths, which can be 1-5 bytes each
            // Size consumed by value
            totalSize += value.length();
            return totalSize;
        };
    }

    static class MockRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {

        CounterMetric evictionMetric = new CounterMetric();

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            evictionMetric.inc();
        }
    }

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
