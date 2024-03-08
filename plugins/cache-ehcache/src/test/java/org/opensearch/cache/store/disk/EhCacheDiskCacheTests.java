/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store.disk;

import org.opensearch.cache.EhcacheDiskCacheSettings;
import org.opensearch.cache.keystore.DummyKeystore;
import org.opensearch.cache.keystore.RBMIntKeyLookupStore;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;

import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_LISTENER_MODE_SYNC_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_MAX_SIZE_IN_BYTES_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.DISK_STORAGE_PATH_KEY;
import static org.opensearch.cache.EhcacheDiskCacheSettings.USE_KEYSTORE_KEY;
import static org.hamcrest.CoreMatchers.instanceOf;

public class EhCacheDiskCacheTests extends OpenSearchSingleNodeTestCase {

    private static final int CACHE_SIZE_IN_BYTES = 1024 * 101;

    public void testBasicGetAndPut() throws IOException {
        Settings settings = Settings.builder().build();
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setThreadPoolAlias("ehcacheTest")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setKeyType(String.class)
                .setValueType(String.class)
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();
            int randomKeys = randomIntBetween(10, 100);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, ehcacheTest.count());

            // Validate misses
            int expectedNumberOfMisses = randomIntBetween(10, 200);
            for (int i = 0; i < expectedNumberOfMisses; i++) {
                ehcacheTest.get(UUID.randomUUID().toString());
            }

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
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, ehcacheTest.count());

            // Validate misses
            int expectedNumberOfMisses = randomIntBetween(10, 200);
            for (int i = 0; i < expectedNumberOfMisses; i++) {
                ehcacheTest.get(UUID.randomUUID().toString());
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
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
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
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String value = ehcacheTest.get(entry.getKey());
                assertEquals(entry.getValue(), value);
            }
            assertEquals(randomKeys, ehcacheTest.count());
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
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
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
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            assertEquals(keyValueMap.size(), ehcacheTest.count());
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    assertEquals(entry.getValue(), ehcacheTest.get(entry.getKey()));
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
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
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(new MockRemovalListener<>())
                .build();

            int randomKeys = randomIntBetween(2, 100);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehcacheTest.put(entry.getKey(), entry.getValue());
            }
            Iterator<String> keys = ehcacheTest.keys().iterator();
            int keysCount = 0;
            while (keys.hasNext()) {
                String key = keys.next();
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
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ICache<String, String> ehcacheTest = new EhcacheDiskCache.Builder<String, String>().setDiskCacheAlias("test1")
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setIsEventListenerModeSync(true)
                .setThreadPoolAlias("ehcacheTest")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();

            // Generate a string with 100 characters
            String value = generateRandomString(100);

            // Trying to generate more than 100kb to cause evictions.
            for (int i = 0; i < 1000; i++) {
                String key = "Key" + i;
                ehcacheTest.put(key, value);
            }
            assertEquals(660, removalListener.evictionMetric.count());
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
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();

            int numberOfRequest = 2;// randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            String value = "dummy";
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Verify value is only loaded once.
            for (int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(String key) {
                            isLoaded = true;
                            return value;
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    try {
                        assertEquals(value, ehcacheTest.computeIfAbsent(key, loadAwareCacheLoader));
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
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Loader throws exception.
            for (int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(String key) throws Exception {
                            isLoaded = true;
                            throw new RuntimeException("Exception");
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    assertThrows(ExecutionException.class, () -> ehcacheTest.computeIfAbsent(key, loadAwareCacheLoader));
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
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(settings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();

            int numberOfRequest = randomIntBetween(200, 400);
            String key = UUID.randomUUID().toString();
            Thread[] threads = new Thread[numberOfRequest];
            Phaser phaser = new Phaser(numberOfRequest + 1);
            CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

            List<LoadAwareCacheLoader<String, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

            // Try to hit different request with the same key concurrently. Loader throws exception.
            for (int i = 0; i < numberOfRequest; i++) {
                threads[i] = new Thread(() -> {
                    LoadAwareCacheLoader<String, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                        boolean isLoaded;

                        @Override
                        public boolean isLoaded() {
                            return isLoaded;
                        }

                        @Override
                        public String load(String key) throws Exception {
                            isLoaded = true;
                            return null;
                        }
                    };
                    loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                    phaser.arriveAndAwaitAdvance();
                    try {
                        ehcacheTest.computeIfAbsent(key, loadAwareCacheLoader);
                    } catch (Exception ex) {
                        assertThat(ex.getCause(), instanceOf(NullPointerException.class));
                    }
                    assertThrows(ExecutionException.class, () -> ehcacheTest.computeIfAbsent(key, loadAwareCacheLoader));
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

    public void testKeystoreSettings() throws Exception {
        Settings useRBMsettings = Settings.builder()
            .put(
                EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE).get(USE_KEYSTORE_KEY).getKey(),
                RBMIntKeyLookupStore.KEYSTORE_NAME
            )
            .build();

        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            EhcacheDiskCache<String, String> ehcacheTest = (EhcacheDiskCache<String, String>) new EhcacheDiskCache.Builder<String, String>()
                .setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setIsEventListenerModeSync(true)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(useRBMsettings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();
            assertEquals(RBMIntKeyLookupStore.class, ehcacheTest.getKeystore().getClass());
            ehcacheTest.close();
        }

        Settings useDummySettings = Settings.builder()
            .put(
                EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE).get(USE_KEYSTORE_KEY).getKey(),
                "unrecognized_name"
            )
            .build();

        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            EhcacheDiskCache<String, String> ehcacheTest = (EhcacheDiskCache<String, String>) new EhcacheDiskCache.Builder<String, String>()
                .setDiskCacheAlias("test1")
                .setThreadPoolAlias("ehcacheTest")
                .setIsEventListenerModeSync(true)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeyType(String.class)
                .setValueType(String.class)
                .setCacheType(CacheType.INDICES_REQUEST_CACHE)
                .setSettings(useDummySettings)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setRemovalListener(removalListener)
                .build();
            assertEquals(DummyKeystore.class, ehcacheTest.getKeystore().getClass());

            // Also test the dummy keystore doesn't incorrectly block any gets
            int numKeys = 50;
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < numKeys; i++) {
                String key = generateRandomString(50);
                String value = generateRandomString(50);
                keyValueMap.put(key, value);
                ehcacheTest.put(key, value);
            }
            for (String key : keyValueMap.keySet()) {
                assertEquals(keyValueMap.get(key), ehcacheTest.get(key));
            }
            ehcacheTest.close();
        }

        // Check the factory correctly gives RBM keystore
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            ICache.Factory ehcacheFactory = new EhcacheDiskCache.EhcacheDiskCacheFactory();
            EhcacheDiskCache<String, String> ehcacheTest = (EhcacheDiskCache<String, String>) ehcacheFactory.create(
                new CacheConfig.Builder<String, String>().setValueType(String.class)
                    .setKeyType(String.class)
                    .setRemovalListener(removalListener)
                    .setSettings(
                        Settings.builder()
                            .put(
                                EhcacheDiskCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                                    .get(DISK_STORAGE_PATH_KEY)
                                    .getKey(),
                                env.nodePaths()[0].indicesPath.toString() + "/request_cache"
                            )
                            .build()
                    )
                    .build(),
                CacheType.INDICES_REQUEST_CACHE,
                Map.of()
            );
            assertEquals(RBMIntKeyLookupStore.class, ehcacheTest.getKeystore().getClass());
            ehcacheTest.close();
        }
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

    static class MockRemovalListener<K, V> implements RemovalListener<K, V> {

        CounterMetric evictionMetric = new CounterMetric();

        @Override
        public void onRemoval(RemovalNotification<K, V> notification) {
            evictionMetric.inc();
        }
    }
}
