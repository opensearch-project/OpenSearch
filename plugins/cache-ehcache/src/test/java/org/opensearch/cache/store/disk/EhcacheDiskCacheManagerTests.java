/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store.disk;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.function.Supplier;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;

public class EhcacheDiskCacheManagerTests extends OpenSearchSingleNodeTestCase {

    private static final String THREAD_POOL_ALIAS = "poolAlias";

    @SuppressWarnings("rawTypes")
    public void testCreateAndCloseCacheConcurrently() throws Exception {
        Settings settings = Settings.builder().build();
        String path = null;
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            path = env.nodePaths()[0].path.toString() + "/request_cache";
            EhcacheDiskCacheManager.getCacheManager(CacheType.INDICES_REQUEST_CACHE, path, settings, THREAD_POOL_ALIAS);
        }
        int randomThreads = randomIntBetween(5, 10);
        Thread[] threads = new Thread[randomThreads];
        Phaser phaser = new Phaser(randomThreads + 1);
        CountDownLatch countDownLatch = new CountDownLatch(randomThreads);
        List<String> diskCacheAliases = new ArrayList<>();
        for (int i = 0; i < randomThreads; i++) {
            threads[i] = new Thread(() -> {
                String diskCacheAlias = UUID.randomUUID().toString();
                diskCacheAliases.add(diskCacheAlias);
                phaser.arriveAndAwaitAdvance();
                EhcacheDiskCacheManager.createCache(CacheType.INDICES_REQUEST_CACHE, diskCacheAlias, getCacheConfigurationBuilder());
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(randomThreads, EhcacheDiskCacheManager.getCacheManagerMap().get(CacheType.INDICES_REQUEST_CACHE).v2().get());

        threads = new Thread[randomThreads];
        Phaser phaser2 = new Phaser(randomThreads + 1);
        CountDownLatch countDownLatch2 = new CountDownLatch(randomThreads);
        for (int i = 0; i < randomThreads; i++) {
            String finalPath = path;
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser2.arriveAndAwaitAdvance();
                EhcacheDiskCacheManager.closeCache(CacheType.INDICES_REQUEST_CACHE, diskCacheAliases.get(finalI), finalPath);
                countDownLatch2.countDown();
            });
            threads[i].start();
        }
        phaser2.arriveAndAwaitAdvance();
        countDownLatch2.await();

        assertNull(EhcacheDiskCacheManager.getCacheManagerMap().get(CacheType.INDICES_REQUEST_CACHE));
        assertFalse(EhcacheDiskCacheManager.doesCacheManagerExist(CacheType.INDICES_REQUEST_CACHE));
    }

    public void testCreateCacheWithNullArguments() {
        assertThrows(
            IllegalArgumentException.class,
            () -> EhcacheDiskCacheManager.createCache(CacheType.INDICES_REQUEST_CACHE, "test", null)
        );
    }

    public void testCreateCacheWithInvalidArgument() throws IOException {
        String cacheName = "test";
        String path = null;
        CacheConfigurationBuilder<String, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class,
            String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1024 * 100, MemoryUnit.B)
        );
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            path = env.nodePaths()[0].path.toString() + "/request_cache";
            EhcacheDiskCacheManager.getCacheManager(CacheType.INDICES_REQUEST_CACHE, path, Settings.EMPTY, THREAD_POOL_ALIAS);
        }
        EhcacheDiskCacheManager.createCache(CacheType.INDICES_REQUEST_CACHE, cacheName, cacheConfigurationBuilder);
        // Try creating cache with the same alias, should fail
        assertThrows(
            IllegalArgumentException.class,
            () -> EhcacheDiskCacheManager.createCache(CacheType.INDICES_REQUEST_CACHE, cacheName, cacheConfigurationBuilder)
        );
        EhcacheDiskCacheManager.closeCache(CacheType.INDICES_REQUEST_CACHE, cacheName, path);
    }

    public void testCreateCacheWithInvalidCacheSize() throws Exception {
        String cacheName = "test";
        String path = null;
        CacheConfigurationBuilder<String, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class,
            String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().disk(100, MemoryUnit.B)
        );
        try (NodeEnvironment env = newNodeEnvironment(Settings.EMPTY)) {
            path = env.nodePaths()[0].path.toString() + "/request_cache";
            EhcacheDiskCacheManager.getCacheManager(CacheType.INDICES_REQUEST_CACHE, path, Settings.EMPTY, THREAD_POOL_ALIAS);
        }
        assertThrows(
            IllegalStateException.class,
            () -> EhcacheDiskCacheManager.createCache(CacheType.INDICES_REQUEST_CACHE, cacheName, cacheConfigurationBuilder)
        );
        EhcacheDiskCacheManager.getCacheManagerMap().remove(CacheType.INDICES_REQUEST_CACHE); // Clear up
    }

    public void testCreateCacheWithCacheManagerDoesNotExist() {
        String cacheName = "test";
        CacheConfigurationBuilder<String, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class,
            String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1024 * 100, MemoryUnit.B)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> EhcacheDiskCacheManager.createCache(CacheType.INDICES_REQUEST_CACHE, cacheName, cacheConfigurationBuilder)
        );
    }

    private CacheConfigurationBuilder<String, String> getCacheConfigurationBuilder() {
        CacheConfigurationBuilder<String, String> cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class,
            String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1024 * 101, MemoryUnit.B)
        ).withExpiry(new ExpiryPolicy<>() {
            @Override
            public Duration getExpiryForCreation(String key, String value) {
                return null;
            }

            @Override
            public Duration getExpiryForAccess(String key, Supplier<? extends String> value) {
                return null;
            }

            @Override
            public Duration getExpiryForUpdate(String key, Supplier<? extends String> oldValue, String newValue) {
                return null;
            }
        })
            .withService(
                CacheEventListenerConfigurationBuilder.newEventListenerConfiguration(
                    new MockEhcacheEventListener<String>(),
                    EventType.EVICTED,
                    EventType.EXPIRED,
                    EventType.REMOVED,
                    EventType.UPDATED,
                    EventType.CREATED
                ).unordered().synchronous()
            )
            .withService(new OffHeapDiskStoreConfiguration(EhcacheDiskCacheManagerTests.THREAD_POOL_ALIAS, 1));
        return cacheConfigurationBuilder;
    }

    class MockEhcacheEventListener<String> implements CacheEventListener<String, String> {

        @Override
        public void onEvent(CacheEvent<? extends String, ? extends String> event) {}
    }
}
