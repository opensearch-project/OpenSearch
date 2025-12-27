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
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            final String path = env.nodePaths()[0].path.resolve("request_cache").toString();
            EhcacheDiskCacheManager.getCacheManager(CacheType.INDICES_REQUEST_CACHE, path, settings, THREAD_POOL_ALIAS);
            final int threadCount = randomIntBetween(5, 10);
            final String[] diskCacheAliases = new String[threadCount];
            // ---- Phase 1: create caches concurrently ----
            final Thread[] createThreads = new Thread[threadCount];
            final Phaser createPhaser = new Phaser(threadCount + 1);
            final CountDownLatch createdLatch = new CountDownLatch(threadCount);
            final java.util.concurrent.atomic.AtomicReference<Throwable> createFailure =
                new java.util.concurrent.atomic.AtomicReference<>();

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                createThreads[i] = new Thread(() -> {
                    diskCacheAliases[idx] = UUID.randomUUID().toString();
                    createPhaser.arriveAndAwaitAdvance();
                    try {
                        EhcacheDiskCacheManager.createCache(
                            CacheType.INDICES_REQUEST_CACHE,
                            diskCacheAliases[idx],
                            getCacheConfigurationBuilder()
                        );
                    } catch (Throwable t) {
                        createFailure.compareAndSet(null, t);
                    } finally {
                        createdLatch.countDown();
                    }
                }, "cache-create-" + i);
                createThreads[i].start();
            }

            createPhaser.arriveAndAwaitAdvance();
            assertTrue("Timed out waiting for cache creation threads", createdLatch.await(60, java.util.concurrent.TimeUnit.SECONDS));
            if (createFailure.get() != null) {
                throw new AssertionError("Failure in cache creation thread", createFailure.get());
            }

            assertEquals(threadCount, EhcacheDiskCacheManager.getCacheManagerMap().get(CacheType.INDICES_REQUEST_CACHE).v2().get());
            // ---- Phase 2: close caches concurrently ----
            final Thread[] closeThreads = new Thread[threadCount];
            final Phaser closePhaser = new Phaser(threadCount + 1);
            final CountDownLatch closedLatch = new CountDownLatch(threadCount);
            final java.util.concurrent.atomic.AtomicReference<Throwable> closeFailure = new java.util.concurrent.atomic.AtomicReference<>();
            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                closeThreads[i] = new Thread(() -> {
                    closePhaser.arriveAndAwaitAdvance();
                    try {
                        EhcacheDiskCacheManager.closeCache(CacheType.INDICES_REQUEST_CACHE, diskCacheAliases[idx], path);
                    } catch (Throwable t) {
                        closeFailure.compareAndSet(null, t);
                    } finally {
                        closedLatch.countDown();
                    }
                }, "cache-close-" + i);
                closeThreads[i].start();
            }
            closePhaser.arriveAndAwaitAdvance();
            assertTrue("Timed out waiting for cache close threads", closedLatch.await(60, java.util.concurrent.TimeUnit.SECONDS));
            if (closeFailure.get() != null) {
                throw new AssertionError("Failure in cache close thread", closeFailure.get());
            }
            assertNull(EhcacheDiskCacheManager.getCacheManagerMap().get(CacheType.INDICES_REQUEST_CACHE));
            assertFalse(EhcacheDiskCacheManager.doesCacheManagerExist(CacheType.INDICES_REQUEST_CACHE));
        }
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
