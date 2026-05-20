/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.cache.CacheSettings;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;

/**
 * Tests for DataFusionService lifecycle and NativeRuntimeHandle.
 */
public class DataFusionServiceTests extends OpenSearchTestCase {

    private void ensureTokioInit() {
        NativeBridge.initTokioRuntimeManager(2);
    }

    public void testServiceStartStop() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .build();
        service.start();

        NativeRuntimeHandle handle = service.getNativeRuntime();
        assertNotNull(handle);
        assertTrue(handle.isOpen());
        assertTrue(handle.get() != 0);

        service.stop();
        assertFalse(handle.isOpen());
    }

    public void testGetNativeRuntimeBeforeStartThrows() {
        DataFusionService service = DataFusionService.builder().build();
        expectThrows(IllegalStateException.class, service::getNativeRuntime);
    }

    public void testNativeRuntimeHandleCloseIsIdempotent() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        long ptr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle handle = new NativeRuntimeHandle(ptr);

        assertTrue(handle.isOpen());
        handle.close();
        assertFalse(handle.isOpen());
        // Second close should not throw
        handle.close();
        assertFalse(handle.isOpen());
    }

    public void testNativeRuntimeHandleGetAfterCloseThrows() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        long ptr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle handle = new NativeRuntimeHandle(ptr);
        handle.close();
        expectThrows(IllegalStateException.class, handle::get);
    }

    public void testNativeRuntimeHandleRejectsZeroPointer() {
        expectThrows(IllegalArgumentException.class, () -> new NativeRuntimeHandle(0L));
    }

    public void testCacheFileOperationsDoNotThrow() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .build();
        service.start();

        // These are no-ops in the current Rust impl but should not throw
        service.onFilesAdded(java.util.List.of("/tmp/test1.parquet", "/tmp/test2.parquet"));
        service.onFilesDeleted(java.util.List.of("/tmp/test1.parquet"));
        service.onFilesAdded(null);
        service.onFilesDeleted(java.util.List.of());

        service.stop();
    }

    public void testServiceWithCacheEnabled() {
        ensureTokioInit();
        ClusterSettings clusterSettings = createCacheClusterSettings(Settings.EMPTY);
        Path spillDir = createTempDir("spill");

        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .clusterSettings(clusterSettings)
            .build();
        service.start();

        assertNotNull(service.getCacheManager());
        assertNotNull(service.getNativeRuntime());
        assertTrue(service.getNativeRuntime().isOpen());

        service.stop();
    }

    public void testServiceWithoutCacheReturnsNullCacheManager() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");

        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .build();
        service.start();

        assertNull(service.getCacheManager());

        service.stop();
    }

    public void testPluginRegistersAllCacheSettings() {
        List<Setting<?>> settings = new DataFusionPlugin().getSettings();
        assertTrue(settings.contains(CacheSettings.METADATA_CACHE_SIZE_LIMIT));
        assertTrue(settings.contains(CacheSettings.STATISTICS_CACHE_SIZE_LIMIT));
        assertTrue(settings.contains(CacheSettings.METADATA_CACHE_EVICTION_TYPE));
        assertTrue(settings.contains(CacheSettings.STATISTICS_CACHE_EVICTION_TYPE));
        assertTrue(settings.contains(CacheSettings.METADATA_CACHE_ENABLED));
        assertTrue(settings.contains(CacheSettings.STATISTICS_CACHE_ENABLED));
    }

    public void testNativeBridgeCacheManagerLifecycle() {
        ensureTokioInit();
        long ptr = NativeBridge.createCustomCacheManager();
        assertTrue(ptr != 0);
        NativeBridge.destroyCustomCacheManager(ptr);
    }

    public void testNativeBridgeCreateCacheOnManager() {
        ensureTokioInit();
        long ptr = NativeBridge.createCustomCacheManager();
        NativeBridge.createCache(ptr, "METADATA", 250 * 1024 * 1024, "LRU");
        NativeBridge.createCache(ptr, "STATISTICS", 100 * 1024 * 1024, "LRU");
        NativeBridge.destroyCustomCacheManager(ptr);
    }

    public void testRuntimeWithCacheManagerPointer() {
        ensureTokioInit();
        long cachePtr = NativeBridge.createCustomCacheManager();
        NativeBridge.createCache(cachePtr, "METADATA", 250 * 1024 * 1024, "LRU");
        NativeBridge.createCache(cachePtr, "STATISTICS", 100 * 1024 * 1024, "LRU");

        Path spillDir = createTempDir("spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, cachePtr, spillDir.toString(), 32 * 1024 * 1024);
        assertTrue(runtimePtr != 0);

        NativeBridge.closeGlobalRuntime(runtimePtr);
    }

    private ClusterSettings createCacheClusterSettings(Settings settings) {
        Set<Setting<?>> all = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        all.add(CacheSettings.METADATA_CACHE_ENABLED);
        all.add(CacheSettings.METADATA_CACHE_SIZE_LIMIT);
        all.add(CacheSettings.METADATA_CACHE_EVICTION_TYPE);
        all.add(CacheSettings.STATISTICS_CACHE_ENABLED);
        all.add(CacheSettings.STATISTICS_CACHE_SIZE_LIMIT);
        all.add(CacheSettings.STATISTICS_CACHE_EVICTION_TYPE);
        all.add(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT);
        all.add(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT);
        return new ClusterSettings(settings, all);
    }
}
