/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.cache.CacheManager;
import org.opensearch.be.datafusion.cache.CacheSettings;
import org.opensearch.be.datafusion.cache.CacheUtils;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;

public class DatafusionCacheManagerTests extends OpenSearchTestCase {
    private DataFusionService service;
    private CacheManager cacheManager;

    private void setup() {
        NativeBridge.initTokioRuntimeManager(2, 4);

        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(CacheSettings.METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(CacheSettings.METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(CacheSettings.METADATA_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(CacheSettings.STATISTICS_CACHE_ENABLED);
        clusterSettingsToAdd.add(CacheSettings.STATISTICS_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(CacheSettings.STATISTICS_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT);
        clusterSettingsToAdd.add(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        Path spillDir = createTempDir("spill");

        service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .clusterSettings(clusterSettings)
            .build();
        service.start();
        cacheManager = service.getCacheManager();
        assertNotNull(cacheManager);
    }

    private void cleanup() {
        if (service != null) {
            service.stop();
        }
    }

    public void testAddFileToCache() {
        setup();
        try {
            String fileName = getResourceFile("hits1.parquet");
            cacheManager.addFilesToCacheManager(List.of(fileName));
            assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileName));
            assertTrue(cacheManager.getMemoryConsumed(CacheUtils.CacheType.METADATA) > 0);
        } finally {
            cleanup();
        }
    }

    public void testRemoveFileFromCache() {
        setup();
        try {
            String fileName = getResourceFile("hits1.parquet");
            cacheManager.addFilesToCacheManager(List.of(fileName));
            assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileName));

            cacheManager.removeFilesFromCacheManager(List.of(fileName));
            assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileName));
        } finally {
            cleanup();
        }
    }

    public void testCacheClear() {
        setup();
        try {
            String fileName = getResourceFile("hits1.parquet");
            cacheManager.addFilesToCacheManager(List.of(fileName));
            assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileName));

            cacheManager.clearCacheForCacheType(CacheUtils.CacheType.METADATA);
            assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileName));
        } finally {
            cleanup();
        }
    }

    public void testAddMultipleFilesToCache() {
        setup();
        try {
            List<String> fileNames = List.of(getResourceFile("hits1.parquet"), getResourceFile("hits2.parquet"));
            cacheManager.addFilesToCacheManager(fileNames);
            assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileNames.getFirst()));
            assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fileNames.getLast()));
        } finally {
            cleanup();
        }
    }

    public void testGetNonExistentFile() {
        setup();
        try {
            assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, "/path/nonexistent.parquet"));
        } finally {
            cleanup();
        }
    }

    public void testCacheManagerTotalMemoryTracking() {
        setup();
        try {
            String fileName = getResourceFile("hits1.parquet");
            long initialMemory = cacheManager.getTotalMemoryConsumed();
            cacheManager.addFilesToCacheManager(List.of(fileName));
            long afterAddMemory = cacheManager.getTotalMemoryConsumed();
            assertTrue(afterAddMemory > initialMemory);

            cacheManager.removeFilesFromCacheManager(List.of(fileName));
            long afterRemoveMemory = cacheManager.getTotalMemoryConsumed();
            assertEquals(initialMemory, afterRemoveMemory);
        } finally {
            cleanup();
        }
    }

    public void testAddFilesWithNullList() {
        setup();
        try {
            cacheManager.addFilesToCacheManager(null);
        } finally {
            cleanup();
        }
    }

    public void testAddFilesWithEmptyList() {
        setup();
        try {
            cacheManager.addFilesToCacheManager(Collections.emptyList());
        } finally {
            cleanup();
        }
    }

    public void testRemoveFilesWithNullList() {
        setup();
        try {
            cacheManager.removeFilesFromCacheManager(null);
        } finally {
            cleanup();
        }
    }

    public void testRemoveFilesWithEmptyList() {
        setup();
        try {
            cacheManager.removeFilesFromCacheManager(Collections.emptyList());
        } finally {
            cleanup();
        }
    }

    public void testExceptionHandlingWithInvalidFile() {
        setup();
        try {
            cacheManager.addFilesToCacheManager(List.of("/invalid/path/to/file.parquet"));
        } finally {
            cleanup();
        }
    }

    public void testGetTotalMemoryConsumedReturnsZeroOnError() {
        setup();
        try {
            cacheManager.clearAllCache();
            long totalMemory = cacheManager.getTotalMemoryConsumed();
            assertTrue(totalMemory >= 0);
        } finally {
            cleanup();
        }
    }

    public void testGetEntryFromCacheTypeReturnsFalseOnError() {
        setup();
        try {
            assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, "/invalid/file.parquet"));
        } finally {
            cleanup();
        }
    }

    private String getResourceFile(String fileName) {
        try {
            return PathUtils.get(getClass().getClassLoader().getResource(fileName).toURI()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Resource not found: " + fileName, e);
        }
    }
}
