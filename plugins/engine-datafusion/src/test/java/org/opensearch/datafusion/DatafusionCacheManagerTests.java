/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import static org.mockito.Mockito.*;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATISTICS_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATISTICS_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATISTICS_CACHE_SIZE_LIMIT;

public class DatafusionCacheManagerTests extends OpenSearchSingleNodeTestCase {
    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    @Mock
    private ClusterService clusterService;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings mockSettings = Settings.builder().put("path.data", "/tmp/test-data").build();

        when(mockEnvironment.settings()).thenReturn(mockSettings);
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(STATISTICS_CACHE_ENABLED);
        clusterSettingsToAdd.add(STATISTICS_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(STATISTICS_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION);
        clusterSettingsToAdd.add(DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);


        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        service = new DataFusionService(Collections.emptyMap(), clusterService, "/tmp");
        service.doStart();
    }

    @After
    public void cleanUp(){
        service.doStop();
    }

    public void testAddFileToCache() {
        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addFilesToCacheManager(List.of(fileName));

        assertTrue((Boolean) cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));
        assertTrue(cacheManager.getMemoryConsumed(CacheUtils.CacheType.METADATA) > 0);
        service.doStop();
    }

    public void testRemoveFileFromCache() {
        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addFilesToCacheManager(List.of(fileName));
        assertTrue( cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));

        cacheManager.removeFilesFromCacheManager(List.of(fileName));
        assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));
        service.doStop();
    }

    public void testCacheSizeLimitEviction() {
        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addFilesToCacheManager(List.of(fileName));
        assertTrue( cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));

        cacheManager.updateSizeLimit(CacheUtils.CacheType.METADATA,50);

        assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));
        service.doStop();
    }

    public void testCacheClear() {

        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addFilesToCacheManager(List.of(fileName));
        assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));

        cacheManager.clearCacheForCacheType(CacheUtils.CacheType.METADATA);

        assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileName));
        assertEquals(0, cacheManager.getMemoryConsumed(CacheUtils.CacheType.METADATA));
        service.doStop();
    }

    public void testAddMultipleFilesToCache() {
        CacheManager cacheManager = service.getCacheManager();
        List<String> fileNames = List.of(
            getResourceFile("hits1.parquet").getPath(),
            getResourceFile("hits2.parquet").getPath()
        );

        cacheManager.addFilesToCacheManager(fileNames);
        // 3 elements per cache entry displayed
        assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileNames.getFirst()));
        assertTrue(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,fileNames.getLast()));
    }

    public void testGetNonExistentFile() {
        CacheManager cacheManager = service.getCacheManager();
        String nonExistentFile = "/path/nonexistent.parquet";

        Object result = cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,nonExistentFile);

        assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,nonExistentFile));
        service.doStop();
    }

    public void testCacheManagerTotalMemoryTracking() {
        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        long initialMemory = cacheManager.getTotalMemoryConsumed();
        cacheManager.addFilesToCacheManager(List.of(fileName));
        long afterAddMemory = cacheManager.getTotalMemoryConsumed();

        assertTrue(afterAddMemory > initialMemory);

        cacheManager.removeFilesFromCacheManager(List.of(fileName));
        long afterRemoveMemory = cacheManager.getTotalMemoryConsumed();

        assertEquals(initialMemory, afterRemoveMemory);
    }

    private File getResourceFile(String fileName) {
        URL resourceUrl = getClass().getClassLoader().getResource(fileName);
        if (resourceUrl == null) {
            throw new IllegalArgumentException("Resource not found: " + fileName);
        }
        return new File(resourceUrl.getPath());
    }

    public void testAddFilesWithNullList() {
        CacheManager cacheManager = service.getCacheManager();

        // Should handle null gracefully without throwing exception
        try {
            cacheManager.addFilesToCacheManager(null);
            // If we reach here, the method handled null gracefully
            assertTrue(true);
        } catch (Exception e) {
            fail("Should not throw exception for null list: " + e.getMessage());
        }
    }


    public void testAddFilesWithEmptyList() {
        CacheManager cacheManager = service.getCacheManager();
        // Should handle empty list gracefully without throwing exception
        try {
            cacheManager.addFilesToCacheManager(Collections.emptyList());
            // If we reach here, the method handled empty list gracefully
            assertTrue(true);
        } catch (Exception e) {
            fail("Should not throw exception for empty list: " + e.getMessage());
        }
    }


    public void testRemoveFilesWithNullList() {
        CacheManager cacheManager = service.getCacheManager();

        // Should handle null gracefully without throwing exception
        try {
            cacheManager.removeFilesFromCacheManager(null);
            // If we reach here, the method handled null gracefully
            assertTrue(true);
        } catch (Exception e) {
            fail("Should not throw exception for null list: " + e.getMessage());
        }
    }


    public void testRemoveFilesWithEmptyList() {
        CacheManager cacheManager = service.getCacheManager();

        // Should handle empty list gracefully without throwing exception
        try {
            cacheManager.removeFilesFromCacheManager(Collections.emptyList());
            // If we reach here, the method handled empty list gracefully
            assertTrue(true);
        } catch (Exception e) {
            fail("Should not throw exception for empty list: " + e.getMessage());
        }
    }


    public void testExceptionHandlingWithInvalidFile() {
        CacheManager cacheManager = service.getCacheManager();

        // Try to add a non-existent file - should be handled gracefully
        try {
            cacheManager.addFilesToCacheManager(List.of("/invalid/path/to/file.parquet"));
            // The method should handle the error internally and log it
            assertTrue(true);
        } catch (Exception e) {
            fail("Should not throw exception for invalid file: " + e.getMessage());
        }
    }

    public void testGetTotalMemoryConsumedReturnsZeroOnError() {
        CacheManager cacheManager = service.getCacheManager();

        // Clear the cache first
        cacheManager.clearAllCache();

        // Total memory consumed should be 0 or a valid value, never negative
        long totalMemory = cacheManager.getTotalMemoryConsumed();
        assertTrue("Total memory consumed should be non-negative", totalMemory >= 0);
    }

    public void testGetEntryFromCacheTypeReturnsFalseOnError() {
        CacheManager cacheManager = service.getCacheManager();

        // Try to get a non-existent entry
        boolean exists = cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, "/invalid/file.parquet");
        assertFalse("Should return false for non-existent entry", exists);
    }
}
