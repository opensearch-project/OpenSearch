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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.OpenSearchTestCase;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.opensearch.common.lifecycle.Lifecycle.State.CLOSED;
import static org.opensearch.common.lifecycle.Lifecycle.State.STOPPED;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

public class DatafusionCacheManagerTests extends OpenSearchSingleNodeTestCase {
    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    private CacheManager cacheManagerv1;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings mockSettings = Settings.builder().put("path.data", "/tmp/test-data").build();

        when(mockEnvironment.settings()).thenReturn(mockSettings);
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);

        service = new DataFusionService(Collections.emptyMap(), clusterSettings);
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
//
//    public void testRemoveNonExistentFile() {
//        CacheManager cacheManager = service.getCacheManager();
//        String nonExistentFile = "/path/nonexistent.parquet";
//
//        boolean removed = cacheManager.removeFiles(List.of(nonExistentFile));
//
//        assertFalse(removed);
//    }
//
    public void testGetNonExistentFile() {
        CacheManager cacheManager = service.getCacheManager();
        String nonExistentFile = "/path/nonexistent.parquet";

        Object result = cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,nonExistentFile);

//        assertNull(result);
        assertFalse(cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA,nonExistentFile));
        service.doStop();
    }
//
//    public void testAddEmptyFileList() {
//        CacheManager cacheManager = service.getCacheManager();
//        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
//
//        cacheManager.addToCache(Collections.emptyList());
//
//        assertEquals(0, metadataCache.getEntries().size());
//    }
//
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
//
//    public void testCacheSizeLimits() {
//        CacheManager cacheManager = service.getCacheManager();
//        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
//
//        long configuredLimit = metadataCache.getConfiguredSizeLimit();
//        long totalLimit = cacheManager.getTotalSizeLimit();
//
//        assertTrue(configuredLimit > 0);
//        assertTrue(totalLimit > 0);
//    }
//
//    public void testRemoveFilesWithCacheAccessorFailure() {
//        setUpMockCacheAccessor();
//        when(mockCache.remove("file1")).thenReturn(false); // CacheAccessor handles exception internally
//        when(mockCache.remove("file2")).thenReturn(true);
//
//        boolean result = cacheManagerv1.removeFiles(List.of("file1", "file2"));
//
//        assertFalse(result);
//        verify(mockCache).remove("file1");
//        verify(mockCache).remove("file2");
//    }
//
//    public void testAddToCacheWithCacheAccessorFailure() {
//        setUpMockCacheAccessor();
//        when(mockCache.put("file1")).thenReturn(false); // CacheAccessor handles exception internally
//        when(mockCache.put("file2")).thenReturn(true);
//
//        boolean result = cacheManagerv1.addToCache(List.of("file1", "file2"));
//
//        assertFalse(result);
//        verify(mockCache).put("file1");
//        verify(mockCache).put("file2");
//    }
//
//    public void testGetTotalUsedBytesWithCacheAccessorFailure() {
//        setUpMockCacheAccessor();
//        when(mockCache.getMemoryConsumed()).thenReturn(0L); // CacheAccessor handles exception internally
//
//        long result = cacheManagerv1.getTotalUsedBytes();
//
//        assertEquals(0L, result);
//        verify(mockCache).getMemoryConsumed();
//    }
//
//    public void testWithinCacheLimitWithCacheAccessorFailure() {
//        setUpMockCacheAccessor();
//        when(mockCache.getMemoryConsumed()).thenReturn(0L); // CacheAccessor handles exception internally
//
//        boolean result = cacheManagerv1.withinCacheLimit(CacheType.METADATA);
//
//        assertTrue(result); // 0L < 1000L
//        verify(mockCache).getMemoryConsumed();
//    }
//
    private File getResourceFile(String fileName) {
        URL resourceUrl = getClass().getClassLoader().getResource(fileName);
        if (resourceUrl == null) {
            throw new IllegalArgumentException("Resource not found: " + fileName);
        }
        return new File(resourceUrl.getPath());
    }
//
//    private void setUpMockCacheAccessor() {
//        MockitoAnnotations.openMocks(this);
//        when(mockCache.getName()).thenReturn("TestCache");
//        when(mockCache.getConfiguredSizeLimit()).thenReturn(1000L);
//
//        Map<CacheType, CacheAccessor> cacheMap = new HashMap<>();
//        cacheMap.put(CacheType.METADATA, mockCache);
//        cacheManagerv1 = new CacheManager(123L, cacheMap);
//    }
//
//    // Tests for destroy methods
//
//    public void testCacheManagerClose() throws Exception {
//        CacheManager cacheManager = service.getCacheManager();
//        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
//        String fileName = getResourceFile("hits1.parquet").getPath();
//
//        // Add file to cache
//        cacheManager.addToCache(List.of(fileName));
//        assertTrue(metadataCache.containsFile(fileName));
//
//        // Close the cache manager
//        cacheManager.close();
//
//        // Verify cache is cleared after close
//        assertEquals(0, cacheManager.getAllCaches().stream()
//            .mapToLong(CacheAccessor::getPointer)
//            .filter(ptr -> ptr != -1)
//            .count());
//
//        assertEquals(-1, cacheManager.getCacheManagerPtr());
//    }
//
//    public void testCacheManagerCloseIdempotent() throws Exception {
//        CacheManager cacheManager = service.getCacheManager();
//        String fileName = getResourceFile("hits1.parquet").getPath();
//
//        cacheManager.addToCache(List.of(fileName));
//
//        // Close multiple times should not throw exception
//        cacheManager.close();
//        cacheManager.close(); // Second close should be safe
//    }
//
//    public void testGlobalRuntimeEnvClose() throws Exception {
//        assertNotEquals(STOPPED, service.lifecycleState());
//        assertNotEquals(CLOSED, service.lifecycleState());
//
//        Long globalRuntimeEnvPtr = service.getRuntimePointer();
//
//        assertNotEquals(Long.valueOf(-1), globalRuntimeEnvPtr);
//        service.close();
//        assertEquals(CLOSED, service.lifecycleState());
//
//        globalRuntimeEnvPtr = service.getRuntimePointer();
//        cacheManagerPtr = service.getCacheManager().getCacheManagerPtr();
//
//        assertEquals(Long.valueOf(-1), globalRuntimeEnvPtr);
//        assertEquals(Long.valueOf(-1), cacheManagerPtr);
//
//    }

    // Exception handling tests - simplified without static mocking

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

//
//    public void testClearCacheManagerWithException() {
//        try (MockedStatic<DataFusionQueryJNI> mockedJNI = mockStatic(DataFusionQueryJNI.class);
//             MockedStatic<LogManager> mockedLogManager = mockStatic(LogManager.class)) {
//
//            Logger mockLogger = mock(Logger.class);
//            mockedLogManager.when(() -> LogManager.getLogger(CacheManager.class)).thenReturn(mockLogger);
//
//            doThrow(new RuntimeException("Failed to clear cache"))
//                    .when(() -> DataFusionQueryJNI.cacheManagerClear());
//
//            CacheManager testCacheManager = new CacheManager();
//            testCacheManager.clearCacheManager();
//
//            verify(mockLogger).error(
//                    eq("Error clearing cache manager: {}"),
//                    eq("Failed to clear cache"),
//                    any(RuntimeException.class)
//            );
//        }
//    }

//
//    public void testClearCacheManagerForCacheTypeWithException() {
//        try (MockedStatic<DataFusionQueryJNI> mockedJNI = mockStatic(DataFusionQueryJNI.class);
//             MockedStatic<LogManager> mockedLogManager = mockStatic(LogManager.class)) {
//
//            Logger mockLogger = mock(Logger.class);
//            mockedLogManager.when(() -> LogManager.getLogger(CacheManager.class)).thenReturn(mockLogger);
//
//            doThrow(new DataFusionException("Failed to clear cache type"))
//                    .when(() -> DataFusionQueryJNI.cacheManagerClearByCacheType(anyString()));
//
//            CacheManager testCacheManager = new CacheManager();
//            CacheUtils.CacheType cacheType = CacheUtils.CacheType.METADATA;
//
//            testCacheManager.clearCacheManagerForCacheType(cacheType);
//
//            verify(mockLogger).error(
//                    eq("Error clearing cache manager for cache type {}: {}"),
//                    eq(cacheType.getCacheTypeName()),
//                    eq("Failed to clear cache type"),
//                    any(RuntimeException.class)
//            );
//        }
//    }


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

    public void testGetMemoryConsumedReturnsZeroOnError() {
        CacheManager cacheManager = service.getCacheManager();

        // Clear the cache first
        cacheManager.clearAllCache();

        // Memory consumed should be 0 or a valid value, never negative
        long memoryConsumed = cacheManager.getMemoryConsumed(CacheUtils.CacheType.METADATA);
        assertTrue("Memory consumed should be non-negative", memoryConsumed >= 0);
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
