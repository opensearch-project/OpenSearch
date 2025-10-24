/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.datafusion.search.cache.CacheAccessor;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheType;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

public class DataFusionCacheManagerTests extends OpenSearchTestCase {
    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

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

    public void testAddFileToCache() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addToCache(List.of(fileName));

        assertTrue((Boolean) metadataCache.get(fileName));
        assertTrue(metadataCache.containsFile(fileName));
        assertTrue(metadataCache.getMemoryConsumed() > 0);
    }

    public void testRemoveFileFromCache() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addToCache(List.of(fileName));
        assertTrue(metadataCache.containsFile(fileName));

        boolean removed = cacheManager.removeFiles(List.of(fileName));

        assertTrue(removed);
        assertFalse(metadataCache.containsFile(fileName));
    }

    public void testCacheSizeLimitEviction() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        metadataCache.put(fileName);
        assertTrue(metadataCache.containsFile(fileName));

        metadataCache.setSizeLimit(new ByteSizeValue(40));

        assertFalse(metadataCache.containsFile(fileName));
        assertEquals(0, metadataCache.getEntries().size());
    }

    public void testCachePutWithIncreasedSizeLimit() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        metadataCache.setSizeLimit(new ByteSizeValue(500000));
        metadataCache.put(fileName);

        assertTrue(metadataCache.containsFile(fileName));
        logger.info("Entries: {}", metadataCache.getEntries());
        //(we print 3 elements per entry : filePath, memorySize, HitCount)
        assertEquals(1*3, metadataCache.getEntries().size());
    }

    public void testCacheClear() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        metadataCache.put(fileName);
        assertTrue(metadataCache.containsFile(fileName));

        metadataCache.clear();

        assertFalse(metadataCache.containsFile(fileName));
        assertEquals(0, metadataCache.getEntries().size());
    }

    public void testAddMultipleFilesToCache() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        List<String> fileNames = List.of(
            getResourceFile("hits1.parquet").getPath(),
            getResourceFile("hits2.parquet").getPath()
        );

        cacheManager.addToCache(fileNames);
        // 3 elements per cache entry displayed
        assertEquals(2*3, metadataCache.getEntries().size());
        fileNames.forEach(fileName -> assertTrue(metadataCache.containsFile(fileName)));
    }

    public void testRemoveNonExistentFile() {
        CacheManager cacheManager = service.getCacheManager();
        String nonExistentFile = "/path/nonexistent.parquet";

        boolean removed = cacheManager.removeFiles(List.of(nonExistentFile));

        assertFalse(removed);
    }

    public void testGetNonExistentFile() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String nonExistentFile = "/path/nonexistent.parquet";

        Object result = metadataCache.get(nonExistentFile);

//        assertNull(result);
        assertFalse(metadataCache.containsFile(nonExistentFile));
    }

    public void testAddEmptyFileList() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);

        cacheManager.addToCache(Collections.emptyList());

        assertEquals(0, metadataCache.getEntries().size());
    }

    public void testCacheManagerTotalMemoryTracking() {
        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        long initialMemory = cacheManager.getTotalUsedBytes();
        cacheManager.addToCache(List.of(fileName));
        long afterAddMemory = cacheManager.getTotalUsedBytes();

        assertTrue(afterAddMemory > initialMemory);

        cacheManager.removeFiles(List.of(fileName));
        long afterRemoveMemory = cacheManager.getTotalUsedBytes();

        assertEquals(initialMemory, afterRemoveMemory);
    }

    public void testCacheSizeLimits() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);

        long configuredLimit = metadataCache.getConfiguredSizeLimit();
        long totalLimit = cacheManager.getTotalSizeLimit();

        assertTrue(configuredLimit > 0);
        assertTrue(totalLimit > 0);
    }

    private File getResourceFile(String fileName) {
        URL resourceUrl = getClass().getClassLoader().getResource(fileName);
        if (resourceUrl == null) {
            throw new IllegalArgumentException("Resource not found: " + fileName);
        }
        return new File(resourceUrl.getPath());
    }
}
