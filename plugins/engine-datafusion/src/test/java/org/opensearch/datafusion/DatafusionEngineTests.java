/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;
import org.opensearch.env.Environment;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.spi.vectorized.DataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATISTICS_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATISTICS_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATISTICS_CACHE_SIZE_LIMIT;

/**
 * Tests for DatafusionEngine initialization behavior, testing
 * that files are added to cache during engine construction.
 */
public class DatafusionEngineTests extends OpenSearchTestCase {
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
    public void cleanUp() {
        service.doStop();
    }

    /**
     * Test that existing files are added to cache during DatafusionEngine initialization.
     * This verifies the code change in DatafusionEngine constructor that adds existing files
     * from formatCatalogSnapshot to the cache manager.
     */
    public void testExistingFilesAddedToCacheOnInitialization() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles(
            "test-index",
            0,
            "hits1.parquet",
            "hits2.parquet"
        );

        List<FileMetadata> existingFiles = List.of(
            new FileMetadata("parquet", "hits1.parquet"),
            new FileMetadata("parquet", "hits2.parquet")
        );

        CacheManager cacheManager = service.getCacheManager();
        assertNotNull("Cache manager should be available", cacheManager);
        
        long initialMetadataCacheMemory = cacheManager.getMemoryConsumed(CacheUtils.CacheType.METADATA);

        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, existingFiles, service, shardPath);

        long afterInitMetadataCacheMemory = cacheManager.getMemoryConsumed(CacheUtils.CacheType.METADATA);

        assertTrue(
            "Metadata cache should have consumed memory after initialization (initial: " + 
            initialMetadataCacheMemory + ", after: " + afterInitMetadataCacheMemory + ")",
            afterInitMetadataCacheMemory > initialMetadataCacheMemory
        );

        String dataPath = shardPath.getDataPath().resolve(DataFormat.PARQUET.getName()).toString();
        for (FileMetadata fileMetadata : existingFiles) {
            String fullPath = java.nio.file.Paths.get(dataPath, fileMetadata.file()).toString();
            boolean inCache = cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fullPath);
            assertTrue("File should be in cache: " + fullPath, inCache);
        }

        engine.close();
    }

    private ShardPath createShardPathWithResourceFiles(String indexName, int shardId, String... resourceFileNames)
        throws IOException {
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardId shId = new ShardId(index, shardId);
        Path dataPath = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(shardId));
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shId);

        for (String resourceFileName : resourceFileNames) {
            try (InputStream is = getClass().getResourceAsStream("/" + resourceFileName)) {
                Path targetPath = shardPath.getDataPath().resolve("parquet").resolve(resourceFileName);
                Files.createDirectories(targetPath.getParent());
                if (is != null) {
                    Files.copy(is, targetPath);
                }
            }
        }

        return shardPath;
    }
}
