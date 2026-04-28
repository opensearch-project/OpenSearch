/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmFsServiceTests extends OpenSearchTestCase {

    private Settings settings;
    private FileCacheSettings fileCacheSettings;
    private IndicesService indicesService;
    private FileCache fileCache;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.EMPTY;
        fileCacheSettings = mock(FileCacheSettings.class);
        indicesService = mock(IndicesService.class);
        fileCache = mock(FileCache.class);
    }

    public void testStatsWithNormalOperation() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100MB
        long fileCacheUsage = 20L * 1024 * 1024; // 20MB
        long shard1Size = 50L * 1024 * 1024; // 50MB
        long shard2Size = 30L * 1024 * 1024; // 30MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Mock indices and shards
        IndexService indexService = mockIndexService(shard1Size, shard2Size);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify
            assertNotNull(fsInfo);
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            assertEquals(1, paths.size());

            FsInfo.Path warmPath = paths.get(0);
            assertEquals("/warm", warmPath.path);
            assertEquals("warm", warmPath.mount);
            assertEquals("warm", warmPath.type);

            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            long expectedUsed = shard1Size + shard2Size;
            long expectedFree = expectedTotal - expectedUsed;

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedFree, warmPath.free);
            assertEquals(expectedFree, warmPath.available);
            assertEquals(fileCacheCapacity, warmPath.fileCacheReserved);
            assertEquals(fileCacheUsage, warmPath.fileCacheUtilized);
        }
    }

    public void testStatsWithNullFileCache() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);

        long shard1Size = 50L * 1024 * 1024; // 50MB
        long shard2Size = 30L * 1024 * 1024; // 30MB

        // Mock indices and shards
        IndexService indexService = mockIndexService(shard1Size, shard2Size);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service with null file cache
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, null);
            FsInfo fsInfo = warmFsService.stats();

            // Verify
            assertNotNull(fsInfo);
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            assertEquals(1, paths.size());

            FsInfo.Path warmPath = paths.get(0);
            assertEquals("/warm", warmPath.path);
            assertEquals("warm", warmPath.mount);
            assertEquals("warm", warmPath.type);
            assertEquals(0L, warmPath.total);
            assertEquals(0L, warmPath.free);
            assertEquals(0L, warmPath.available);
            assertEquals(-1L, warmPath.fileCacheReserved);
            assertEquals(0L, warmPath.fileCacheUtilized);
        }
    }

    public void testStatsWithNullIndicesService() throws IOException {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100MB
        long fileCacheUsage = 20L * 1024 * 1024; // 20MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Create service with null indices service
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, null, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify
            assertNotNull(fsInfo);
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            assertEquals(1, paths.size());

            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedTotal, warmPath.free); // No used bytes since no indices
            assertEquals(expectedTotal, warmPath.available);
        }
    }

    public void testStatsWithNonPrimaryShards() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100MB
        long fileCacheUsage = 20L * 1024 * 1024; // 20MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Create shards - one primary, one replica
        List<IndexShard> shards = new ArrayList<>();
        IndexShard primaryShard = mockShard(true, true, 50L * 1024 * 1024); // Primary, 50MB
        IndexShard replicaShard = mockShard(false, true, 30L * 1024 * 1024); // Replica, 30MB
        shards.add(primaryShard);
        shards.add(replicaShard);

        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify only primary shard size is counted
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            long expectedUsed = 50L * 1024 * 1024; // Only primary shard
            long expectedFree = expectedTotal - expectedUsed;

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedFree, warmPath.free);
            assertEquals(expectedFree, warmPath.available);

            // Verify that store.stats was only called on primary shard
            verify(primaryShard.store()).stats(anyLong());
            verify(replicaShard.store(), never()).stats(anyLong());
        }
    }

    public void testStatsWithInactiveShards() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100MB
        long fileCacheUsage = 20L * 1024 * 1024; // 20MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Create shards - one active, one inactive
        List<IndexShard> shards = new ArrayList<>();
        IndexShard activeShard = mockShard(true, true, 50L * 1024 * 1024); // Active primary, 50MB
        IndexShard inactiveShard = mockShard(true, false, 30L * 1024 * 1024); // Inactive primary, 30MB
        shards.add(activeShard);
        shards.add(inactiveShard);

        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify only active shard size is counted
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            long expectedUsed = 50L * 1024 * 1024; // Only active shard
            long expectedFree = expectedTotal - expectedUsed;

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedFree, warmPath.free);
            assertEquals(expectedFree, warmPath.available);

            // Verify that store.stats was only called on active shard
            verify(activeShard.store()).stats(anyLong());
            verify(inactiveShard.store(), never()).stats(anyLong());
        }
    }

    public void testStatsWithNullRoutingEntry() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100MB
        long fileCacheUsage = 20L * 1024 * 1024; // 20MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Create shard with null routing entry
        IndexShard shard = mock(IndexShard.class);
        when(shard.routingEntry()).thenReturn(null);

        List<IndexShard> shards = Collections.singletonList(shard);
        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedTotal, warmPath.free); // No used bytes since shard has null routing
            assertEquals(expectedTotal, warmPath.available);

            // Verify that store.stats was never called
            verify(shard, never()).store();
        }
    }

    public void testStatsWithExceptionWhileGettingShardSize() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100MB
        long fileCacheUsage = 20L * 1024 * 1024; // 20MB
        long shard1Size = 50L * 1024 * 1024; // 50MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Create shards - one normal, one that throws exception
        List<IndexShard> shards = new ArrayList<>();
        IndexShard normalShard = mockShard(true, true, shard1Size);
        IndexShard errorShard = mockShardWithError(true, true);
        shards.add(normalShard);
        shards.add(errorShard);

        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify only normal shard size is counted
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            long expectedUsed = shard1Size; // Only the normal shard
            long expectedFree = expectedTotal - expectedUsed;

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedFree, warmPath.free);
            assertEquals(expectedFree, warmPath.available);
        }
    }

    public void testStatsWithUsedBytesExceedingTotal() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 1.0; // Small ratio
        long fileCacheCapacity = 10L * 1024 * 1024; // 10MB
        long fileCacheUsage = 5L * 1024 * 1024; // 5MB
        long shard1Size = 20L * 1024 * 1024; // 20MB - larger than total

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Mock indices and shards
        IndexService indexService = mockIndexService(shard1Size);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify free bytes is 0 (not negative)
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(0L, warmPath.free); // Math.max(0, negative) = 0
            assertEquals(0L, warmPath.available);
        }
    }

    public void testStatsWithMultipleIndices() throws Exception {
        // Setup
        double dataToFileCacheSizeRatio = 5.0;
        long fileCacheCapacity = 200L * 1024 * 1024; // 200MB
        long fileCacheUsage = 40L * 1024 * 1024; // 40MB

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(dataToFileCacheSizeRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(fileCache.usage()).thenReturn(fileCacheUsage);

        // Mock multiple indices
        IndexService indexService1 = mockIndexService(50L * 1024 * 1024, 30L * 1024 * 1024);
        IndexService indexService2 = mockIndexService(20L * 1024 * 1024, 10L * 1024 * 1024);

        List<IndexService> indexServices = new ArrayList<>();
        indexServices.add(indexService1);
        indexServices.add(indexService2);
        when(indicesService.iterator()).thenReturn(indexServices.iterator());

        // Create service and get stats
        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService warmFsService = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService, fileCache);
            FsInfo fsInfo = warmFsService.stats();

            // Verify
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path path : fsInfo) {
                paths.add(path);
            }
            FsInfo.Path warmPath = paths.get(0);
            long expectedTotal = (long) (dataToFileCacheSizeRatio * fileCacheCapacity);
            long expectedUsed = 50L * 1024 * 1024 + 30L * 1024 * 1024 + 20L * 1024 * 1024 + 10L * 1024 * 1024; // All shards
            long expectedFree = expectedTotal - expectedUsed;

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedFree, warmPath.free);
            assertEquals(expectedFree, warmPath.available);
        }
    }

    // Helper methods

    private IndexService mockIndexService(long... shardSizes) throws Exception {
        List<IndexShard> shards = new ArrayList<>();
        for (long size : shardSizes) {
            shards.add(mockShard(true, true, size));
        }

        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenReturn(shards.iterator());
        return indexService;
    }

    private IndexShard mockShard(boolean isPrimary, boolean isActive, long sizeInBytes) throws Exception {
        IndexShard shard = mock(IndexShard.class);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId("test", "_na_", 0),
            "node1",
            isPrimary,
            isActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shard.shardId()).thenReturn(shardRouting.shardId());

        Store store = mock(Store.class);
        StoreStats storeStats = mock(StoreStats.class);
        when(storeStats.getSizeInBytes()).thenReturn(sizeInBytes);
        when(store.stats(anyLong())).thenReturn(storeStats);
        when(shard.store()).thenReturn(store);

        return shard;
    }

    private IndexShard mockShardWithError(boolean isPrimary, boolean isActive) throws Exception {
        IndexShard shard = mock(IndexShard.class);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId("test", "_na_", 1),
            "node1",
            isPrimary,
            isActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shard.shardId()).thenReturn(shardRouting.shardId());

        Store store = mock(Store.class);
        when(store.stats(anyLong())).thenThrow(new RuntimeException("Test exception"));
        when(shard.store()).thenReturn(store);

        return shard;
    }
}
