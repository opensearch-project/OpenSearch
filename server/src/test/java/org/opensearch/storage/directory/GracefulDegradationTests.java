/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for graceful degradation when sandbox plugins are not loaded.
 *
 * <p>When no data format plugins provide tiered directories (e.g., sandbox not loaded),
 * the warm directory stack should still function correctly using only the default
 * TieredDirectory for Lucene files. No errors should occur.
 */
public class GracefulDegradationTests extends OpenSearchTestCase {

    private Supplier<TieredStoragePrefetchSettings> getMockPrefetchSettingsSupplier() {
        return () -> {
            TieredStoragePrefetchSettings settings = mock(TieredStoragePrefetchSettings.class);
            when(settings.getReadAheadBlockCount()).thenReturn(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
            when(settings.getReadAheadEnableFileFormats()).thenReturn(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS);
            when(settings.isStoredFieldsPrefetchEnabled()).thenReturn(true);
            return settings;
        };
    }

    /**
     * Tests that when DataFormatRegistry returns empty tiered directories (simulating
     * sandbox not loaded), the factory creates a valid directory stack that works
     * for plain Lucene warm operations without errors.
     */
    public void testNoFormatPluginsCreatesValidStack() throws IOException {
        Path tempDir = createTempDir();
        Index index = new Index("test-degradation", "test-uuid");
        ShardId shardId = new ShardId(index, 0);

        Path shardStatePath = tempDir.resolve("state").resolve("test-uuid").resolve("0");
        Path shardDataPath = tempDir.resolve("data").resolve("test-uuid").resolve("0");
        Files.createDirectories(shardStatePath);
        Files.createDirectories(shardDataPath);
        Files.createDirectories(shardDataPath.resolve("index"));

        ShardPath shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-degradation").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        FSDirectory fsDir = FSDirectory.open(shardPath.resolveIndex());
        IndexStorePlugin.DirectoryFactory localDirFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        when(localDirFactory.newDirectory(any(), any())).thenReturn(fsDir);

        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir(shardId);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);

        TieredDataFormatAwareStoreDirectoryFactory factory = new TieredDataFormatAwareStoreDirectoryFactory(
            getMockPrefetchSettingsSupplier()
        );

        // Should not throw — graceful degradation
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            org.opensearch.repositories.NativeStoreRepository.EMPTY,
            java.util.Map.of(),
            remoteDir,
            null
        );
        DataFormatAwareStoreDirectory storeDir = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardId,
            shardPath,
            localDirFactory,
            Map.of(),
            registry,
            remoteDir,
            fileCache,
            null
        );

        assertNotNull("Directory should be created even without format plugins", storeDir);

        // Verify the stack is correct
        Directory delegate = ((FilterDirectory) storeDir).getDelegate();
        assertTrue(
            "Should have TieredSubdirectoryAwareDirectory even without format plugins",
            delegate instanceof TieredSubdirectoryAwareDirectory
        );

        Directory innerDelegate = ((FilterDirectory) delegate).getDelegate();
        assertTrue("Should have SubdirectoryAwareDirectory", innerDelegate instanceof SubdirectoryAwareDirectory);

        storeDir.close();
    }

    /**
     * Tests that TieredSubdirectoryAwareDirectory with empty format directories
     * routes all operations to TieredDirectory without errors.
     */
    public void testEmptyFormatDirectoriesRoutesToTieredDirectory() throws IOException {
        Path tempDir = createTempDir();
        Index index = new Index("test-empty-formats", "test-uuid");
        ShardId shardId = new ShardId(index, 0);

        Path shardStatePath = tempDir.resolve("state").resolve("test-uuid").resolve("0");
        Path shardDataPath = tempDir.resolve("data").resolve("test-uuid").resolve("0");
        Files.createDirectories(shardStatePath);
        Files.createDirectories(shardDataPath);
        Files.createDirectories(shardDataPath.resolve("index"));

        ShardPath shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);

        FSDirectory fsDir = FSDirectory.open(shardPath.resolveIndex());
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(fsDir, shardPath);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir(shardId);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);

        // Empty strategies — simulates no sandbox plugins

        TieredSubdirectoryAwareDirectory tieredSubdir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteDir,
            fileCache,
            null,
            StoreStrategyRegistry.EMPTY,
            shardPath,
            getMockPrefetchSettingsSupplier()
        );

        // listAll should work without errors
        String[] files = tieredSubdir.listAll();
        assertNotNull("listAll should return non-null", files);

        // close should not throw
        tieredSubdir.close();
    }

    /**
     * Tests that the factory key constant is correctly defined.
     */
    public void testFactoryKeyConstant() {
        assertEquals(
            "Factory key should be 'dataformat-tiered'",
            "dataformat-tiered",
            TieredDataFormatAwareStoreDirectoryFactory.FACTORY_KEY
        );
    }

    private RemoteSegmentStoreDirectory createRealRemoteDir(ShardId shardId) throws IOException {
        RemoteDirectory remoteDataDir = mock(RemoteDirectory.class);
        RemoteDirectory remoteMetadataDir = mock(RemoteDirectory.class);
        RemoteStoreLockManager lockManager = mock(RemoteStoreLockManager.class);
        ThreadPool tp = mock(ThreadPool.class);

        BlobContainer mockBlobContainer = mock(BlobContainer.class);
        when(mockBlobContainer.path()).thenReturn(new BlobPath().add("test-base-path"));
        when(remoteDataDir.getBlobContainer()).thenReturn(mockBlobContainer);

        return new RemoteSegmentStoreDirectory(remoteDataDir, remoteMetadataDir, lockManager, tp, shardId, new HashMap<>());
    }
}
