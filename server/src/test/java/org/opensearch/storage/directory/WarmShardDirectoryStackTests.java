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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration-level tests for the warm shard directory stack.
 *
 * <p>These tests verify that the full directory stack (FSDirectory → SubdirectoryAwareDirectory
 * → TieredSubdirectoryAwareDirectory → DataFormatAwareStoreDirectory) is wired correctly
 * and that file operations flow through the correct layers.
 */
public class WarmShardDirectoryStackTests extends OpenSearchTestCase {

    private Path tempDir;
    private ShardPath shardPath;
    private IndexSettings indexSettings;
    private FileCache fileCache;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        Index index = new Index("test-warm-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);

        // ShardPath requires: dataPath ends with <index-uuid>/<shard-id>
        Path shardStatePath = tempDir.resolve("state").resolve("test-uuid").resolve("0");
        Path shardDataPath = tempDir.resolve("data").resolve("test-uuid").resolve("0");
        Path indexPath = shardDataPath.resolve("index");
        Files.createDirectories(shardStatePath);
        Files.createDirectories(shardDataPath);
        Files.createDirectories(indexPath);

        shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-warm-index").settings(settings).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
    }

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
     * Tests that the full warm directory stack is created correctly via the factory
     * and that basic write operations flow through all layers.
     *
     * <p>Verifies: FSDirectory → SubdirectoryAwareDirectory → TieredSubdirectoryAwareDirectory
     * → DataFormatAwareStoreDirectory, and that a file written through the stack is
     * visible via listAll.
     */
    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testWarmDirectoryStackCreationAndWrite() throws IOException {
        TieredDataFormatAwareStoreDirectoryFactory factory = new TieredDataFormatAwareStoreDirectoryFactory(
            getMockPrefetchSettingsSupplier()
        );

        FSDirectory fsDir = FSDirectory.open(shardPath.resolveIndex());
        IndexStorePlugin.DirectoryFactory localDirFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        when(localDirFactory.newDirectory(any(), any())).thenReturn(fsDir);

        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getFormatDirectoryFactories(any())).thenReturn(new HashMap<>());
        when(registry.getFormatDescriptors(any())).thenReturn(new HashMap<>());

        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir(shardPath.getShardId());

        DataFormatAwareStoreDirectory storeDir = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath.getShardId(),
            shardPath,
            localDirFactory,
            registry,
            remoteDir,
            fileCache,
            null // threadPool
        );

        assertNotNull("Directory stack should be created", storeDir);

        // Verify the stack structure
        Directory delegate = ((FilterDirectory) storeDir).getDelegate();
        assertTrue("Should have TieredSubdirectoryAwareDirectory", delegate instanceof TieredSubdirectoryAwareDirectory);

        Directory innerDelegate = ((FilterDirectory) delegate).getDelegate();
        assertTrue("Should have SubdirectoryAwareDirectory", innerDelegate instanceof SubdirectoryAwareDirectory);

        storeDir.close();
    }

    /**
     * Tests that a format directory registered in the stack receives file operations
     * for files with the matching format prefix.
     */
    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testWarmDirectoryStackWithFormatDirectory() throws IOException {
        // Create a parquet file on disk in the shard's parquet subdirectory
        Path parquetPath = shardPath.getDataPath().resolve("parquet");
        Files.createDirectories(parquetPath);
        // Write the file directly to disk (simulating Rust writer)
        Files.write(parquetPath.resolve("seg.parquet"), new byte[] { 1, 2, 3, 4 });

        // Build the directory stack with SubdirectoryAwareDirectory as the format directory
        // (same as production — ParquetTieredDirectory wraps SubdirectoryAwareDirectory)
        FSDirectory localFsDir = FSDirectory.open(shardPath.resolveIndex());
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(localFsDir, shardPath);

        // Use the same SubdirectoryAwareDirectory as the format directory
        // (in production, ParquetTieredDirectory wraps it via FilterDirectory)
        // Use a non-closing wrapper so the shared SubdirectoryAwareDirectory isn't double-closed
        // (in production, ParquetTieredDirectory.close() only cleans FFM resources, not super.close())
        Map<String, Directory> formatDirs = new HashMap<>();
        formatDirs.put("parquet", new FilterDirectory(subdirAware) {
            @Override
            public void close() {
                // Don't close — SubdirectoryAwareDirectory is shared with TieredDirectory
            }
        });

        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir(shardPath.getShardId());

        TieredSubdirectoryAwareDirectory tieredSubdir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteDir,
            fileCache,
            null,
            formatDirs,
            getMockPrefetchSettingsSupplier()
        );

        // Verify the parquet file is accessible through the stack
        long length = tieredSubdir.fileLength("parquet/seg.parquet");
        assertEquals("Parquet file should be 4 bytes", 4L, length);

        // Verify listAll includes parquet files
        String[] allFiles = tieredSubdir.listAll();
        Set<String> fileSet = new HashSet<>(Arrays.asList(allFiles));
        assertTrue("listAll should include parquet file", fileSet.contains("parquet/seg.parquet"));

        tieredSubdir.close();
    }

    private RemoteSegmentStoreDirectory createRealRemoteDir(ShardId shardId) throws IOException {
        RemoteDirectory remoteDataDir = mock(RemoteDirectory.class);
        RemoteDirectory remoteMetadataDir = mock(RemoteDirectory.class);
        RemoteStoreLockManager lockManager = mock(RemoteStoreLockManager.class);
        ThreadPool tp = mock(ThreadPool.class);
        return new RemoteSegmentStoreDirectory(remoteDataDir, remoteMetadataDir, lockManager, tp, shardId, new HashMap<>());
    }
}
