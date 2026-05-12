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
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandler;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandlerFactory;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.NativeStoreRepository;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration-level tests for the warm shard directory stack.
 *
 * <p>Verifies that the full directory stack (FSDirectory → SubdirectoryAwareDirectory
 * → TieredSubdirectoryAwareDirectory → DataFormatAwareStoreDirectory) is wired
 * correctly via {@link TieredDataFormatAwareStoreDirectoryFactory} and that file
 * operations flow through the correct layers.
 */
public class WarmShardDirectoryStackTests extends OpenSearchTestCase {

    private static final DataFormat PARQUET_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return "parquet";
        }

        @Override
        public long priority() {
            return 2;
        }

        @Override
        public java.util.Set<org.opensearch.index.engine.dataformat.FieldTypeCapabilities> supportedFields() {
            return java.util.Set.of();
        }
    };

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
     * Exercises the factory end-to-end with no store strategies — verifies the stack
     * nests FSDirectory → SubdirectoryAwareDirectory → TieredSubdirectoryAwareDirectory
     * → DataFormatAwareStoreDirectory.
     */
    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testWarmDirectoryStackCreationAndWrite() throws IOException {
        TieredDataFormatAwareStoreDirectoryFactory factory = new TieredDataFormatAwareStoreDirectoryFactory(
            getMockPrefetchSettingsSupplier()
        );

        FSDirectory fsDir = FSDirectory.open(shardPath.resolveIndex());
        IndexStorePlugin.DirectoryFactory localDirFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        when(localDirFactory.newDirectory(any(), any())).thenReturn(fsDir);

        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir(shardPath.getShardId());

        DataFormatAwareStoreDirectory storeDir = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath.getShardId(),
            shardPath,
            localDirFactory,
            java.util.Map.of(),
            java.util.Map.of(),             // no strategies
            NativeStoreRepository.EMPTY,
            true,
            remoteDir,
            fileCache,
            null
        );

        assertNotNull("Directory stack should be created", storeDir);

        Directory delegate = ((FilterDirectory) storeDir).getDelegate();
        assertTrue("Should have TieredSubdirectoryAwareDirectory", delegate instanceof TieredSubdirectoryAwareDirectory);

        Directory innerDelegate = ((FilterDirectory) delegate).getDelegate();
        assertTrue("Should have SubdirectoryAwareDirectory", innerDelegate instanceof SubdirectoryAwareDirectory);

        storeDir.close();
    }

    /**
     * Exercises the stack with a parquet strategy. File ops on {@code parquet/…} route
     * to the remote store; the mock remote has no parquet metadata so {@code fileLength}
     * throws. {@code listAll} reflects whatever is on disk (format files included).
     */
    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testWarmDirectoryStackWithFormatStrategy() throws IOException {
        FSDirectory localFsDir = FSDirectory.open(shardPath.resolveIndex());
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(localFsDir, shardPath);

        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir(shardPath.getShardId());

        DataFormatStoreHandler nativeRegistry = mock(DataFormatStoreHandler.class);
        DataFormatStoreHandlerFactory factory = (sid, isWarm, repo) -> nativeRegistry;
        StoreStrategy parquet = new StoreStrategy() {
            @Override
            public Optional<DataFormatStoreHandlerFactory> storeHandler() {
                return Optional.of(factory);
            }
        };

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, parquet),
            remoteDir
        );

        TieredSubdirectoryAwareDirectory tieredSubdir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteDir,
            fileCache,
            null,
            registry,
            shardPath,
            getMockPrefetchSettingsSupplier()
        );

        expectThrows(Exception.class, () -> tieredSubdir.fileLength("parquet/seg.parquet"));

        String[] allFiles = tieredSubdir.listAll();
        Set<String> fileSet = new HashSet<>(Arrays.asList(allFiles));
        assertFalse("listAll should not surface an unwritten parquet file", fileSet.contains("parquet/seg.parquet"));

        tieredSubdir.close();
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
