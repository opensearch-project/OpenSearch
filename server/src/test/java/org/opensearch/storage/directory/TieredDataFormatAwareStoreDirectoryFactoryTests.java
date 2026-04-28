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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TieredDataFormatAwareStoreDirectoryFactory}.
 *
 * <p>Verifies the factory creates the correct directory stack for warm+format indices
 * and rejects the hot path (5-param method).
 */
public class TieredDataFormatAwareStoreDirectoryFactoryTests extends OpenSearchTestCase {

    private TieredDataFormatAwareStoreDirectoryFactory factory;
    private IndexSettings indexSettings;
    private ShardId shardId;
    private ShardPath shardPath;
    private IndexStorePlugin.DirectoryFactory localDirectoryFactory;
    private DataFormatRegistry dataFormatRegistry;
    private RemoteSegmentStoreDirectory remoteDirectory;
    private FileCache fileCache;
    private ThreadPool threadPool;

    /**
     * Sets up the factory and mock dependencies before each test.
     */
    @Before
    public void setup() throws IOException {
        Supplier<TieredStoragePrefetchSettings> prefetchSupplier = () -> {
            TieredStoragePrefetchSettings settings = mock(TieredStoragePrefetchSettings.class);
            when(settings.getReadAheadBlockCount()).thenReturn(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
            when(settings.getReadAheadEnableFileFormats()).thenReturn(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS);
            when(settings.isStoredFieldsPrefetchEnabled()).thenReturn(true);
            return settings;
        };
        factory = new TieredDataFormatAwareStoreDirectoryFactory(prefetchSupplier);

        Path tempDir = createTempDir();
        Index index = new Index("test-index", "test-uuid");
        shardId = new ShardId(index, 0);

        // ShardPath requires: dataPath ends with <index-uuid>/<shard-id>
        Path shardStatePath = tempDir.resolve("state").resolve("test-uuid").resolve("0");
        Path shardDataPath = tempDir.resolve("data").resolve("test-uuid").resolve("0");
        Path indexPath = shardDataPath.resolve("index");
        java.nio.file.Files.createDirectories(shardStatePath);
        java.nio.file.Files.createDirectories(shardDataPath);
        java.nio.file.Files.createDirectories(indexPath);
        shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        FSDirectory fsDir = FSDirectory.open(indexPath);
        localDirectoryFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        when(localDirectoryFactory.newDirectory(any(), any())).thenReturn(fsDir);

        dataFormatRegistry = mock(DataFormatRegistry.class);
        when(dataFormatRegistry.getFormatDirectoryFactories(any())).thenReturn(new HashMap<>());
        when(dataFormatRegistry.getFormatDescriptors(any())).thenReturn(new HashMap<>());

        remoteDirectory = createRealRemoteSegmentStoreDirectory(shardId);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        threadPool = mock(ThreadPool.class);
    }

    /**
     * Creates a real RemoteSegmentStoreDirectory with mocked inner directories.
     * RemoteSegmentStoreDirectory is a final class and cannot be mocked.
     */
    private RemoteSegmentStoreDirectory createRealRemoteSegmentStoreDirectory(ShardId shardId) throws IOException {
        RemoteDirectory remoteDataDir = mock(RemoteDirectory.class);
        RemoteDirectory remoteMetadataDir = mock(RemoteDirectory.class);
        org.opensearch.index.store.lockmanager.RemoteStoreLockManager lockManager = mock(
            org.opensearch.index.store.lockmanager.RemoteStoreLockManager.class
        );
        ThreadPool tp = mock(ThreadPool.class);
        return new RemoteSegmentStoreDirectory(remoteDataDir, remoteMetadataDir, lockManager, tp, shardId, new HashMap<>());
    }

    /**
     * Tests that the warm-aware factory method creates the correct directory stack:
     * DataFormatAwareStoreDirectory wrapping TieredSubdirectoryAwareDirectory.
     */
    public void testCreatesCorrectDirectoryStack() throws IOException {
        DataFormatAwareStoreDirectory result = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardId,
            shardPath,
            localDirectoryFactory,
            dataFormatRegistry,
            remoteDirectory,
            fileCache,
            threadPool
        );

        assertNotNull("Factory should return a non-null directory", result);
        assertTrue("Outermost directory should be DataFormatAwareStoreDirectory", result instanceof DataFormatAwareStoreDirectory);

        // The delegate of DataFormatAwareStoreDirectory should be TieredSubdirectoryAwareDirectory
        Directory delegate = ((FilterDirectory) result).getDelegate();
        assertTrue("Delegate should be TieredSubdirectoryAwareDirectory", delegate instanceof TieredSubdirectoryAwareDirectory);

        // The delegate of TieredSubdirectoryAwareDirectory should be SubdirectoryAwareDirectory
        Directory innerDelegate = ((FilterDirectory) delegate).getDelegate();
        assertTrue("Inner delegate should be SubdirectoryAwareDirectory", innerDelegate instanceof SubdirectoryAwareDirectory);

        result.close();
    }

    /**
     * Tests that SubdirectoryAwareDirectory appears only once in the directory chain.
     * The factory should NOT double-wrap with SubdirectoryAwareDirectory.
     */
    public void testNoDoubleSubdirectoryAwareDirectoryWrapping() throws IOException {
        DataFormatAwareStoreDirectory result = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardId,
            shardPath,
            localDirectoryFactory,
            dataFormatRegistry,
            remoteDirectory,
            fileCache,
            threadPool
        );

        int subdirAwareCount = 0;
        Directory current = result;
        while (current instanceof FilterDirectory) {
            if (current instanceof SubdirectoryAwareDirectory) {
                subdirAwareCount++;
            }
            current = ((FilterDirectory) current).getDelegate();
        }

        assertEquals("SubdirectoryAwareDirectory should appear exactly once in the chain", 1, subdirAwareCount);

        result.close();
    }

    /**
     * Tests that when DataFormatRegistry returns empty tiered directories,
     * the factory still creates a valid directory stack with no format directories.
     */
    public void testEmptyFormatDirectoriesWhenNoPluginProvides() throws IOException {
        when(dataFormatRegistry.getFormatDirectoryFactories(any())).thenReturn(Map.of());

        DataFormatAwareStoreDirectory result = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardId,
            shardPath,
            localDirectoryFactory,
            dataFormatRegistry,
            remoteDirectory,
            fileCache,
            threadPool
        );

        assertNotNull("Factory should return a non-null directory even with no format plugins", result);

        // Verify the stack is still correct
        Directory delegate = ((FilterDirectory) result).getDelegate();
        assertTrue("Delegate should still be TieredSubdirectoryAwareDirectory", delegate instanceof TieredSubdirectoryAwareDirectory);

        result.close();
    }

    /**
     * Tests that calling the 5-param (hot path) method throws UnsupportedOperationException.
     */
    public void testHotPathThrowsUnsupportedOperation() {
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> factory.newDataFormatAwareStoreDirectory(indexSettings, shardId, shardPath, localDirectoryFactory, dataFormatRegistry)
        );

        assertTrue("Exception message should mention warm parameters", exception.getMessage().contains("warm"));
    }
}
