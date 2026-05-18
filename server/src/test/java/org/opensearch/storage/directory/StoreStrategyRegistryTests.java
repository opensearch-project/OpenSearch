/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandler;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandlerFactory;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.remote.FormatBlobRouter;
import org.opensearch.plugins.BlockCacheRegistry;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link StoreStrategyRegistry}.
 */
public class StoreStrategyRegistryTests extends OpenSearchTestCase {

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
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    private ShardPath shardPath;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path tempDir = createTempDir();
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        Path shardDataPath = tempDir.resolve("data").resolve("test-uuid").resolve("0");
        Path shardStatePath = tempDir.resolve("state").resolve("test-uuid").resolve("0");
        Files.createDirectories(shardDataPath.resolve("index"));
        Files.createDirectories(shardStatePath);
        shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);
    }

    // ═══════════════════════════════════════════════════════════════
    // open() tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenWithNullStrategiesReturnsEmpty() throws IOException {
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(shardPath, true, NativeStoreRepository.EMPTY, null, remoteDir, null);
        assertSame(StoreStrategyRegistry.EMPTY, registry);
    }

    public void testOpenWithEmptyStrategiesReturnsEmpty() throws IOException {
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Collections.emptyMap(),
            remoteDir,
            null
        );
        assertSame(StoreStrategyRegistry.EMPTY, registry);
    }

    public void testOpenCreatesHandlerFromFactory() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        assertNotSame(StoreStrategyRegistry.EMPTY, registry);
        assertTrue(registry.hasStoreHandlers());
        registry.close();
    }

    /**
     * Verifies that the BlockCacheRegistry passed to open() is forwarded verbatim
     * to the factory's create() call. This is the contract that allows format
     * handlers to resolve their preferred cache by name.
     */
    public void testOpenForwardsCacheRegistryToFactory() throws IOException {
        BlockCacheRegistry mockRegistry = mock(BlockCacheRegistry.class);
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);

        // Capture what cacheRegistry the factory receives.
        BlockCacheRegistry[] capturedRegistry = { null };
        StoreStrategy strategy = new StoreStrategy() {
            @Override
            public Optional<DataFormatStoreHandlerFactory> storeHandler() {
                return Optional.of((shardId, isWarm, repo, cacheRegistry) -> {
                    capturedRegistry[0] = cacheRegistry;
                    return handler;
                });
            }
        };

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            createRealRemoteDir(),
            mockRegistry
        );

        assertSame("cacheRegistry must be forwarded to factory unchanged", mockRegistry, capturedRegistry[0]);
        registry.close();
    }

    public void testOpenFactoryThrowsClosesCreatedHandlers() throws IOException {
        // First format succeeds
        DataFormatStoreHandler successHandler = mock(DataFormatStoreHandler.class);
        DataFormat format1 = new DataFormat() {
            @Override
            public String name() {
                return "format1";
            }

            @Override
            public long priority() {
                return 1;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
        StoreStrategy strategy1 = createTestStrategy(successHandler);

        // Second format throws during factory.create()
        DataFormat format2 = new DataFormat() {
            @Override
            public String name() {
                return "format2";
            }

            @Override
            public long priority() {
                return 2;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
        StoreStrategy strategy2 = new StoreStrategy() {
            @Override
            public Optional<DataFormatStoreHandlerFactory> storeHandler() {
                return Optional.of((shardId, isWarm, repo, cacheRegistry) -> { throw new RuntimeException("factory boom"); });
            }
        };

        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        // Use a LinkedHashMap to guarantee iteration order: format1 first, format2 second
        Map<DataFormat, StoreStrategy> strategies = new java.util.LinkedHashMap<>();
        strategies.put(format1, strategy1);
        strategies.put(format2, strategy2);

        expectThrows(
            RuntimeException.class,
            () -> StoreStrategyRegistry.open(shardPath, true, NativeStoreRepository.EMPTY, strategies, remoteDir, null)
        );

        // The successfully created handler should have been closed during cleanup
        verify(successHandler).close();
    }

    // ═══════════════════════════════════════════════════════════════
    // matchFor() tests
    // ═══════════════════════════════════════════════════════════════

    public void testMatchForReturnsNullForLuceneFile() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        assertNull(registry.matchFor("_0.cfe"));
        registry.close();
    }

    public void testMatchForReturnsMatchForFormatFile() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        StoreStrategyRegistry.Match match = registry.matchFor("parquet/_0.parquet");
        assertNotNull(match);
        assertEquals(PARQUET_FORMAT, match.format());
        assertSame(strategy, match.strategy());
        registry.close();
    }

    public void testMatchForReturnsNullForNull() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        assertNull(registry.matchFor(null));
        registry.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // onUploaded() tests
    // ═══════════════════════════════════════════════════════════════

    public void testOnUploadedDispatchesToHandler() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        boolean dispatched = registry.onUploaded("parquet/_0.parquet", "test-base-path/parquet/", "new_blob_key", 1024L);
        assertTrue(dispatched);
        // remotePath: basePath + blobKey (basePath already includes format subdirectory)
        verify(handler).onUploaded(
            org.mockito.ArgumentMatchers.contains("parquet/_0.parquet"),
            eq("test-base-path/parquet/new_blob_key"),
            eq(1024L)
        );
        registry.close();
    }

    public void testOnUploadedReturnsFalseForUnownedFile() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        boolean dispatched = registry.onUploaded("_0.cfe", "test-base-path/", "blob_key", 512L);
        assertFalse(dispatched);
        verify(handler, never()).onUploaded(anyString(), anyString(), org.mockito.ArgumentMatchers.anyLong());
        registry.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // onRemoved() tests
    // ═══════════════════════════════════════════════════════════════

    public void testOnRemovedDispatchesToHandler() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        boolean dispatched = registry.onRemoved("parquet/_0.parquet");
        assertTrue(dispatched);
        verify(handler).onRemoved(org.mockito.ArgumentMatchers.contains("parquet/_0.parquet"));
        registry.close();
    }

    public void testOnRemovedReturnsFalseForUnownedFile() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        boolean dispatched = registry.onRemoved("_0.cfe");
        assertFalse(dispatched);
        verify(handler, never()).onRemoved(anyString());
        registry.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // getFormatStoreHandles() tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetFormatStoreHandlesReturnsLiveHandles() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        NativeStoreHandle liveHandle = new NativeStoreHandle(42L, ptr -> {});
        when(handler.getFormatStoreHandle()).thenReturn(liveHandle);

        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        Map<DataFormat, NativeStoreHandle> handles = registry.getFormatStoreHandles();
        assertEquals(1, handles.size());
        assertSame(liveHandle, handles.get(PARQUET_FORMAT));

        liveHandle.close();
        registry.close();
    }

    public void testGetFormatStoreHandlesSkipsClosedHandles() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        NativeStoreHandle closedHandle = new NativeStoreHandle(99L, ptr -> {});
        closedHandle.close(); // close it before returning
        when(handler.getFormatStoreHandle()).thenReturn(closedHandle);

        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        Map<DataFormat, NativeStoreHandle> handles = registry.getFormatStoreHandles();
        assertTrue("Closed handles should not be returned", handles.isEmpty());

        registry.close();
    }

    public void testGetFormatStoreHandlesReturnsSameHandleOnMultipleCalls() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        NativeStoreHandle liveHandle = new NativeStoreHandle(77L, ptr -> {});
        when(handler.getFormatStoreHandle()).thenReturn(liveHandle);

        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        Map<DataFormat, NativeStoreHandle> handles1 = registry.getFormatStoreHandles();
        Map<DataFormat, NativeStoreHandle> handles2 = registry.getFormatStoreHandles();
        assertSame("Same handle should be returned on multiple calls", handles1.get(PARQUET_FORMAT), handles2.get(PARQUET_FORMAT));

        liveHandle.close();
        registry.close();
    }

    public void testGetFormatStoreHandlesEmptyWhenNoHandlers() {
        Map<DataFormat, NativeStoreHandle> handles = StoreStrategyRegistry.EMPTY.getFormatStoreHandles();
        assertTrue(handles.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════
    // close() tests
    // ═══════════════════════════════════════════════════════════════

    public void testCloseClosesAllHandlers() throws IOException {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        registry.close();
        verify(handler).close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Seed key tests
    // ═══════════════════════════════════════════════════════════════

    public void testSeedUsesAbsolutePathKeys() throws Exception {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);

        // Create a real RemoteSegmentStoreDirectory and inject a parquet entry
        // into its uploaded segments map via reflection so that seedFromRemoteMetadata
        // picks it up during open().
        RemoteSegmentStoreDirectory remoteDir = createRealRemoteDir();
        injectUploadedSegment(remoteDir, "parquet/_0.parquet", "parquet_blob_key");

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        // Capture the seed call argument
        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, DataFormatStoreHandler.FileEntry>> captor = org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(handler).seed(captor.capture());

        Map<String, DataFormatStoreHandler.FileEntry> seeded = captor.getValue();
        assertFalse("Seed map should not be empty", seeded.isEmpty());

        // The key should be the absolute path: shardPath.getDataPath() + relative file
        String expectedKey = shardPath.getDataPath().resolve("parquet/_0.parquet").toString();
        assertTrue("Seed key should be absolute path: " + expectedKey, seeded.containsKey(expectedKey));

        DataFormatStoreHandler.FileEntry entry = seeded.get(expectedKey);
        assertEquals("test-base-path/parquet/parquet_blob_key", entry.path());
        assertEquals(DataFormatStoreHandler.REMOTE, entry.location());

        registry.close();
    }

    public void testSeedUsesFormatSpecificBasePath() throws Exception {
        DataFormatStoreHandler handler = mock(DataFormatStoreHandler.class);
        StoreStrategy strategy = createTestStrategy(handler);

        // Create a RemoteSegmentStoreDirectory with a FormatBlobRouter that routes
        // "parquet" to a different sub-path than the base path.
        RemoteSegmentStoreDirectory remoteDir = createRemoteDirWithFormatRouter();
        injectUploadedSegment(remoteDir, "parquet/_0.parquet", "parquet_blob_key");

        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, strategy),
            remoteDir,
            null
        );

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, DataFormatStoreHandler.FileEntry>> captor = org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(handler).seed(captor.capture());

        Map<String, DataFormatStoreHandler.FileEntry> seeded = captor.getValue();
        assertFalse("Seed map should not be empty", seeded.isEmpty());

        String expectedKey = shardPath.getDataPath().resolve("parquet/_0.parquet").toString();
        assertTrue("Seed key should be absolute path", seeded.containsKey(expectedKey));

        DataFormatStoreHandler.FileEntry entry = seeded.get(expectedKey);
        // The FormatBlobRouter routes "parquet" to "base-path/parquet/" so getRemoteBasePath("parquet")
        // returns "base-path/parquet/". The default remotePath appends format + "/" + blobKey.
        assertTrue(
            "Seeded path should use format-specific base path containing 'parquet': " + entry.path(),
            entry.path().contains("parquet")
        );
        assertEquals(DataFormatStoreHandler.REMOTE, entry.location());

        registry.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private StoreStrategy createTestStrategy(DataFormatStoreHandler handler) {
        DataFormatStoreHandlerFactory factory = (shardId, isWarm, repo, cacheRegistry) -> handler;
        return new StoreStrategy() {
            @Override
            public Optional<DataFormatStoreHandlerFactory> storeHandler() {
                return Optional.of(factory);
            }
        };
    }

    private RemoteSegmentStoreDirectory createRealRemoteDir() throws IOException {
        RemoteDirectory remoteDataDir = mock(RemoteDirectory.class);
        RemoteDirectory remoteMetadataDir = mock(RemoteDirectory.class);
        RemoteStoreLockManager lockManager = mock(RemoteStoreLockManager.class);
        ThreadPool tp = mock(ThreadPool.class);

        BlobContainer mockBlobContainer = mock(BlobContainer.class);
        when(mockBlobContainer.path()).thenReturn(new BlobPath().add("test-base-path").add("parquet"));
        when(remoteDataDir.getBlobContainer()).thenReturn(mockBlobContainer);

        return new RemoteSegmentStoreDirectory(remoteDataDir, remoteMetadataDir, lockManager, tp, shardPath.getShardId(), new HashMap<>());
    }

    private RemoteSegmentStoreDirectory createRemoteDirWithFormatRouter() throws IOException {
        RemoteDirectory remoteDataDir = mock(RemoteDirectory.class);
        RemoteDirectory remoteMetadataDir = mock(RemoteDirectory.class);
        RemoteStoreLockManager lockManager = mock(RemoteStoreLockManager.class);
        ThreadPool tp = mock(ThreadPool.class);

        BlobPath basePath = new BlobPath().add("base-path");
        BlobContainer baseBlobContainer = mock(BlobContainer.class);
        when(baseBlobContainer.path()).thenReturn(basePath);
        when(remoteDataDir.getBlobContainer()).thenReturn(baseBlobContainer);

        // Create a FormatBlobRouter that routes "parquet" to "base-path/parquet/"
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(basePath)).thenReturn(baseBlobContainer);

        BlobPath parquetPath = basePath.add("parquet");
        BlobContainer parquetBlobContainer = mock(BlobContainer.class);
        when(parquetBlobContainer.path()).thenReturn(parquetPath);
        when(blobStore.blobContainer(parquetPath)).thenReturn(parquetBlobContainer);

        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        when(remoteDataDir.getFormatBlobRouter()).thenReturn(Optional.of(router));

        return new RemoteSegmentStoreDirectory(remoteDataDir, remoteMetadataDir, lockManager, tp, shardPath.getShardId(), new HashMap<>());
    }

    /**
     * Injects an uploaded segment entry into the RemoteSegmentStoreDirectory's
     * internal map via reflection. This avoids the need to set up the full
     * metadata serialization pipeline just to test seeding behaviour.
     */
    @SuppressForbidden(reason = "test needs reflection to inject parquet metadata without full upload pipeline")
    private static void injectUploadedSegment(RemoteSegmentStoreDirectory remoteDir, String localFilename, String uploadedFilename)
        throws Exception {
        Field field = RemoteSegmentStoreDirectory.class.getDeclaredField("segmentsUploadedToRemoteStore");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, UploadedSegmentMetadata> map = (Map<String, UploadedSegmentMetadata>) field.get(remoteDir);
        // The UploadedSegmentMetadata constructor is package-private, so we use fromString
        // Format: originalFilename::uploadedFilename::checksum::length::writtenByMajor
        String separator = "::";
        String metadataStr = localFilename
            + separator
            + uploadedFilename
            + separator
            + "checksum123"
            + separator
            + "1024"
            + separator
            + org.apache.lucene.util.Version.LATEST.major;
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString(metadataStr);
        map.put(localFilename, metadata);
    }
}
