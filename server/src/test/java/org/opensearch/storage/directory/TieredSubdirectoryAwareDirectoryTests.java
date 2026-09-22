/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandler;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandlerFactory;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.repositories.NativeStoreRepository;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Functional tests for {@link TieredSubdirectoryAwareDirectory} exercising real I/O
 * through the full directory stack (FSDirectory → SubdirectoryAwareDirectory → TieredDirectory).
 *
 * <p>Format routing is verified via a real {@link StoreStrategyRegistry} built from a
 * {@link StoreStrategy} whose {@link DataFormatStoreHandlerFactory} returns a Mockito-mocked
 * {@link DataFormatStoreHandler} — the mock verifies {@code onUploaded} / {@code onRemoved} /
 * {@code close} calls. Lucene files skip the strategy lookup entirely.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class TieredSubdirectoryAwareDirectoryTests extends TieredStorageBaseTestCase {

    private FileCache fileCache;
    private ShardPath shardPath;
    private FSDirectory localFsDir;
    private SubdirectoryAwareDirectory subdirAware;
    private TieredSubdirectoryAwareDirectory directory;

    private static final byte[] TEST_DATA = "hello-tiered".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PARQUET_DATA = "parquet-payload".getBytes(StandardCharsets.UTF_8);
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

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();

        // Stub getBlobContainer().path() so getRemoteBasePath(format) doesn't NPE in afterSyncToRemote tests
        // In production, getRemoteBasePath("parquet") returns a path that includes the format subdirectory
        BlobContainer mockBlobContainer = mock(BlobContainer.class);
        when(mockBlobContainer.path()).thenReturn(new BlobPath().add("test-base-path").add("parquet"));
        when(((RemoteDirectory) remoteDataDirectory).getBlobContainer()).thenReturn(mockBlobContainer);

        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Path tempDir = createTempDir();
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        Path shardDataPath = tempDir.resolve("data").resolve("test-uuid").resolve("0");
        Path shardStatePath = tempDir.resolve("state").resolve("test-uuid").resolve("0");
        Files.createDirectories(shardDataPath.resolve("index"));
        Files.createDirectories(shardStatePath);
        shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);

        localFsDir = FSDirectory.open(shardPath.resolveIndex());
        subdirAware = new SubdirectoryAwareDirectory(localFsDir, shardPath);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, 1);
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
     * Builds a TieredSubdirectoryAwareDirectory with no strategies (Lucene-only).
     */
    private TieredSubdirectoryAwareDirectory buildDirectoryNoFormats() {
        return new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            StoreStrategyRegistry.EMPTY,
            shardPath,
            getMockPrefetchSettingsSupplier()
        );
    }

    /**
     * Builds a TieredSubdirectoryAwareDirectory with a parquet strategy whose native
     * file registry is a mock. Returns both the directory and the mock so tests can
     * verify calls routed to the registry.
     */
    private WithRegistry buildDirectoryWithParquetFormat() {
        return buildDirectoryWithParquetFormat(mock(DataFormatStoreHandler.class));
    }

    private WithRegistry buildDirectoryWithParquetFormat(DataFormatStoreHandler nativeRegistry) {
        DataFormatStoreHandlerFactory factory = (sid, warm, repo, cacheRegistry) -> nativeRegistry;
        StoreStrategy parquet = new TestParquetStrategy(factory);
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, parquet),
            remoteSegmentStoreDirectory,
            null
        );
        TieredSubdirectoryAwareDirectory dir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            registry,
            shardPath,
            getMockPrefetchSettingsSupplier()
        );
        return new WithRegistry(dir, nativeRegistry);
    }

    /** Writes a parquet file directly to disk (simulating the Rust writer). */
    private void writeParquetFileToDisk(String relativePath) throws IOException {
        Path fullPath = shardPath.getDataPath().resolve(relativePath);
        Files.createDirectories(fullPath.getParent());
        Files.write(fullPath, PARQUET_DATA);
    }

    /**
     * Directly adds a parquet file entry to the remote metadata map.
     * Parquet files don't have Lucene codec footers, so we can't use copyFrom.
     * In production, the upload path adds entries via a separate mechanism.
     */
    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "test needs reflection to inject parquet metadata without full upload pipeline")
    private void addParquetMetadataEntry(String localFilename, String uploadedFilename) {
        try {
            java.lang.reflect.Field field = RemoteSegmentStoreDirectory.class.getDeclaredField("segmentsUploadedToRemoteStore");
            field.setAccessible(true);
            java.util.concurrent.ConcurrentHashMap<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> map =
                (java.util.concurrent.ConcurrentHashMap<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata>) field.get(
                    remoteSegmentStoreDirectory
                );
            RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(
                localFilename + "::" + uploadedFilename + "::checksum123::100::" + org.apache.lucene.util.Version.LATEST.major
            );
            map.put(localFilename, metadata);
        } catch (Exception e) {
            throw new RuntimeException("Failed to add parquet metadata entry", e);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Routing tests — openInput
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInputLuceneFileRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String luceneFile = "_0_test.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            Path switchablePath = getFilePathSwitchable(localFsDir, luceneFile);
            assertNotNull("Lucene file should be in FileCache after createOutput", fileCache.get(switchablePath));
            fileCache.decRef(switchablePath);

            try (IndexInput in = directory.openInput(luceneFile, IOContext.DEFAULT)) {
                assertNotNull("openInput should return non-null for Lucene file", in);
                byte[] buf = new byte[TEST_DATA.length];
                in.readBytes(buf, 0, buf.length);
                assertArrayEquals("Data read back should match data written", TEST_DATA, buf);
            }
        } finally {
            directory.close();
        }
    }

    public void testOpenInputFormatFileRoutesToRemoteDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            // On read-only warm, openInput for format files goes to remoteDirectory.
            // Our mock remote has no parquet files, so this throws.
            expectThrows(Exception.class, () -> directory.openInput("parquet/seg.parquet", IOContext.DEFAULT));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Routing tests — fileLength
    // ═══════════════════════════════════════════════════════════════

    public void testFileLengthLuceneFile() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String luceneFile = "_0_len.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            long length = directory.fileLength(luceneFile);
            assertEquals("fileLength should match written data length", TEST_DATA.length, length);
        } finally {
            directory.close();
        }
    }

    public void testFileLengthFormatFileRoutesToRemote() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            expectThrows(Exception.class, () -> directory.fileLength("parquet/seg_len.parquet"));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // listAll tests
    // ═══════════════════════════════════════════════════════════════

    public void testListAllReturnsLuceneAndFormatFiles() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            try (IndexOutput out = directory.createOutput("_0_list.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            writeParquetFileToDisk("parquet/seg_list.parquet");

            String[] files = directory.listAll();
            Set<String> fileSet = new HashSet<>(Arrays.asList(files));
            assertTrue("listAll should contain Lucene file", fileSet.contains("_0_list.cfe"));
            assertTrue("listAll should contain parquet file", fileSet.contains("parquet/seg_list.parquet"));
        } finally {
            directory.close();
        }
    }

    public void testListAllWithEmptyFormatDirectories() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            try (IndexOutput out = directory.createOutput("_0_only.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            String[] files = directory.listAll();
            Set<String> fileSet = new HashSet<>(Arrays.asList(files));
            assertTrue("listAll should contain Lucene file", fileSet.contains("_0_only.cfe"));

            for (String f : files) {
                assertFalse("No parquet files should appear without format dirs", f.startsWith("parquet/"));
            }
        } finally {
            directory.close();
        }
    }

    public void testListAllSortedAndDeduplicates() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            try (IndexOutput out = directory.createOutput("_0_dup_a.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            try (IndexOutput out = directory.createOutput("_0_dup_b.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            String[] files = directory.listAll();
            for (int i = 1; i < files.length; i++) {
                assertTrue("listAll should return sorted results", files[i - 1].compareTo(files[i]) <= 0);
            }
            Set<String> fileSet = new HashSet<>(Arrays.asList(files));
            assertEquals("listAll should have no duplicates", fileSet.size(), files.length);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile tests
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFileLuceneRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String luceneFile = "_0_del.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            Set<String> beforeDelete = new HashSet<>(Arrays.asList(directory.listAll()));
            assertTrue("File should exist before delete", beforeDelete.contains(luceneFile));

            directory.deleteFile(luceneFile);

            Set<String> afterDelete = new HashSet<>(Arrays.asList(directory.listAll()));
            assertFalse("File should be gone after delete", afterDelete.contains(luceneFile));
        } finally {
            directory.close();
        }
    }

    public void testDeleteFileFormatRoutesToNativeRegistry() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            w.directory.deleteFile("parquet/seg_del.parquet");
            String expectedDelKey = shardPath.getDataPath().resolve("parquet/seg_del.parquet").toString();
            verify(w.storeHandler).onRemoved(expectedDelKey);
        } finally {
            w.directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // afterSyncToRemote tests
    // ═══════════════════════════════════════════════════════════════

    public void testAfterSyncToRemoteLuceneFile() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String luceneFile = "_0_sync.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            Path switchablePath = getFilePathSwitchable(localFsDir, luceneFile);
            assertNotNull("File should be in FileCache before afterSyncToRemote", fileCache.get(switchablePath));
            fileCache.decRef(switchablePath);

            directory.afterSyncToRemote(luceneFile);

            Integer refCount = fileCache.getRef(switchablePath);
            assertTrue("Ref count should be 0 or null after afterSyncToRemote", refCount == null || refCount == 0);
        } finally {
            directory.close();
        }
    }

    public void testAfterSyncToRemoteFormatFileRoutesToNativeRegistry() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        String parquetFile = "parquet/seg_sync.parquet";
        addParquetMetadataEntry(parquetFile, "seg_sync.parquet__UUID1");
        w.directory.afterSyncToRemote(parquetFile);
        String expectedUploadKey = shardPath.getDataPath().resolve(parquetFile).toString();
        verify(w.storeHandler).onUploaded(
            org.mockito.ArgumentMatchers.eq(expectedUploadKey),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong()
        );
    }

    public void testAfterSyncToRemotePassesCorrectFormatToGetRemoteBasePath() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        String parquetFile = "parquet/seg_format_path.parquet";
        addParquetMetadataEntry(parquetFile, "seg_format_path.parquet__UUID1");
        w.directory.afterSyncToRemote(parquetFile);
        // remotePath builds: basePath + blobKey. basePath from getRemoteBasePath("parquet")
        // returns "test-base-path/parquet/" (mock BlobPath includes format subdirectory),
        // so the path should be "test-base-path/parquet/seg_format_path.parquet__UUID1"
        String expectedUploadKey = shardPath.getDataPath().resolve(parquetFile).toString();
        verify(w.storeHandler).onUploaded(
            org.mockito.ArgumentMatchers.eq(expectedUploadKey),
            org.mockito.ArgumentMatchers.eq("test-base-path/parquet/seg_format_path.parquet__UUID1"),
            org.mockito.ArgumentMatchers.anyLong()
        );
    }

    public void testAfterSyncToRemoteFormatFileWithoutRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            String parquetFile = "parquet/seg_nosync.parquet";
            addParquetMetadataEntry(parquetFile, "seg_nosync.parquet__UUID2");
            directory.afterSyncToRemote(parquetFile);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput tests
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputLuceneFile() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String luceneFile = "_0_create.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            Path switchablePath = getFilePathSwitchable(localFsDir, luceneFile);
            assertNotNull("Lucene file should be cached in FileCache after createOutput", fileCache.get(switchablePath));
            fileCache.decRef(switchablePath);

            assertTrue("Lucene file should exist on local disk", Arrays.asList(localFsDir.listAll()).contains(luceneFile));
        } finally {
            directory.close();
        }
    }

    public void testFormatFileWrittenToDiskNotAccessibleViaRemote() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            String parquetFile = "parquet/seg_create.parquet";
            writeParquetFileToDisk(parquetFile);
            // File exists locally but not in remote metadata — should be readable from local.
            // This is the translog bump edge case: file created locally, not yet synced.
            long len = directory.fileLength(parquetFile);
            assertTrue("Local format file should have non-zero length", len > 0);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Edge case tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInputNonExistentFile() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            expectThrows(NoSuchFileException.class, () -> directory.openInput("non_existent_file.cfe", IOContext.DEFAULT));
        } finally {
            directory.close();
        }
    }

    public void testFileLengthNonExistentFile() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            expectThrows(Exception.class, () -> directory.fileLength("non_existent_file.cfe"));
        } finally {
            directory.close();
        }
    }

    public void testCloseClosesNativeRegistryAndTieredDirectory() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        w.directory.close();
        verify(w.storeHandler).close();
    }

    public void testCloseDoesNotDoubleCloseSharedSubdirectoryAwareDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            try (IndexOutput out = directory.createOutput("_0_noclose.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Constructor resource leak safety
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorFailureClosesStrategyRegistry() throws IOException {
        DataFormatStoreHandler nativeRegistry = mock(DataFormatStoreHandler.class);
        DataFormatStoreHandlerFactory factory = (sid, warm, repo, cacheRegistry) -> nativeRegistry;
        StoreStrategy parquet = new TestParquetStrategy(factory);
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath,
            true,
            NativeStoreRepository.EMPTY,
            Map.of(PARQUET_FORMAT, parquet),
            remoteSegmentStoreDirectory,
            null
        );

        try {
            new TieredSubdirectoryAwareDirectory(
                subdirAware,
                remoteSegmentStoreDirectory,
                null, // null fileCache → triggers IllegalStateException in CompositeDirectory
                threadPool,
                registry,
                shardPath,
                getMockPrefetchSettingsSupplier()
            );
            fail("Expected IllegalStateException from null fileCache");
        } catch (IllegalStateException e) {
            // Expected
        }

        // The registry (and its native registries) must have been closed by the constructor's
        // failure path so no native resources leak.
        verify(nativeRegistry).close();
    }

    // ═══════════════════════════════════════════════════════════════
    // IOUtils.close — partial close safety
    // ═══════════════════════════════════════════════════════════════

    public void testCloseWithThrowingNativeRegistryStillClosesTieredDirectory() throws IOException {
        DataFormatStoreHandler throwingRegistry = mock(DataFormatStoreHandler.class);
        org.mockito.Mockito.doThrow(new IOException("native close failed")).when(throwingRegistry).close();

        WithRegistry w = buildDirectoryWithParquetFormat(throwingRegistry);

        IOException ex = expectThrows(IOException.class, w.directory::close);
        assertEquals("native close failed", ex.getMessage());
        verify(throwingRegistry).close();
    }

    public void testAfterSyncToRemoteFormatFileNoopWhenNotRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            String parquetFile = "parquet/seg_noop.parquet";
            addParquetMetadataEntry(parquetFile, "seg_noop.parquet__UUID3");
            // Delegates to the native registry — must NOT fall through to tieredDirectory.
            directory.afterSyncToRemote(parquetFile);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // IllegalStateException guard tests (no matching strategy)
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInputUnregisteredFormatThrowsIllegalState() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> directory.openInput("csv/data.csv", IOContext.DEFAULT)
            );
            assertTrue(ex.getMessage().contains("csv"));
            assertTrue(ex.getMessage().contains("No StoreStrategy"));
        } finally {
            directory.close();
        }
    }

    public void testFileLengthUnregisteredFormatThrowsIllegalState() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> directory.fileLength("csv/data.csv"));
            assertTrue(ex.getMessage().contains("csv"));
        } finally {
            directory.close();
        }
    }

    public void testDeleteFileUnregisteredFormatThrowsIllegalState() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> directory.deleteFile("csv/data.csv"));
            assertTrue(ex.getMessage().contains("csv"));
        } finally {
            directory.close();
        }
    }

    public void testAfterSyncToRemoteUnregisteredFormatThrowsIllegalState() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> directory.afterSyncToRemote("csv/data.csv"));
            assertTrue(ex.getMessage().contains("csv"));
        } finally {
            directory.close();
        }
    }

    public void testLuceneFileWithNoStrategyRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            String luceneFile = "_0_guard.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            long length = directory.fileLength(luceneFile);
            assertEquals(TEST_DATA.length, length);
        } finally {
            directory.close();
        }
    }

    /** Minimal test strategy for "parquet" wiring. */
    private static final class TestParquetStrategy extends StoreStrategy {
        private final DataFormatStoreHandlerFactory factory;

        TestParquetStrategy(DataFormatStoreHandlerFactory factory) {
            this.factory = factory;
        }

        @Override
        public Optional<DataFormatStoreHandlerFactory> storeHandler() {
            return Optional.of(factory);
        }
    }

    private static final class WithRegistry {
        final TieredSubdirectoryAwareDirectory directory;
        final DataFormatStoreHandler storeHandler;

        WithRegistry(TieredSubdirectoryAwareDirectory directory, DataFormatStoreHandler storeHandler) {
            this.directory = directory;
            this.storeHandler = storeHandler;
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // sync() tests
    // ═══════════════════════════════════════════════════════════════

    public void testSyncIsNoOp() throws IOException {
        directory = buildDirectoryNoFormats();
        try {
            // sync should not throw even with non-existent files — it's a no-op on warm
            directory.sync(java.util.List.of("_0.cfe", "parquet/seg_0.parquet", "nonexistent.file"));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // rename() tests
    // ═══════════════════════════════════════════════════════════════

    public void testRenameLuceneFileDelegatesToTieredDirectory() throws IOException {
        directory = buildDirectoryNoFormats();
        try {
            // Write a file, then rename it (simulates Lucene commit: pending_segments → segments)
            try (IndexOutput out = directory.createOutput("pending_segments_1", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            directory.rename("pending_segments_1", "segments_1");
            // Original gone, new name exists
            assertTrue(Arrays.asList(directory.listAll()).contains("segments_1"));
        } finally {
            directory.close();
        }
    }

    public void testRenameFormatFileRoutesToSubdirectoryAware() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_rename.parquet";
            writeParquetFileToDisk(parquetFile);
            // Rename should succeed — routes through SubdirectoryAwareDirectory for recovery support
            w.directory.rename(parquetFile, "parquet/seg_renamed.parquet");
            // Verify renamed file exists
            assertTrue(java.nio.file.Files.exists(shardPath.getDataPath().resolve("parquet/seg_renamed.parquet")));
            assertFalse(java.nio.file.Files.exists(shardPath.getDataPath().resolve(parquetFile)));
        } finally {
            w.directory.close();
        }
    }

    public void testRenameRecoveryPrefixedFormatFile() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            // Simulate recovery: write with recovery prefix, then rename to final name
            String recoveryFile = "parquet/recovery.abc123.seg_0.parquet";
            String finalFile = "parquet/seg_0.parquet";
            writeParquetFileToDisk(recoveryFile);
            w.directory.rename(recoveryFile, finalFile);
            assertTrue(java.nio.file.Files.exists(shardPath.getDataPath().resolve(finalFile)));
            assertFalse(java.nio.file.Files.exists(shardPath.getDataPath().resolve(recoveryFile)));
        } finally {
            w.directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // listAll() tests
    // ═══════════════════════════════════════════════════════════════

    public void testListAllIncludesLuceneFiles() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            String[] files = directory.listAll();
            // Should contain Lucene files from remote metadata (populated in setup)
            assertTrue("Should contain _0.si", Arrays.asList(files).contains("_0.si"));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // afterSyncToRemote() — null blobKey test
    // ═══════════════════════════════════════════════════════════════

    public void testAfterSyncToRemoteThrowsWhenBlobKeyNull() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            // "parquet/unknown.parquet" is a format file but has no remote metadata entry
            // → getExistingRemoteFilename returns null → should throw
            IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> w.directory.afterSyncToRemote("parquet/unknown.parquet")
            );
            assertTrue(ex.getMessage().contains("parquet/unknown.parquet"));
            assertTrue(ex.getMessage().contains("no remote filename"));
        } finally {
            w.directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Local-to-remote routing and afterSyncToRemote local delete tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInputRoutesToLocalWhenNotInRemoteMetadata() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            String parquetFile = "parquet/seg_local_only.parquet";
            writeParquetFileToDisk(parquetFile);
            // File exists locally but NOT in remote metadata → should read from local
            IndexInput input = directory.openInput(parquetFile, IOContext.DEFAULT);
            assertNotNull(input);
            assertTrue("Local format file should have non-zero length", input.length() > 0);
            input.close();
        } finally {
            directory.close();
        }
    }

    public void testOpenInputRoutesToRemoteWhenInRemoteMetadata() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String parquetFile = "parquet/seg_remote.parquet";
            addParquetMetadataEntry(parquetFile, "seg_remote.parquet__UUID1");
            // File is in remote metadata → should route to remote directory
            // (remote directory is mocked, so this verifies routing not actual read)
            IndexInput input = directory.openInput(parquetFile, IOContext.DEFAULT);
            assertNotNull(input);
            input.close();
        } finally {
            directory.close();
        }
    }

    public void testAfterSyncToRemoteDeletesLocalCopy() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_delete_local.parquet";
            writeParquetFileToDisk(parquetFile);
            // Verify file exists locally
            assertTrue(java.nio.file.Files.exists(shardPath.getDataPath().resolve(parquetFile)));
            // Simulate sync: add remote metadata entry
            addParquetMetadataEntry(parquetFile, "seg_delete_local.parquet__UUID1");
            // afterSyncToRemote should register as REMOTE and delete local copy
            w.directory.afterSyncToRemote(parquetFile);
            // Local file should be gone
            assertFalse(
                "Local file should be deleted after sync to remote",
                java.nio.file.Files.exists(shardPath.getDataPath().resolve(parquetFile))
            );
        } finally {
            w.directory.close();
        }
    }

    public void testAfterSyncToRemoteNoErrorWhenLocalAlreadyGone() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_already_gone.parquet";
            // Don't write file to disk — it's already gone
            addParquetMetadataEntry(parquetFile, "seg_already_gone.parquet__UUID1");
            // Should not throw — catches NoSuchFileException silently
            w.directory.afterSyncToRemote(parquetFile);
        } finally {
            w.directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // FormatSwitchableIndexInput integration tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInputLocalFormatFileReturnsFormatSwitchable() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_switchable.parquet";
            writeParquetFileToDisk(parquetFile);
            // File NOT in remote metadata → openInput takes local path → wraps in FormatSwitchableIndexInput
            IndexInput input = w.directory.openInput(parquetFile, IOContext.DEFAULT);
            assertTrue(
                "Local format file should be wrapped in FormatSwitchableIndexInput",
                input instanceof org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper
            );
            input.close();
        } finally {
            w.directory.close();
        }
    }

    public void testOpenInputRemoteFormatFileDoesNotWrapInSwitchable() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            String parquetFile = "parquet/seg_remote_nowrap.parquet";
            addParquetMetadataEntry(parquetFile, "seg_remote_nowrap.parquet__UUID1");
            // File IS in remote metadata → openInput routes directly to remote, no switchable wrapper
            IndexInput input = directory.openInput(parquetFile, IOContext.DEFAULT);
            assertFalse(
                "Remote format file should NOT be wrapped in FormatSwitchableIndexInput",
                input instanceof org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper
            );
            input.close();
        } finally {
            directory.close();
        }
    }

    public void testAfterSyncToRemoteSwitchesInFlightReader() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        populateData();
        try {
            String parquetFile = "parquet/seg_inflight.parquet";
            writeParquetFileToDisk(parquetFile);

            // Open input while file is local (not yet synced)
            IndexInput input = w.directory.openInput(parquetFile, IOContext.DEFAULT);
            assertTrue(input instanceof org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper);
            org.opensearch.storage.indexinput.FormatSwitchableIndexInput switchable =
                ((org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper) input).unwrap();
            assertFalse("Should start on local", switchable.hasSwitchedToRemote());

            // Read some bytes from local
            byte[] buf = new byte[PARQUET_DATA.length];
            switchable.readBytes(buf, 0, buf.length);
            assertArrayEquals("Should read local data before sync", PARQUET_DATA, buf);

            // Now simulate sync: add remote metadata and call afterSyncToRemote
            addParquetMetadataEntry(parquetFile, "seg_inflight.parquet__UUID1");
            w.directory.afterSyncToRemote(parquetFile);

            // The switchable should now be on remote
            assertTrue("Should be switched to remote after afterSyncToRemote", switchable.hasSwitchedToRemote());

            // Local file should be deleted
            assertFalse("Local file should be deleted", java.nio.file.Files.exists(shardPath.getDataPath().resolve(parquetFile)));

            input.close();
        } finally {
            w.directory.close();
        }
    }

    public void testAfterSyncToRemoteSwitchesClonesOfInFlightReader() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        populateData();
        try {
            String parquetFile = "parquet/seg_clone_switch.parquet";
            writeParquetFileToDisk(parquetFile);

            // Open input and clone it
            IndexInput input = w.directory.openInput(parquetFile, IOContext.DEFAULT);
            assertTrue(input instanceof org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper);
            org.opensearch.storage.indexinput.FormatSwitchableIndexInput switchable =
                ((org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper) input).unwrap();
            assertFalse(switchable.hasSwitchedToRemote());

            // Sync — the switch cascades to clones internally (tested in FormatSwitchableIndexInputTests)
            addParquetMetadataEntry(parquetFile, "seg_clone_switch.parquet__UUID1");
            w.directory.afterSyncToRemote(parquetFile);

            assertTrue("Original should be switched", switchable.hasSwitchedToRemote());

            input.close();
        } finally {
            w.directory.close();
        }
    }

    public void testAfterSyncToRemoteWithNoOpenInputStillDeletesLocal() throws IOException {
        WithRegistry w = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_no_reader.parquet";
            writeParquetFileToDisk(parquetFile);
            assertTrue(java.nio.file.Files.exists(shardPath.getDataPath().resolve(parquetFile)));

            // No openInput call — no FormatSwitchableIndexInput tracked
            addParquetMetadataEntry(parquetFile, "seg_no_reader.parquet__UUID1");
            w.directory.afterSyncToRemote(parquetFile);

            // Local file should still be deleted
            assertFalse(
                "Local file should be deleted even without open readers",
                java.nio.file.Files.exists(shardPath.getDataPath().resolve(parquetFile))
            );
        } finally {
            w.directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput — segments_N local fallback tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInputSegmentsFileReadsFromLocalWhenNotInRemoteMetadata() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            // Write a segments_N file locally (simulates a restart where local generation differs from remote)
            String segmentsFile = "segments_5";
            try (IndexOutput out = subdirAware.createOutput(segmentsFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            // segments_5 is NOT in remote metadata → should read from local disk
            try (IndexInput in = directory.openInput(segmentsFile, IOContext.DEFAULT)) {
                assertNotNull("openInput should return non-null for local segments file", in);
                byte[] buf = new byte[TEST_DATA.length];
                in.readBytes(buf, 0, buf.length);
                assertArrayEquals("Should read local segments file data", TEST_DATA, buf);
            }
        } finally {
            directory.close();
        }
    }

    public void testOpenInputSegmentsFileFallsToTieredDirectoryWhenNotLocal() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            // segments_99 doesn't exist locally or in remote metadata.
            // The local read throws NoSuchFileException, then falls through to TieredDirectory.
            // TieredDirectory also won't find it → expect an exception.
            expectThrows(Exception.class, () -> directory.openInput("segments_99", IOContext.DEFAULT));
        } finally {
            directory.close();
        }
    }

    public void testOpenInputSegmentsFileInRemoteMetadataRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            // segments_N that IS in remote metadata should NOT take the local fallback path.
            // It should go through TieredDirectory (the normal Lucene file path).
            // The remote metadata from populateMetadata() includes standard Lucene files.
            // We'll use a segments file that's in remote metadata by adding it.
            String segmentsFile = "segments_2";
            addParquetMetadataEntry(segmentsFile, "segments_2__UUID1");

            // Since it's in remote metadata, getExistingRemoteFilename != null → skips local fallback.
            // Goes to tieredDirectory.openInput which reads from remote via FileCache.
            try (IndexInput in = directory.openInput(segmentsFile, IOContext.DEFAULT)) {
                assertNotNull("Should read segments file from TieredDirectory when in remote metadata", in);
            }
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput — format file routing tests
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputFormatFileRoutesToSubdirectoryAwareDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            String parquetFile = "parquet/seg_create_output.parquet";
            // createOutput for format files should route through SubdirectoryAwareDirectory
            // which creates parent directories automatically.
            try (IndexOutput out = directory.createOutput(parquetFile, IOContext.DEFAULT)) {
                out.writeBytes(PARQUET_DATA, PARQUET_DATA.length);
            }

            // Verify the file was written to disk via SubdirectoryAwareDirectory path resolution
            Path expectedPath = shardPath.getDataPath().resolve(parquetFile);
            assertTrue("Format file should exist on disk after createOutput", Files.exists(expectedPath));
            byte[] content = Files.readAllBytes(expectedPath);
            assertArrayEquals("Written content should match", PARQUET_DATA, content);
        } finally {
            directory.close();
        }
    }

    public void testCreateOutputFormatFileCreatesParentDirectories() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            // Use a nested subdirectory path to verify parent dir creation
            String parquetFile = "parquet/nested/seg_nested.parquet";
            // This would fail without SubdirectoryAwareDirectory creating parent dirs
            try (IndexOutput out = directory.createOutput(parquetFile, IOContext.DEFAULT)) {
                out.writeBytes(PARQUET_DATA, PARQUET_DATA.length);
            }

            Path expectedPath = shardPath.getDataPath().resolve(parquetFile);
            assertTrue("Nested format file should exist after createOutput", Files.exists(expectedPath));
        } finally {
            directory.close();
        }
    }

    public void testCreateOutputLuceneFileStillRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        populateData();
        try {
            // Lucene files should still go through TieredDirectory (FileCache integration)
            String luceneFile = "_0_create_lucene.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            // Verify it's in the FileCache (TieredDirectory behavior)
            Path switchablePath = getFilePathSwitchable(localFsDir, luceneFile);
            assertNotNull("Lucene file should be in FileCache via TieredDirectory", fileCache.get(switchablePath));
            fileCache.decRef(switchablePath);
        } finally {
            directory.close();
        }
    }
}
