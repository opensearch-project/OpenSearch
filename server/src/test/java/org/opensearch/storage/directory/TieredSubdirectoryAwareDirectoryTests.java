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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.NativeFileRegistry;
import org.opensearch.index.engine.dataformat.NativeFileRegistryFactory;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteDirectory;
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
 * {@link StoreStrategy} whose {@link NativeFileRegistryFactory} returns a Mockito-mocked
 * {@link NativeFileRegistry} — the mock verifies {@code onUploaded} / {@code onRemoved} /
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

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();

        // Stub getBlobContainer().path() so getRemoteBasePath() doesn't NPE in afterSyncToRemote tests
        BlobContainer mockBlobContainer = mock(BlobContainer.class);
        when(mockBlobContainer.path()).thenReturn(new BlobPath().add("test-base-path"));
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
        return buildDirectoryWithParquetFormat(mock(NativeFileRegistry.class));
    }

    private WithRegistry buildDirectoryWithParquetFormat(NativeFileRegistry nativeRegistry) {
        NativeFileRegistryFactory factory = (sid, warm, repo) -> nativeRegistry;
        StoreStrategy parquet = new TestParquetStrategy(factory);
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath.getShardId(),
            true,
            NativeStoreRepository.EMPTY,
            Map.of("parquet", parquet),
            remoteSegmentStoreDirectory
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
            verify(w.nativeRegistry).onRemoved("parquet/seg_del.parquet");
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

    public void testAfterSyncToRemoteFormatFileRoutesToNativeRegistry() {
        WithRegistry w = buildDirectoryWithParquetFormat();
        String parquetFile = "parquet/seg_sync.parquet";
        w.directory.afterSyncToRemote(parquetFile);
        verify(w.nativeRegistry).onUploaded(
            org.mockito.ArgumentMatchers.eq(parquetFile),
            org.mockito.ArgumentMatchers.any()
        );
    }

    public void testAfterSyncToRemoteFormatFileWithoutRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat().directory;
        try {
            String parquetFile = "parquet/seg_nosync.parquet";
            writeParquetFileToDisk(parquetFile);
            // Delegates to the native registry (even if the file isn't tracked).
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
            expectThrows(Exception.class, () -> directory.fileLength(parquetFile));
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
        verify(w.nativeRegistry).close();
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
        NativeFileRegistry nativeRegistry = mock(NativeFileRegistry.class);
        NativeFileRegistryFactory factory = (sid, warm, repo) -> nativeRegistry;
        StoreStrategy parquet = new TestParquetStrategy(factory);
        StoreStrategyRegistry registry = StoreStrategyRegistry.open(
            shardPath.getShardId(),
            true,
            NativeStoreRepository.EMPTY,
            Map.of("parquet", parquet),
            remoteSegmentStoreDirectory
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
        NativeFileRegistry throwingRegistry = mock(NativeFileRegistry.class);
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
            writeParquetFileToDisk(parquetFile);
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
    private static final class TestParquetStrategy implements StoreStrategy {
        private final NativeFileRegistryFactory factory;

        TestParquetStrategy(NativeFileRegistryFactory factory) {
            this.factory = factory;
        }

        @Override
        public Optional<NativeFileRegistryFactory> nativeFileRegistry() {
            return Optional.of(factory);
        }
    }

    private static final class WithRegistry {
        final TieredSubdirectoryAwareDirectory directory;
        final NativeFileRegistry nativeRegistry;

        WithRegistry(TieredSubdirectoryAwareDirectory directory, NativeFileRegistry nativeRegistry) {
            this.directory = directory;
            this.nativeRegistry = nativeRegistry;
        }
    }
}
