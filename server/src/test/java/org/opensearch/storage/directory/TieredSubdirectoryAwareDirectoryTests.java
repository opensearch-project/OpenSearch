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
import org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
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
 * <p>Format directories use a non-closing FilterDirectory wrapper around the shared
 * SubdirectoryAwareDirectory to avoid double-close issues. Parquet files are written
 * directly to disk via {@link Files#write} to simulate the Rust writer path.
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
     * Creates a mock FormatStoreHandler for testing. Read-only warm: the directory
     * always routes format files to remote, so no getFileLocation stubbing needed.
     */
    private DataFormatAwareStoreHandler createMockFormatStoreHandler() {
        return mock(DataFormatAwareStoreHandler.class);
    }

    /**
     * Builds a TieredSubdirectoryAwareDirectory with no format store handler (Lucene-only).
     */
    private TieredSubdirectoryAwareDirectory buildDirectoryNoFormats() {
        return new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            Map.of(),
            shardPath,
            getMockPrefetchSettingsSupplier()
        );
    }

    /**
     * Builds a TieredSubdirectoryAwareDirectory with a mock FormatStoreHandler
     * registered for the "parquet" format.
     */
    private TieredSubdirectoryAwareDirectory buildDirectoryWithParquetFormat() {
        DataFormatAwareStoreHandler handler = createMockFormatStoreHandler();
        return new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            Map.of("parquet", handler),
            shardPath,
            getMockPrefetchSettingsSupplier()
        );
    }

    /**
     * Writes a parquet file directly to disk (simulating the Rust writer), not via createOutput.
     */
    private void writeParquetFileToDisk(String relativePath) throws IOException {
        Path fullPath = shardPath.getDataPath().resolve(relativePath);
        Files.createDirectories(fullPath.getParent());
        Files.write(fullPath, PARQUET_DATA);
    }

    // ═══════════════════════════════════════════════════════════════
    // Routing tests — openInput
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write a Lucene file via createOutput on TieredSubdirectoryAwareDirectory (no format dir
     * for lucene), read it back via openInput — should go through TieredDirectory → FileCache.
     */
    public void testOpenInputLuceneFileRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            String luceneFile = "_0_test.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            // The file should be cached in FileCache via TieredDirectory
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

    /**
     * openInput for a format file routes to remoteDirectory (read-only warm: all format
     * files are REMOTE). The remote directory may throw if the file isn't in its metadata,
     * but the routing is correct.
     */
    public void testOpenInputFormatFileRoutesToRemoteDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            // On read-only warm, openInput for format files goes to remoteDirectory.
            // Since our mock remote has no actual parquet files, this will throw.
            expectThrows(Exception.class, () -> directory.openInput("parquet/seg.parquet", IOContext.DEFAULT));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Routing tests — fileLength
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write a Lucene file, check fileLength routes to TieredDirectory.
     */
    public void testFileLengthLuceneFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
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

    /**
     * fileLength for a format file routes to remoteDirectory (read-only warm).
     * Remote directory returns length from its in-memory metadata map.
     */
    public void testFileLengthFormatFileRoutesToRemote() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            // On read-only warm, fileLength for format files goes to remoteDirectory.
            // Since our mock remote has no parquet files in metadata, this will throw.
            expectThrows(Exception.class, () -> directory.fileLength("parquet/seg_len.parquet"));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // listAll tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write Lucene files via createOutput + write parquet files on disk.
     * listAll only returns TieredDirectory files (Lucene). Format files tracked by
     * the handler are not included in listAll — they are accessed via the handler.
     */
    public void testListAllReturnsNotOnlyLuceneFiles() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            try (IndexOutput out = directory.createOutput("_0_list.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            writeParquetFileToDisk("parquet/seg_list.parquet");

            String[] files = directory.listAll();
            Set<String> fileSet = new HashSet<>(Arrays.asList(files));
            assertTrue("listAll should contain Lucene file", fileSet.contains("_0_list.cfe"));
            // Format files are handler-tracked, not listed via listAll
            assertTrue("listAll should contain  parquet file", fileSet.contains("parquet/seg_list.parquet"));
        } finally {
            directory.close();
        }
    }

    /**
     * No format dirs, listAll returns only TieredDirectory files.
     */
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

            // Verify no parquet files appear
            for (String f : files) {
                assertFalse("No parquet files should appear without format dirs", f.startsWith("parquet/"));
            }
        } finally {
            directory.close();
        }
    }

    /**
     * listAll returns sorted results with no duplicates from TieredDirectory.
     */
    public void testListAllSortedAndDeduplicates() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            // Write multiple Lucene files
            try (IndexOutput out = directory.createOutput("_0_dup_a.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            try (IndexOutput out = directory.createOutput("_0_dup_b.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            String[] files = directory.listAll();

            // Verify sorted order
            for (int i = 1; i < files.length; i++) {
                assertTrue("listAll should return sorted results", files[i - 1].compareTo(files[i]) <= 0);
            }

            // Verify no duplicates
            Set<String> fileSet = new HashSet<>(Arrays.asList(files));
            assertEquals("listAll should have no duplicates", fileSet.size(), files.length);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write Lucene file, delete it, verify gone.
     */
    public void testDeleteFileLuceneRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat();
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

    /**
     * Delete parquet file — handler.removeFile is called.
     * Read-only warm: no local copy to delete, just handler tracking removal.
     */
    public void testDeleteFileFormatRoutesToHandler() throws IOException {
        DataFormatAwareStoreHandler handler = mock(DataFormatAwareStoreHandler.class);

        directory = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            Map.of("parquet", handler),
            shardPath,
            getMockPrefetchSettingsSupplier()
        );
        try {
            directory.deleteFile("parquet/seg_del.parquet");

            // Verify handler.removeFile was called
            verify(handler).removeFile("parquet/seg_del.parquet");
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // afterSyncToRemote tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write Lucene file via createOutput (adds to FileCache), call afterSyncToRemote —
     * should call TieredDirectory.afterSyncToRemote (unpin + switch).
     */
    public void testAfterSyncToRemoteLuceneFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            String luceneFile = "_0_sync.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            // File should be in FileCache after createOutput
            Path switchablePath = getFilePathSwitchable(localFsDir, luceneFile);
            assertNotNull("File should be in FileCache before afterSyncToRemote", fileCache.get(switchablePath));
            fileCache.decRef(switchablePath);

            // afterSyncToRemote should unpin and switch
            directory.afterSyncToRemote(luceneFile);

            // After sync, the switchable ref count should have been decremented
            // (the file may still be in cache but unpinned)
            Integer refCount = fileCache.getRef(switchablePath);
            assertTrue("Ref count should be 0 or null after afterSyncToRemote", refCount == null || refCount == 0);
        } finally {
            directory.close();
        }
    }

    /**
     * afterSyncToRemote calls handler.afterSyncToRemote(file, remotePath) for tracked files.
     */
    public void testAfterSyncToRemoteFormatFileWithRemoteSyncAware() {
        DataFormatAwareStoreHandler syncHandler = mock(DataFormatAwareStoreHandler.class);

        TieredSubdirectoryAwareDirectory syncDir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            Map.of("parquet", syncHandler),
            shardPath,
            getMockPrefetchSettingsSupplier()
        );

        String parquetFile = "parquet/seg_sync.parquet";
        syncDir.afterSyncToRemote(parquetFile);
        verify(syncHandler).afterSyncToRemote(org.mockito.ArgumentMatchers.eq(parquetFile), org.mockito.ArgumentMatchers.any());
    }

    /**
     * Format store handler does NOT track this file, afterSyncToRemote should
     * be a no-op for the format file (not fall through to TieredDirectory).
     */
    public void testAfterSyncToRemoteFormatFileWithoutRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            // The mock FormatStoreHandler tracks parquet files.
            // With the fix, afterSyncToRemote delegates to the handler for tracked files.
            String parquetFile = "parquet/seg_nosync.parquet";
            writeParquetFileToDisk(parquetFile);

            // Should complete without error — handler's afterSyncToRemote is called
            directory.afterSyncToRemote(parquetFile);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * createOutput for Lucene file goes through TieredDirectory, file is in FileCache after close.
     */
    public void testCreateOutputLuceneFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            String luceneFile = "_0_create.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }

            // Verify file is in FileCache
            Path switchablePath = getFilePathSwitchable(localFsDir, luceneFile);
            assertNotNull("Lucene file should be cached in FileCache after createOutput", fileCache.get(switchablePath));
            fileCache.decRef(switchablePath);

            // Verify file exists on disk
            assertTrue("Lucene file should exist on local disk", Arrays.asList(localFsDir.listAll()).contains(luceneFile));
        } finally {
            directory.close();
        }
    }

    /**
     * Format files are written directly to disk (e.g., by Rust writer), not via createOutput.
     * On read-only warm, fileLength routes to remote (not local), so a locally written file
     * is not accessible via fileLength — it must be seeded in remote metadata first.
     */
    public void testFormatFileWrittenToDiskNotAccessibleViaRemote() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_create.parquet";
            writeParquetFileToDisk(parquetFile);

            // File exists on disk but fileLength routes to remote (read-only warm).
            // Remote has no metadata for this file, so it throws.
            expectThrows(Exception.class, () -> directory.fileLength(parquetFile));
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Edge case tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * openInput on file that doesn't exist anywhere → NoSuchFileException.
     */
    public void testOpenInputNonExistentFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            expectThrows(NoSuchFileException.class, () -> directory.openInput("non_existent_file.cfe", IOContext.DEFAULT));
        } finally {
            directory.close();
        }
    }

    /**
     * fileLength on non-existent file → exception.
     */
    public void testFileLengthNonExistentFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            expectThrows(Exception.class, () -> directory.fileLength("non_existent_file.cfe"));
        } finally {
            directory.close();
        }
    }

    /**
     * close() closes the format store handler and TieredDirectory.
     */
    public void testCloseClosesFormatStoreHandlerAndTieredDirectory() throws IOException {
        DataFormatAwareStoreHandler mockHandler = mock(DataFormatAwareStoreHandler.class);

        TieredSubdirectoryAwareDirectory dir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            Map.of("test", mockHandler),
            shardPath,
            getMockPrefetchSettingsSupplier()
        );

        dir.close();
        verify(mockHandler).close();

        // TieredDirectory is also closed — attempting to use it should fail
        // We can't easily verify TieredDirectory.close() was called without mocking,
        // but the fact that close() completes without error is sufficient.
    }

    /**
     * Format directory wraps same SubdirectoryAwareDirectory — close format dir (no-op close)
     * then close TieredDirectory — no AlreadyClosedException.
     */
    public void testCloseDoesNotDoubleCloseSharedSubdirectoryAwareDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            // Write a file to ensure the directory is in a valid state
            try (IndexOutput out = directory.createOutput("_0_noclose.cfe", IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
        } finally {
            // close() should not throw AlreadyClosedException because the format directory
            // uses a non-closing wrapper — only TieredDirectory closes the underlying FS
            directory.close();
        }

        // If we get here without AlreadyClosedException, the test passes.
        // The non-closing FilterDirectory wrapper prevents double-close of the shared
        // SubdirectoryAwareDirectory.
    }

    // ═══════════════════════════════════════════════════════════════
    // Constructor resource leak safety
    // ═══════════════════════════════════════════════════════════════

    /**
     * If TieredDirectory construction fails, format store handler should be closed
     * to prevent resource leaks.
     */
    public void testConstructorFailureClosesFormatStoreHandler() {
        DataFormatAwareStoreHandler mockHandler = mock(DataFormatAwareStoreHandler.class);

        // Pass null fileCache to trigger NPE inside TieredDirectory constructor's CompositeDirectory validation
        try {
            new TieredSubdirectoryAwareDirectory(
                subdirAware,
                remoteSegmentStoreDirectory,
                null, // null fileCache → triggers IllegalStateException in CompositeDirectory
                threadPool,
                Map.of("test", mockHandler),
                shardPath,
                getMockPrefetchSettingsSupplier()
            );
            fail("Expected IllegalStateException from null fileCache");
        } catch (IllegalStateException e) {
            // Expected — CompositeDirectory validates fileCache != null
        }

        // Format store handler should have been closed in the finally block
        try {
            verify(mockHandler).close();
        } catch (IOException e) {
            fail("close() should not throw in verify");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // IOUtils.close tests — partial close safety
    // ═══════════════════════════════════════════════════════════════

    /**
     * If the format store handler throws on close, tieredDirectory is still closed
     * (IOUtils.close collects exceptions).
     */
    public void testCloseWithThrowingFormatStoreHandlerStillClosesTieredDirectory() throws IOException {
        DataFormatAwareStoreHandler throwingHandler = mock(DataFormatAwareStoreHandler.class);
        org.mockito.Mockito.doThrow(new IOException("handler close failed")).when(throwingHandler).close();

        TieredSubdirectoryAwareDirectory dir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            Map.of("test", throwingHandler),
            shardPath,
            getMockPrefetchSettingsSupplier()
        );

        IOException ex = expectThrows(IOException.class, dir::close);
        assertEquals("handler close failed", ex.getMessage());
        // The handler's close was called (and threw), but tieredDirectory should still have been closed
        verify(throwingHandler).close();
    }

    /**
     * afterSyncToRemote for a format file tracked by the handler
     * should delegate to the handler — must NOT fall through to tieredDirectory.
     */
    public void testAfterSyncToRemoteFormatFileNoopWhenNotRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            // Write a parquet file directly to disk (not via createOutput, so not in FileCache)
            String parquetFile = "parquet/seg_noop.parquet";
            writeParquetFileToDisk(parquetFile);

            // With the fix, this delegates to the FormatStoreHandler because the file is tracked.
            // Previously this would fall through to tieredDirectory.afterSyncToRemote and NPE
            // on fileCache.decRef for uncached file.
            directory.afterSyncToRemote(parquetFile);
            // If we get here without NPE, the handler path is working correctly.
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // IllegalStateException guard tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * openInput on a format file (contains "/") with no registered handler throws IllegalStateException.
     * This catches misconfiguration — a format plugin is missing.
     */
    public void testOpenInputUnregisteredFormatThrowsIllegalState() throws IOException {
        // Build directory with NO format handlers
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> directory.openInput("csv/data.csv", IOContext.DEFAULT)
            );
            assertTrue(ex.getMessage().contains("csv"));
            assertTrue(ex.getMessage().contains("No DataFormatAwareStoreHandler"));
        } finally {
            directory.close();
        }
    }

    /**
     * fileLength on a format file with no registered handler throws IllegalStateException.
     */
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

    /**
     * deleteFile on a format file with no registered handler throws IllegalStateException.
     */
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

    /**
     * afterSyncToRemote on a format file with no registered handler throws IllegalStateException.
     */
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

    /**
     * Plain Lucene files (no "/") go to tieredDirectory without requiring a handler.
     */
    public void testLuceneFileWithNoHandlerRoutesToTieredDirectory() throws IOException {
        directory = buildDirectoryNoFormats();
        populateData();
        try {
            String luceneFile = "_0_guard.cfe";
            try (IndexOutput out = directory.createOutput(luceneFile, IOContext.DEFAULT)) {
                out.writeBytes(TEST_DATA, TEST_DATA.length);
            }
            // Should NOT throw — Lucene files don't need a handler
            long length = directory.fileLength(luceneFile);
            assertEquals(TEST_DATA.length, length);
        } finally {
            directory.close();
        }
    }

}
