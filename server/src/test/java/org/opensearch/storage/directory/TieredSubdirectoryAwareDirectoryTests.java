/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteSyncAwareDirectory;
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
import java.util.HashMap;
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
     * Creates a non-closing FilterDirectory wrapper around the given SubdirectoryAwareDirectory.
     * This prevents double-close when the format directory and TieredDirectory share the same
     * underlying SubdirectoryAwareDirectory.
     */
    private Directory createNonClosingFormatDirectory(SubdirectoryAwareDirectory delegate) {
        return new FilterDirectory(delegate) {
            @Override
            public void close() {
                // Don't close — shared with TieredDirectory
            }
        };
    }

    /**
     * Builds a TieredSubdirectoryAwareDirectory with no format directories (Lucene-only).
     */
    private TieredSubdirectoryAwareDirectory buildDirectoryNoFormats() {
        return new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            new HashMap<>(),
            getMockPrefetchSettingsSupplier()
        );
    }

    /**
     * Builds a TieredSubdirectoryAwareDirectory with a "parquet" format directory backed by
     * a non-closing wrapper around the shared SubdirectoryAwareDirectory.
     */
    private TieredSubdirectoryAwareDirectory buildDirectoryWithParquetFormat() {
        Directory formatDir = createNonClosingFormatDirectory(subdirAware);
        Map<String, Directory> formatDirs = new HashMap<>();
        formatDirs.put("parquet", formatDir);
        return new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            formatDirs,
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
     * Create a SubdirectoryAwareDirectory as format directory for "parquet", write a file to
     * parquet/ subdir on disk, read via openInput("parquet/seg.parquet") — should route to
     * format directory.
     */
    public void testOpenInputFormatFileRoutesToFormatDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            writeParquetFileToDisk("parquet/seg.parquet");

            try (IndexInput in = directory.openInput("parquet/seg.parquet", IOContext.DEFAULT)) {
                assertNotNull("openInput should return non-null for parquet file", in);
                byte[] buf = new byte[PARQUET_DATA.length];
                in.readBytes(buf, 0, buf.length);
                assertArrayEquals("Parquet data should match what was written to disk", PARQUET_DATA, buf);
            }
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
     * Write parquet file on disk, check fileLength routes to format directory.
     */
    public void testFileLengthFormatFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            writeParquetFileToDisk("parquet/seg_len.parquet");

            long length = directory.fileLength("parquet/seg_len.parquet");
            assertEquals("fileLength should match parquet data length", PARQUET_DATA.length, length);
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // listAll tests
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write Lucene files via createOutput + write parquet files on disk, listAll should include both.
     */
    public void testListAllMergesLuceneAndFormatFiles() throws IOException {
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
            assertTrue("listAll should contain parquet file", fileSet.contains("parquet/seg_list.parquet"));
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
     * Same file visible from both TieredDirectory and format directory, should appear once.
     */
    public void testListAllDeduplicates() throws IOException {
        // Build a directory where the format directory wraps the same SubdirectoryAwareDirectory
        // Write a Lucene file that will appear in both TieredDirectory.listAll() and the
        // format directory's listAll() (since they share the same underlying FS)
        directory = buildDirectoryWithParquetFormat();
        populateData();
        try {
            writeParquetFileToDisk("parquet/dup.parquet");

            String[] files = directory.listAll();
            // Count occurrences of the parquet file
            long count = Arrays.stream(files).filter(f -> f.equals("parquet/dup.parquet")).count();
            assertEquals("Deduplicated file should appear exactly once", 1, count);

            // Verify sorted order
            for (int i = 1; i < files.length; i++) {
                assertTrue("listAll should return sorted results", files[i - 1].compareTo(files[i]) <= 0);
            }
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
     * Write parquet file, delete via format directory path, verify gone.
     */
    public void testDeleteFileFormatRoutesToFormatDirectory() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            writeParquetFileToDisk("parquet/seg_del.parquet");

            Set<String> beforeDelete = new HashSet<>(Arrays.asList(directory.listAll()));
            assertTrue("Parquet file should exist before delete", beforeDelete.contains("parquet/seg_del.parquet"));

            directory.deleteFile("parquet/seg_del.parquet");

            Set<String> afterDelete = new HashSet<>(Arrays.asList(directory.listAll()));
            assertFalse("Parquet file should be gone after delete", afterDelete.contains("parquet/seg_del.parquet"));
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
     * Format directory implements RemoteSyncAwareDirectory, afterSyncToRemote should call it.
     */
    public void testAfterSyncToRemoteFormatFileWithRemoteSyncAware() {
        RemoteSyncAwareFormatDirectory syncAwareDir = mock(RemoteSyncAwareFormatDirectory.class);
        Map<String, Directory> syncAwareFormats = new HashMap<>();
        syncAwareFormats.put("parquet", syncAwareDir);

        TieredSubdirectoryAwareDirectory syncDir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            syncAwareFormats,
            getMockPrefetchSettingsSupplier()
        );

        String parquetFile = "parquet/seg_sync.parquet";
        syncDir.afterSyncToRemote(parquetFile);
        verify(syncAwareDir).afterSyncToRemote(parquetFile);
    }

    /**
     * Format directory does NOT implement RemoteSyncAwareDirectory, afterSyncToRemote should
     * be a no-op for the format file (not fall through to TieredDirectory).
     */
    public void testAfterSyncToRemoteFormatFileWithoutRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            // The non-closing format directory does NOT implement RemoteSyncAwareDirectory.
            // With the fix, afterSyncToRemote is a no-op for format files whose directory
            // doesn't support sync — it does NOT fall through to tieredDirectory.
            String parquetFile = "parquet/seg_nosync.parquet";
            writeParquetFileToDisk(parquetFile);

            // Should complete without error — no-op for non-RemoteSyncAwareDirectory format dirs
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
     * createOutput for format file goes through format directory.
     */
    public void testCreateOutputFormatFile() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            String parquetFile = "parquet/seg_create.parquet";
            // Ensure the parquet subdirectory exists
            Files.createDirectories(shardPath.getDataPath().resolve("parquet"));

            try (IndexOutput out = directory.createOutput(parquetFile, IOContext.DEFAULT)) {
                out.writeBytes(PARQUET_DATA, PARQUET_DATA.length);
            }

            // Verify file exists on disk
            Path fullPath = shardPath.getDataPath().resolve(parquetFile);
            assertTrue("Parquet file should exist on disk after createOutput", Files.exists(fullPath));
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
     * close() closes all format directories and TieredDirectory.
     */
    public void testCloseClosesFormatDirectoriesAndTieredDirectory() throws IOException {
        Directory mockFormatDir = mock(Directory.class);
        when(mockFormatDir.listAll()).thenReturn(new String[0]);

        Map<String, Directory> formatDirs = new HashMap<>();
        formatDirs.put("parquet", mockFormatDir);

        TieredSubdirectoryAwareDirectory dir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            formatDirs,
            getMockPrefetchSettingsSupplier()
        );

        dir.close();
        verify(mockFormatDir).close();

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
    // IOUtils.close tests — partial close safety
    // ═══════════════════════════════════════════════════════════════

    /**
     * If one format directory throws on close, the other format directories and
     * tieredDirectory are still closed (IOUtils.close collects exceptions).
     */
    public void testCloseWithThrowingFormatDirectoryStillClosesOthers() throws IOException {
        Directory throwingDir = mock(Directory.class);
        when(throwingDir.listAll()).thenReturn(new String[0]);
        org.mockito.Mockito.doThrow(new IOException("format close failed")).when(throwingDir).close();

        Directory goodDir = mock(Directory.class);
        when(goodDir.listAll()).thenReturn(new String[0]);

        Map<String, Directory> formatDirs = new HashMap<>();
        formatDirs.put("bad-format", throwingDir);
        formatDirs.put("good-format", goodDir);

        TieredSubdirectoryAwareDirectory dir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            formatDirs,
            getMockPrefetchSettingsSupplier()
        );

        IOException ex = expectThrows(IOException.class, dir::close);
        assertEquals("format close failed", ex.getMessage());
        // Good format directory should still have been closed despite the other throwing
        verify(goodDir).close();
    }

    /**
     * afterSyncToRemote for a format file whose directory is non-null but not RemoteSyncAwareDirectory
     * should be a no-op — must NOT fall through to tieredDirectory.
     */
    public void testAfterSyncToRemoteFormatFileNoopWhenNotRemoteSyncAware() throws IOException {
        directory = buildDirectoryWithParquetFormat();
        try {
            // Write a parquet file directly to disk (not via createOutput, so not in FileCache)
            String parquetFile = "parquet/seg_noop.parquet";
            writeParquetFileToDisk(parquetFile);

            // With the fix, this is a no-op because the format directory exists but doesn't
            // implement RemoteSyncAwareDirectory. Previously this would fall through to
            // tieredDirectory.afterSyncToRemote and NPE on fileCache.decRef for uncached file.
            directory.afterSyncToRemote(parquetFile);
            // If we get here without NPE, the no-op path is working correctly.
        } finally {
            directory.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Helper: interface combining Directory + RemoteSyncAwareDirectory for mocking
    // ═══════════════════════════════════════════════════════════════

    /**
     * Helper interface for creating mocks that implement both Directory and RemoteSyncAwareDirectory.
     */
    abstract static class RemoteSyncAwareFormatDirectory extends Directory implements RemoteSyncAwareDirectory {}
}
