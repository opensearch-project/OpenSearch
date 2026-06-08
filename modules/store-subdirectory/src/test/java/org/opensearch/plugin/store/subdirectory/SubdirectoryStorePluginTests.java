/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.subdirectory;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.StoreStats;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SubdirectoryStorePluginTests extends OpenSearchTestCase {

    public void testPluginInstantiation() {
        SubdirectoryStorePlugin plugin = new SubdirectoryStorePlugin();
        assertNotNull(plugin);
    }

    public void testGetStoreFactories() {
        SubdirectoryStorePlugin plugin = new SubdirectoryStorePlugin();
        Map<String, IndexStorePlugin.StoreFactory> factories = plugin.getStoreFactories();

        assertNotNull(factories);
        assertTrue(factories.containsKey("subdirectory_store"));

        IndexStorePlugin.StoreFactory factory = factories.get("subdirectory_store");
        assertNotNull(factory);
        assertTrue(factory instanceof SubdirectoryStorePlugin.SubdirectoryStoreFactory);
    }

    public void testStats() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0))
            .build();
        Path path = createTempDir().resolve("indices").resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        SubdirectoryAwareStore store = new SubdirectoryAwareStore(
            shardId,
            IndexSettingsModule.newIndexSettings("index", settings),
            SubdirectoryStorePluginTests.newFSDirectory(path.resolve("index")),
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            new ShardPath(false, path, path, shardId),
            new FsDirectoryFactory()
        );

        long initialStoreSize = 0;
        for (String extraFiles : store.directory().listAll()) {
            assertTrue("expected extraFS file but got: " + extraFiles, extraFiles.startsWith("extra"));
            initialStoreSize += store.directory().fileLength(extraFiles);
        }

        final long reservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        StoreStats stats = store.stats(reservedBytes);
        assertEquals(initialStoreSize, stats.getSize().getBytes());
        assertEquals(reservedBytes, stats.getReservedSize().getBytes());

        stats.add(null);
        assertEquals(initialStoreSize, stats.getSize().getBytes());
        assertEquals(reservedBytes, stats.getReservedSize().getBytes());

        final long otherStatsBytes = randomLongBetween(0L, Integer.MAX_VALUE);
        final long otherStatsReservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        stats.add(new StoreStats.Builder().sizeInBytes(otherStatsBytes).reservedSize(otherStatsReservedBytes).build());
        assertEquals(initialStoreSize + otherStatsBytes, stats.getSize().getBytes());
        assertEquals(Math.max(reservedBytes, 0L) + Math.max(otherStatsReservedBytes, 0L), stats.getReservedSize().getBytes());

        Directory dir = store.directory();
        final long length;
        try (IndexOutput output = dir.createOutput("foo/bar", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            length = output.getFilePointer();
        }

        assertTrue(numNonExtraFiles(store) > 0);
        stats = store.stats(0L);
        assertEquals(stats.getSizeInBytes(), length + initialStoreSize);

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public static void deleteContent(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        final List<IOException> exceptions = new ArrayList<>();
        for (String file : files) {
            try {
                directory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public int numNonExtraFiles(Store store) throws IOException {
        int numNonExtra = 0;
        for (String file : store.directory().listAll()) {
            if (file.startsWith("extra") == false) {
                numNonExtra++;
            }
        }
        return numNonExtra;
    }

    /**
     * Helper class to hold test store setup data
     */
    private static class TestStoreSetup {
        final SubdirectoryAwareStore store;
        final Path path;

        TestStoreSetup(SubdirectoryAwareStore store, Path path) {
            this.store = store;
            this.path = path;
        }
    }

    /**
     * Creates a test store setup with all necessary paths and configurations
     */
    private TestStoreSetup createTestStoreSetup() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0))
            .build();
        Path path = createTempDir().resolve("indices").resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        Path indexPath = path.resolve("index");
        Files.createDirectories(indexPath);

        SubdirectoryAwareStore store = new SubdirectoryAwareStore(
            shardId,
            IndexSettingsModule.newIndexSettings("index", settings),
            SubdirectoryStorePluginTests.newFSDirectory(indexPath),
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            new ShardPath(false, path, path, shardId),
            new FsDirectoryFactory()
        );

        return new TestStoreSetup(store, path);
    }

    public void testLoadMetadataWithNonSegmentFiles() throws IOException {
        TestStoreSetup setup = createTestStoreSetup();
        SubdirectoryAwareStore store = setup.store;

        try {
            Path subdirPath = setup.path.resolve("subdir");
            Files.createDirectories(subdirPath);

            // Write non-segment files with codec headers and footers
            try (Directory subdir = FSDirectory.open(subdirPath)) {
                writeTestFileWithCodec(subdir, "metadata_file1.dat", "test_data_1");
                writeTestFileWithCodec(subdir, "metadata_file2.dat", "test_data_2");
            }

            // Create a minimal segments file in the main directory
            try (IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig())) {
                writer.commit();
            }

            // Get the committed SegmentInfos from the directory
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(store.directory());

            // Create mock IndexCommit that includes subdirectory files
            TestIndexCommit testCommit = new TestIndexCommit(segmentInfos, store.directory(), subdirPath, "subdir");

            // Load metadata
            Store.MetadataSnapshot metadata = store.getMetadata(testCommit);

            // Verify non-segment files are included in metadata
            assertTrue("Metadata should contain subdir/metadata_file1.dat", metadata.contains("subdir/metadata_file1.dat"));
            assertTrue("Metadata should contain subdir/metadata_file2.dat", metadata.contains("subdir/metadata_file2.dat"));

            // Verify file metadata properties
            StoreFileMetadata file1Meta = metadata.get("subdir/metadata_file1.dat");
            assertNotNull("File metadata should not be null", file1Meta);
            assertTrue("File length should be greater than 0", file1Meta.length() > 0);
            assertNotNull("Checksum should not be null", file1Meta.checksum());
            assertNotNull("Version should not be null", file1Meta.writtenBy());

            StoreFileMetadata file2Meta = metadata.get("subdir/metadata_file2.dat");
            assertNotNull("File metadata should not be null", file2Meta);
            assertTrue("File length should be greater than 0", file2Meta.length() > 0);
            assertNotNull("Checksum should not be null", file2Meta.checksum());

            // Verify different files have different checksums
            assertFalse("Different files should have different checksums", file1Meta.checksum().equals(file2Meta.checksum()));
        } finally {
            deleteContent(store.directory());
            IOUtils.close(store);
        }
    }

    public void testLoadMetadataWithMixedFiles() throws IOException {
        TestStoreSetup setup = createTestStoreSetup();
        SubdirectoryAwareStore store = setup.store;

        try {
            Path subdirPath = setup.path.resolve("subdir");
            Files.createDirectories(subdirPath);

            // Write segment files
            try (Directory subdir = FSDirectory.open(subdirPath)) {
                try (IndexWriter writer = new IndexWriter(subdir, new IndexWriterConfig())) {
                    writer.commit();
                }
                // Write non-segment files
                writeTestFileWithCodec(subdir, "custom_metadata.dat", "custom_data");
            }

            // Create a minimal segments file in main directory
            try (IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig())) {
                writer.commit();
            }

            // Get the committed SegmentInfos from the directory
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(store.directory());

            // Create mock IndexCommit that includes subdirectory files
            TestIndexCommit testCommit = new TestIndexCommit(segmentInfos, store.directory(), subdirPath, "subdir");

            // Load metadata
            Store.MetadataSnapshot metadata = store.getMetadata(testCommit);

            // Verify segment files are included
            boolean hasSegmentFile = false;
            for (String fileName : metadata.asMap().keySet()) {
                if (fileName.startsWith("subdir/segments_")) {
                    hasSegmentFile = true;
                    break;
                }
            }
            assertTrue("Should have segment file in subdirectory", hasSegmentFile);

            // Verify non-segment file is included
            assertTrue("Metadata should contain custom_metadata.dat", metadata.contains("subdir/custom_metadata.dat"));

            // Verify no duplicate entries
            Collection<String> fileNames = metadata.asMap().keySet();
            assertEquals("Should have no duplicate entries", fileNames.size(), fileNames.stream().distinct().count());
        } finally {
            deleteContent(store.directory());
            IOUtils.close(store);
        }
    }

    public void testNonSegmentFileChecksumValidation() throws IOException {
        TestStoreSetup setup = createTestStoreSetup();
        SubdirectoryAwareStore store = setup.store;

        try {
            Path subdirPath = setup.path.resolve("subdir");
            Files.createDirectories(subdirPath);

            // Write test file with codec headers and footers
            String testContent = "test_content_for_checksum";
            try (Directory subdir = FSDirectory.open(subdirPath)) {
                writeTestFileWithCodec(subdir, "checksum_test.dat", testContent);

                // Compute expected checksum directly
                try (IndexInput in = subdir.openInput("checksum_test.dat", IOContext.READONCE)) {
                    String expectedChecksum = Store.digestToString(CodecUtil.checksumEntireFile(in));

                    // Create main segments file
                    try (IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig())) {
                        writer.commit();
                    }

                    // Get the committed SegmentInfos from the directory
                    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(store.directory());

                    // Create mock IndexCommit
                    TestIndexCommit testCommit = new TestIndexCommit(segmentInfos, store.directory(), subdirPath, "subdir");

                    // Load metadata
                    Store.MetadataSnapshot metadata = store.getMetadata(testCommit);

                    // Verify checksum matches
                    StoreFileMetadata fileMeta = metadata.get("subdir/checksum_test.dat");
                    assertNotNull("File metadata should exist", fileMeta);
                    assertEquals("Checksum should match CodecUtil.checksumEntireFile result", expectedChecksum, fileMeta.checksum());
                }

                // Write another file with different content
                writeTestFileWithCodec(subdir, "checksum_test2.dat", "different_content");

                // Get the committed SegmentInfos from the directory
                SegmentInfos segmentInfos2 = SegmentInfos.readLatestCommit(store.directory());

                // Verify different content produces different checksum
                TestIndexCommit testCommit2 = new TestIndexCommit(segmentInfos2, store.directory(), subdirPath, "subdir");
                Store.MetadataSnapshot metadata2 = store.getMetadata(testCommit2);

                String checksum1 = metadata2.get("subdir/checksum_test.dat").checksum();
                String checksum2 = metadata2.get("subdir/checksum_test2.dat").checksum();
                assertFalse("Different content should produce different checksums", checksum1.equals(checksum2));
            }
        } finally {
            deleteContent(store.directory());
            IOUtils.close(store);
        }
    }

    /**
     * Helper method to write a test file with proper Lucene codec header and footer
     */
    private void writeTestFileWithCodec(Directory dir, String fileName, String content) throws IOException {
        try (IndexOutput output = dir.createOutput(fileName, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, "test_codec", 0);
            output.writeString(content);
            output.writeLong(System.currentTimeMillis());
            CodecUtil.writeFooter(output);
        }
    }

    /**
     * Mock IndexCommit that wraps SegmentInfos and includes files from subdirectories
     */
    static class TestIndexCommit extends IndexCommit {
        private final SegmentInfos segmentInfos;
        private final Directory directory;
        private final List<Path> subdirectoryPaths;
        private final List<String> subdirectoryNames;

        TestIndexCommit(SegmentInfos segmentInfos, Directory directory, Path subdirectoryPath, String subdirectoryName) {
            this.segmentInfos = segmentInfos;
            this.directory = directory;
            this.subdirectoryPaths = List.of(subdirectoryPath);
            this.subdirectoryNames = List.of(subdirectoryName);
        }

        @Override
        public String getSegmentsFileName() {
            return segmentInfos.getSegmentsFileName();
        }

        @Override
        public Collection<String> getFileNames() throws IOException {
            Collection<String> allFiles = new ArrayList<>(segmentInfos.files(true));

            for (int i = 0; i < subdirectoryPaths.size(); i++) {
                Path subdirPath = subdirectoryPaths.get(i);
                String subdirName = subdirectoryNames.get(i);

                if (Files.exists(subdirPath)) {
                    try (Directory directory = FSDirectory.open(subdirPath)) {
                        String[] files = directory.listAll();

                        // Add segment files if they exist
                        try {
                            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
                            for (String fileName : segmentInfos.files(true)) {
                                allFiles.add(subdirName + "/" + fileName);
                            }
                        } catch (Exception e) {
                            // No segment files, that's OK
                        }

                        // Add non-segment files (exclude test infrastructure and Lucene internal files)
                        for (String fileName : files) {
                            if (isCustomMetadataFile(fileName)) {
                                allFiles.add(subdirName + "/" + fileName);
                            }
                        }
                    }
                }
            }

            return allFiles;
        }

        @Override
        public Directory getDirectory() {
            return directory;
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("TestIndexCommit does not support deletion");
        }

        @Override
        public int getSegmentCount() {
            return segmentInfos.size();
        }

        @Override
        public long getGeneration() {
            return segmentInfos.getGeneration();
        }

        @Override
        public Map<String, String> getUserData() throws IOException {
            return segmentInfos.getUserData();
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        /**
         * Returns true if the file is a custom metadata file (not a Lucene internal file or test infrastructure file)
         */
        private static boolean isCustomMetadataFile(String fileName) {
            return !fileName.startsWith("segments_")
                && !fileName.endsWith(".si")
                && !fileName.startsWith("extra")
                && !fileName.endsWith(".cfe")
                && !fileName.endsWith(".cfs")
                && !fileName.endsWith(".lock");
        }
    }
}
