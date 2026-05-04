/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

public class DataFormatAwareStoreDirectoryTests extends OpenSearchTestCase {

    private Path tempDir;
    private Path shardDataPath;
    private Path indexPath;
    private FSDirectory fsDirectory;
    private ShardPath shardPath;
    private DataFormatAwareStoreDirectory dataFormatAwareStoreDirectory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create directory structure: tempDir/<indexUUID>/<shardId>/index/
        tempDir = createTempDir();
        String indexUUID = "test-index-uuid";
        int shardId = 0;
        shardDataPath = tempDir.resolve(indexUUID).resolve(Integer.toString(shardId));
        indexPath = shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createDirectories(indexPath);

        fsDirectory = FSDirectory.open(indexPath);
        ShardId sid = new ShardId(new Index("test-index", indexUUID), shardId);
        shardPath = new ShardPath(false, shardDataPath, shardDataPath, sid);

        dataFormatAwareStoreDirectory = new DataFormatAwareStoreDirectory(fsDirectory, shardPath, Map.of());
    }

    @After
    public void tearDown() throws Exception {
        if (dataFormatAwareStoreDirectory != null) {
            dataFormatAwareStoreDirectory.close();
        }
        super.tearDown();
    }

    // ═══════════════════════════════════════════════════════════════
    // toFileMetadata / toFileIdentifier
    // ═══════════════════════════════════════════════════════════════

    public void testToFileMetadata_luceneFile() {
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    public void testToFileMetadata_prefixedFile() {
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("parquet/data.parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("data.parquet", fm.file());
    }

    public void testToFileMetadata_arrowFile() {
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("arrow/data.arrow");
        assertEquals("arrow", fm.dataFormat());
        assertEquals("data.arrow", fm.file());
    }

    public void testToFileIdentifier_lucene() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("_0.si", identifier);
    }

    public void testToFileIdentifier_metadata() {
        // "metadata" is treated as a default/index format, so no prefix is added
        FileMetadata fm = new FileMetadata("metadata", "meta_file.txt");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("meta_file.txt", identifier);
    }

    public void testToFileIdentifier_nonLucene() {
        FileMetadata fm = new FileMetadata("parquet", "data.parquet");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("parquet/data.parquet", identifier);
    }

    public void testToFileIdentifier_arrow() {
        FileMetadata fm = new FileMetadata("arrow", "data.arrow");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("arrow/data.arrow", identifier);
    }

    public void testRoundtrip_toFileMetadata_toFileIdentifier_lucene() {
        String original = "_0.cfe";
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata(original);
        String result = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals(original, result);
    }

    public void testRoundtrip_toFileMetadata_toFileIdentifier_nonLucene() {
        String original = "parquet/data.parquet";
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata(original);
        String result = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals(original, result);
    }

    // ═══════════════════════════════════════════════════════════════
    // getDataFormat
    // ═══════════════════════════════════════════════════════════════

    public void testGetDataFormat_lucene() {
        assertEquals("lucene", dataFormatAwareStoreDirectory.getDataFormat("_0.cfe"));
    }

    public void testGetDataFormat_nonLucene() {
        assertEquals("arrow", dataFormatAwareStoreDirectory.getDataFormat("arrow/data.arrow"));
    }

    public void testGetDataFormat_parquet() {
        assertEquals("parquet", dataFormatAwareStoreDirectory.getDataFormat("parquet/data.parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput / openInput - Lucene files
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputAndOpenInput_lucene() throws IOException {
        String fileName = "_0_test.si";
        byte[] testData = "hello world lucene".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeBytes(testData, testData.length);
        }

        try (IndexInput in = dataFormatAwareStoreDirectory.openInput(fileName, IOContext.DEFAULT)) {
            byte[] readData = new byte[testData.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(testData, readData);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput / openInput - Non-Lucene files
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputAndOpenInput_nonLucene() throws IOException {
        String fileIdentifier = "parquet/data.parquet";
        byte[] testData = "hello world parquet".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(testData, testData.length);
        }

        try (IndexInput in = dataFormatAwareStoreDirectory.openInput(fileIdentifier, IOContext.DEFAULT)) {
            byte[] readData = new byte[testData.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(testData, readData);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_lucene() throws IOException {
        String fileName = "_test_len.si";
        byte[] data = "some content for length test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        assertEquals(data.length, dataFormatAwareStoreDirectory.fileLength(fileName));
    }

    public void testFileLength_fileMetadata() throws IOException {
        String fileIdentifier = "parquet/len_test.parquet";
        byte[] data = "parquet length test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        FileMetadata fm = new FileMetadata("parquet", "len_test.parquet");
        assertEquals(data.length, dataFormatAwareStoreDirectory.fileLength(fm.serialize()));
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_string() throws IOException {
        String fileName = "_del_test.si";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeString("to be deleted");
        }
        assertTrue(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(fileName));

        dataFormatAwareStoreDirectory.deleteFile(fileName);
        assertFalse(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(fileName));
    }

    public void testDeleteFile_fileMetadata() throws IOException {
        String fileIdentifier = "parquet/del_test.parquet";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeString("to be deleted");
        }
        String serialized = new FileMetadata("parquet", "del_test.parquet").serialize();
        assertTrue(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(serialized));

        FileMetadata fm = new FileMetadata("parquet", "del_test.parquet");
        dataFormatAwareStoreDirectory.deleteFile(fm.serialize());
        assertFalse(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(serialized));
    }

    // ═══════════════════════════════════════════════════════════════
    // listAll / listFileMetadata
    // ═══════════════════════════════════════════════════════════════

    public void testListAll_empty() throws IOException {
        // A fresh directory should list no segment files
        String[] files = dataFormatAwareStoreDirectory.listAll();
        assertNotNull(files);
    }

    public void testListAll_withFiles() throws IOException {
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("data");
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/data.parquet", IOContext.DEFAULT)) {
            out.writeString("parquet data");
        }

        String[] files = dataFormatAwareStoreDirectory.listAll();
        List<String> fileList = Arrays.asList(files);
        assertTrue(fileList.contains("_0.si"));
        assertTrue(fileList.contains("parquet/data.parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Checksum - Lucene file (CodecUtil path)
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_luceneFile() throws IOException {
        String fileName = "_cksum.si";
        // Write a file with Lucene CodecUtil header/footer so CodecUtil.retrieveChecksum works
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, "TestCodec", 1);
            out.writeString("some segment data for checksum");
            CodecUtil.writeFooter(out);
        }

        long checksum = dataFormatAwareStoreDirectory.calculateChecksum(fileName);
        // Verify it's a valid non-zero checksum (CodecUtil stores checksum in footer)
        assertTrue("Checksum should be a valid value", checksum != 0);

        // Verify we get the same checksum via string overload with serialized name
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata(fileName);
        assertEquals(checksum, dataFormatAwareStoreDirectory.calculateChecksum(fm.serialize()));
    }

    // ═══════════════════════════════════════════════════════════════
    // Checksum - Non-Lucene file (CRC32 path)
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_nonLuceneFile() throws IOException {
        String fileIdentifier = "parquet/cksum.parquet";
        byte[] data = "parquet content for checksum".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksum = dataFormatAwareStoreDirectory.calculateChecksum(fileIdentifier);

        // Compute expected CRC32 manually
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        assertEquals("CRC32 checksum should match", crc32.getValue(), checksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // calculateUploadChecksum
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateUploadChecksum() throws IOException {
        String fileIdentifier = "parquet/upload_cksum.parquet";
        byte[] data = "upload checksum test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        FileMetadata fm = new FileMetadata("parquet", "upload_cksum.parquet");
        String uploadChecksum = dataFormatAwareStoreDirectory.calculateUploadChecksum(fm.serialize());
        assertNotNull(uploadChecksum);
        // Should be the string representation of the long checksum
        long parsedChecksum = Long.parseLong(uploadChecksum);
        assertEquals(dataFormatAwareStoreDirectory.calculateChecksum(fm.serialize()), parsedChecksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // rename
    // ═══════════════════════════════════════════════════════════════

    public void testRename_sameFormat() throws IOException {
        String fileName = "_rename_src.si";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeString("rename test data");
        }

        dataFormatAwareStoreDirectory.rename(fileName, "_rename_dest.si");
        assertFalse(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(fileName));
        assertTrue(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains("_rename_dest.si"));
    }

    public void testRename_fileMetadata_sameFormat() throws IOException {
        String fileIdentifier = "parquet/rename_src.parquet";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeString("rename test data parquet");
        }

        FileMetadata src = new FileMetadata("parquet", "rename_src.parquet");
        FileMetadata dest = new FileMetadata("parquet", "rename_dest.parquet");
        dataFormatAwareStoreDirectory.rename(src.serialize(), dest.serialize());

        String srcSerialized = new FileMetadata("parquet", "rename_src.parquet").serialize();
        String destSerialized = new FileMetadata("parquet", "rename_dest.parquet").serialize();
        assertFalse(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(srcSerialized));
        assertTrue(Arrays.asList(dataFormatAwareStoreDirectory.listAll()).contains(destSerialized));
    }

    // ═══════════════════════════════════════════════════════════════
    // sync
    // ═══════════════════════════════════════════════════════════════

    public void testSync() throws IOException {
        String fileName = "_sync_test.si";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeString("sync test data");
        }

        // sync should not throw
        dataFormatAwareStoreDirectory.sync(Set.of(fileName));
    }

    public void testGetShardPath() {
        assertNotNull(dataFormatAwareStoreDirectory.getShardPath());
        assertEquals(shardPath, dataFormatAwareStoreDirectory.getShardPath());
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata convenience: getChecksumOfLocalFile
    // ═══════════════════════════════════════════════════════════════

    public void testGetChecksumOfLocalFile() throws IOException {
        String fileIdentifier = "parquet/local_cksum.parquet";
        byte[] data = "local checksum test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        FileMetadata fm = new FileMetadata("parquet", "local_cksum.parquet");
        long checksum = dataFormatAwareStoreDirectory.calculateChecksum(fm.serialize());
        assertTrue(checksum != 0);
    }

    // ═══════════════════════════════════════════════════════════════
    // resolveFileName with FileMetadata.DELIMITER in name
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_withSerializedFileMetadata() throws IOException {
        // Write a file using plain identifier
        String fileIdentifier = "parquet/delimited_test.parquet";
        byte[] data = "delimited test data".getBytes(StandardCharsets.UTF_8);
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        // Now access it using a serialized FileMetadata string (with "/" delimiter)
        FileMetadata fm = new FileMetadata("parquet", "delimited_test.parquet");
        String serialized = fm.serialize(); // "parquet/delimited_test.parquet"

        // The resolveFileName method should handle the delimiter and resolve correctly
        long length = dataFormatAwareStoreDirectory.fileLength(serialized);
        assertEquals(data.length, length);
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-based openInput / createOutput
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputAndOpenInput_fileMetadata_lucene() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_fm_test.si");
        byte[] testData = "file metadata lucene test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fm, IOContext.DEFAULT)) {
            out.writeBytes(testData, testData.length);
        }

        try (IndexInput in = dataFormatAwareStoreDirectory.openInput(fm.serialize(), IOContext.DEFAULT)) {
            byte[] readData = new byte[testData.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(testData, readData);
        }
    }

    public void testCreateOutputAndOpenInput_fileMetadata_parquet() throws IOException {
        FileMetadata fm = new FileMetadata("parquet", "fm_test.parquet");
        byte[] testData = "file metadata parquet test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fm, IOContext.DEFAULT)) {
            out.writeBytes(testData, testData.length);
        }

        try (IndexInput in = dataFormatAwareStoreDirectory.openInput(fm.serialize(), IOContext.DEFAULT)) {
            byte[] readData = new byte[testData.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(testData, readData);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Checksum idempotency
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_idempotent_nonLucene() throws IOException {
        String fileIdentifier = "parquet/idempotent.parquet";
        byte[] data = "idempotent checksum data".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksum1 = dataFormatAwareStoreDirectory.calculateChecksum(fileIdentifier);
        long checksum2 = dataFormatAwareStoreDirectory.calculateChecksum(fileIdentifier);
        assertEquals("Checksum should be the same on repeated calls", checksum1, checksum2);
    }

    public void testCalculateChecksum_stringAndFileMetadataConsistent() throws IOException {
        String fileIdentifier = "parquet/consistency.parquet";
        byte[] data = "consistency checksum".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksumFromString = dataFormatAwareStoreDirectory.calculateChecksum(fileIdentifier);
        assertEquals(checksumFromString, dataFormatAwareStoreDirectory.calculateChecksum("parquet/consistency.parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Data integrity after rename
    // ═══════════════════════════════════════════════════════════════

    public void testRename_preservesContent_lucene() throws IOException {
        String srcFile = "_rename_content.si";
        byte[] data = "content to preserve after rename".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(srcFile, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        dataFormatAwareStoreDirectory.rename(srcFile, "_rename_content_dest.si");

        try (IndexInput in = dataFormatAwareStoreDirectory.openInput("_rename_content_dest.si", IOContext.DEFAULT)) {
            byte[] readData = new byte[data.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(data, readData);
        }
    }

    public void testRename_preservesContent_nonLucene() throws IOException {
        FileMetadata src = new FileMetadata("parquet", "rename_content.parquet");
        FileMetadata dest = new FileMetadata("parquet", "rename_content_dest.parquet");
        byte[] data = "parquet content to preserve".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(src, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        dataFormatAwareStoreDirectory.rename(src.serialize(), dest.serialize());

        try (IndexInput in = dataFormatAwareStoreDirectory.openInput(dest.serialize(), IOContext.DEFAULT)) {
            byte[] readData = new byte[data.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(data, readData);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Multiple formats in listAll
    // ═══════════════════════════════════════════════════════════════

    public void testListAll_multipleFormats() throws IOException {
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("lucene");
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/data.parquet", IOContext.DEFAULT)) {
            out.writeString("parquet");
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("arrow/data.arrow", IOContext.DEFAULT)) {
            out.writeString("arrow");
        }

        String[] files = dataFormatAwareStoreDirectory.listAll();
        List<String> fileList = Arrays.asList(files);
        assertTrue("Should contain lucene file", fileList.contains("_0.si"));
        assertTrue("Should contain parquet file", fileList.contains("parquet/data.parquet"));
        assertTrue("Should contain arrow file", fileList.contains("arrow/data.arrow"));
    }

    // ═══════════════════════════════════════════════════════════════
    // sync with non-Lucene and multiple files
    // ═══════════════════════════════════════════════════════════════

    public void testSync_nonLuceneFile() throws IOException {
        String fileIdentifier = "parquet/sync_test.parquet";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeString("sync test parquet");
        }

        // Should not throw
        dataFormatAwareStoreDirectory.sync(Set.of(fileIdentifier));
    }

    public void testSync_multipleFiles() throws IOException {
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("_sync1.si", IOContext.DEFAULT)) {
            out.writeString("sync1");
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/sync2.parquet", IOContext.DEFAULT)) {
            out.writeString("sync2");
        }

        // Should not throw with multiple files
        dataFormatAwareStoreDirectory.sync(Set.of("_sync1.si", "parquet/sync2.parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile with non-existent file
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_nonExistent_throws() {
        expectThrows(IOException.class, () -> dataFormatAwareStoreDirectory.deleteFile("_nonexistent.si"));
    }

    public void testDeleteFile_fileMetadata_nonExistent_throws() {
        FileMetadata fm = new FileMetadata("parquet", "nonexistent.parquet");
        expectThrows(IOException.class, () -> dataFormatAwareStoreDirectory.deleteFile(fm.serialize()));
    }

    // ═══════════════════════════════════════════════════════════════
    // toFileMetadata edge cases
    // ═══════════════════════════════════════════════════════════════

    public void testToFileMetadata_nestedPath() {
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("custom/nested_file.data");
        assertEquals("custom", fm.dataFormat());
        assertEquals("nested_file.data", fm.file());
    }

    public void testToFileIdentifier_nullFormatTreatedAsDefault() {
        FileMetadata fm = new FileMetadata(null, "_0.si");
        // null format should be treated as default (no prefix)
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("_0.si", identifier);
    }

    public void testToFileIdentifier_emptyFormatTreatedAsDefault() {
        FileMetadata fm = new FileMetadata("", "_0.si");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("_0.si", identifier);
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength with serialized FileMetadata string
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_withSerializedString_lucene() throws IOException {
        String fileName = "_len_serial.si";
        byte[] data = "serialized length test".getBytes(StandardCharsets.UTF_8);
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        // Access via serialized form
        String serialized = new FileMetadata("lucene", fileName).serialize();
        assertEquals(data.length, dataFormatAwareStoreDirectory.fileLength(serialized));
    }

    // ═══════════════════════════════════════════════════════════════
    // Empty file checksum
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_emptyNonLuceneFile() throws IOException {
        String fileIdentifier = "parquet/empty.parquet";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            // Write nothing - empty file
        }

        long checksum = dataFormatAwareStoreDirectory.calculateChecksum(fileIdentifier);

        CRC32 crc32 = new CRC32();
        // CRC32 of empty data
        assertEquals("CRC32 of empty file should match", crc32.getValue(), checksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // calculateUploadChecksum for lucene file
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateUploadChecksum_lucene() throws IOException {
        String fileName = "_upload_cksum_lucene.si";
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, "UploadTest", 1);
            out.writeString("upload checksum lucene data");
            CodecUtil.writeFooter(out);
        }

        FileMetadata fm = new FileMetadata("lucene", fileName);
        String uploadChecksum = dataFormatAwareStoreDirectory.calculateUploadChecksum(fm.serialize());
        assertNotNull(uploadChecksum);
        long parsedChecksum = Long.parseLong(uploadChecksum);
        assertEquals(dataFormatAwareStoreDirectory.calculateChecksum(fm.serialize()), parsedChecksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata → "/" identifier → SubdirectoryAwareDirectory
    // Path mapping & storage location tests
    // ═══════════════════════════════════════════════════════════════

    // --- Lucene files: no prefix, stored in <shard>/index/ ---

    public void testPathMapping_luceneFile_storedInIndexDir() throws IOException {
        // Lucene files (no slash) should be stored in <shard>/index/<filename>
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("_0.cfs", identifier); // no prefix
        assertFalse("Lucene identifier should not contain '/'", identifier.contains("/"));

        // Write and verify it's accessible
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("lucene data");
        }
        // Verify physical file exists in index directory
        assertTrue(Files.exists(indexPath.resolve("_0.cfs")));
    }

    public void testPathMapping_segmentsFile_storedInIndexDir() throws IOException {
        // segments_N files should be treated as lucene format (default)
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("segments_1");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("segments_1", fm.file());

        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("segments_1", identifier); // no prefix

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("segments data");
        }
        assertTrue("segments file should be in index dir", Files.exists(indexPath.resolve("segments_1")));
    }

    public void testPathMapping_segmentInfoFile_storedInIndexDir() throws IOException {
        // _0.si (segment info) is a lucene file
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("_0.si");
        assertEquals("lucene", fm.dataFormat());
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("_0.si", identifier);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("si data");
        }
        assertTrue(Files.exists(indexPath.resolve("_0.si")));
    }

    // --- Metadata files: treated as default format, stored in <shard>/index/ ---

    public void testPathMapping_metadataFormat_storedInIndexDir() throws IOException {
        // "metadata" is in INDEX_DIRECTORY_FORMATS, so no prefix is added
        FileMetadata fm = new FileMetadata("metadata", "metadata__1__5__abc");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("metadata__1__5__abc", identifier); // no prefix
        assertFalse("Metadata identifier should not contain '/'", identifier.contains("/"));

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("metadata content");
        }
        assertTrue("Metadata file should be in index dir", Files.exists(indexPath.resolve("metadata__1__5__abc")));
    }

    public void testPathMapping_metadataFormat_getDataFormat() {
        // A plain metadata filename (no slash) starting with "metadata" is treated as metadata format
        assertEquals("metadata", dataFormatAwareStoreDirectory.getDataFormat("metadata__1__2__3"));
    }

    // --- Parquet files: "parquet/" prefix, stored in <shard>/parquet/ ---

    public void testPathMapping_parquetFile_storedInSubdir() throws IOException {
        FileMetadata fm = new FileMetadata("parquet", "_0_1.parquet");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("parquet/_0_1.parquet", identifier);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("parquet data");
        }
        assertTrue("Parquet file should be in parquet subdir", Files.exists(shardDataPath.resolve("parquet").resolve("_0_1.parquet")));
        assertFalse("Parquet file should NOT be in index dir", Files.exists(indexPath.resolve("_0_1.parquet")));
    }

    public void testPathMapping_parquetFile_fromIdentifier() {
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata("parquet/_0_1.parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("_0_1.parquet", fm.file());
    }

    // --- Arrow files: "arrow/" prefix, stored in <shard>/arrow/ ---

    public void testPathMapping_arrowFile_storedInSubdir() throws IOException {
        FileMetadata fm = new FileMetadata("arrow", "data.arrow");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("arrow/data.arrow", identifier);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("arrow data");
        }
        assertTrue("Arrow file should be in arrow subdir", Files.exists(shardDataPath.resolve("arrow").resolve("data.arrow")));
        assertFalse("Arrow file should NOT be in index dir", Files.exists(indexPath.resolve("data.arrow")));
    }

    // --- Custom format: "custom/" prefix, stored in <shard>/custom/ ---

    public void testPathMapping_customFormat_storedInSubdir() throws IOException {
        FileMetadata fm = new FileMetadata("custom", "myfile.dat");
        String identifier = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("custom/myfile.dat", identifier);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(identifier, IOContext.DEFAULT)) {
            out.writeString("custom data");
        }
        assertTrue("Custom file should be in custom subdir", Files.exists(shardDataPath.resolve("custom").resolve("myfile.dat")));
    }

    // --- resolveFileName: serialized FileMetadata (with /) → native "/" identifier ---

    public void testResolveFileName_luceneSerialized() throws IOException {
        // Write using plain name
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("lucene data");
        }

        // Access using serialized FileMetadata — lucene serializes to plain name
        String serialized = new FileMetadata("lucene", "_0.si").serialize();
        long length = dataFormatAwareStoreDirectory.fileLength(serialized);
        assertTrue("Should resolve serialized lucene name", length > 0);
    }

    public void testResolveFileName_parquetSerialized() throws IOException {
        // Write using "/" identifier
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/data.parquet", IOContext.DEFAULT)) {
            out.writeString("parquet data");
        }

        // Access using serialized FileMetadata "parquet/data.parquet"
        String serialized = new FileMetadata("parquet", "data.parquet").serialize();
        long length = dataFormatAwareStoreDirectory.fileLength(serialized);
        assertTrue("Should resolve serialized parquet name", length > 0);
    }

    public void testResolveFileName_metadataSerialized() throws IOException {
        // Write using plain name (metadata is a default format)
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("metadata__1__2__3", IOContext.DEFAULT)) {
            out.writeString("metadata content");
        }

        // Access using serialized FileMetadata "metadata/metadata__1__2__3"
        String serialized = new FileMetadata("metadata", "metadata__1__2__3").serialize();
        long length = dataFormatAwareStoreDirectory.fileLength(serialized);
        assertTrue("Should resolve serialized metadata name", length > 0);
    }

    // --- End-to-end: Write via FileMetadata, read via string identifier and vice versa ---

    public void testEndToEnd_writeViaFileMetadata_readViaString_lucene() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_e2e_lucene.si");
        byte[] data = "e2e lucene test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fm, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        // Read using string identifier (no prefix for lucene)
        try (IndexInput in = dataFormatAwareStoreDirectory.openInput("_e2e_lucene.si", IOContext.DEFAULT)) {
            byte[] readData = new byte[data.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(data, readData);
        }
    }

    public void testEndToEnd_writeViaFileMetadata_readViaString_parquet() throws IOException {
        FileMetadata fm = new FileMetadata("parquet", "e2e_data.parquet");
        byte[] data = "e2e parquet test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput(fm, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        // Read using "/" string identifier
        try (IndexInput in = dataFormatAwareStoreDirectory.openInput("parquet/e2e_data.parquet", IOContext.DEFAULT)) {
            byte[] readData = new byte[data.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(data, readData);
        }
    }

    public void testEndToEnd_writeViaString_readViaFileMetadata_parquet() throws IOException {
        byte[] data = "reverse e2e".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/rev_e2e.parquet", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        // Read via FileMetadata
        FileMetadata fm = new FileMetadata("parquet", "rev_e2e.parquet");
        try (IndexInput in = dataFormatAwareStoreDirectory.openInput(fm.serialize(), IOContext.DEFAULT)) {
            byte[] readData = new byte[data.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(data, readData);
        }
    }

    // --- Physical path isolation: files of different formats don't collide ---

    public void testPathIsolation_sameFilenameInDifferentFormats() throws IOException {
        byte[] luceneData = "lucene version".getBytes(StandardCharsets.UTF_8);
        byte[] parquetData = "parquet version".getBytes(StandardCharsets.UTF_8);
        byte[] arrowData = "arrow version".getBytes(StandardCharsets.UTF_8);

        // Write "data.file" in three different formats
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("data.file", IOContext.DEFAULT)) {
            out.writeBytes(luceneData, luceneData.length);
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/data.file", IOContext.DEFAULT)) {
            out.writeBytes(parquetData, parquetData.length);
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("arrow/data.file", IOContext.DEFAULT)) {
            out.writeBytes(arrowData, arrowData.length);
        }

        // Verify different physical locations
        assertTrue(Files.exists(indexPath.resolve("data.file")));
        assertTrue(Files.exists(shardDataPath.resolve("parquet").resolve("data.file")));
        assertTrue(Files.exists(shardDataPath.resolve("arrow").resolve("data.file")));

        // Verify content is different (not overwritten)
        assertEquals(luceneData.length, dataFormatAwareStoreDirectory.fileLength("data.file"));
        assertEquals(parquetData.length, dataFormatAwareStoreDirectory.fileLength("parquet/data.file"));
        assertEquals(arrowData.length, dataFormatAwareStoreDirectory.fileLength("arrow/data.file"));
    }

    // --- Comprehensive toFileMetadata + toFileIdentifier round-trip for all formats ---

    public void testRoundtrip_allFormats() {
        // Lucene
        verifyRoundtrip("_0.cfs", "lucene", "_0.cfs");
        verifyRoundtrip("_0.si", "lucene", "_0.si");
        verifyRoundtrip("segments_1", "lucene", "segments_1");

        // Non-lucene formats
        verifyRoundtrip("parquet/_0_1.parquet", "parquet", "_0_1.parquet");
        verifyRoundtrip("arrow/data.arrow", "arrow", "data.arrow");
        verifyRoundtrip("custom/myfile.dat", "custom", "myfile.dat");
    }

    private void verifyRoundtrip(String identifier, String expectedFormat, String expectedFile) {
        FileMetadata fm = DataFormatAwareStoreDirectory.toFileMetadata(identifier);
        assertEquals("Format for " + identifier, expectedFormat, fm.dataFormat());
        assertEquals("File for " + identifier, expectedFile, fm.file());

        String roundtripped = DataFormatAwareStoreDirectory.toFileIdentifier(fm);
        assertEquals("Roundtrip for " + identifier, identifier, roundtripped);
    }

    // --- isDefaultFormat edge cases (should not add prefix) ---

    public void testToFileIdentifier_defaultFormats_noPrefix() {
        // "lucene" → no prefix
        assertEquals("file.si", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("lucene", "file.si")));
        // "LUCENE" (case-insensitive) → no prefix
        assertEquals("file.si", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("LUCENE", "file.si")));
        // "metadata" → no prefix
        assertEquals("meta.dat", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("metadata", "meta.dat")));
        // "METADATA" (case-insensitive) → no prefix
        assertEquals("meta.dat", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("METADATA", "meta.dat")));
        // null → no prefix
        assertEquals("file.si", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata(null, "file.si")));
        // empty string → no prefix
        assertEquals("file.si", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("", "file.si")));
    }

    public void testToFileIdentifier_nonDefaultFormats_addPrefix() {
        // Non-default formats always get "format/" prefix
        assertEquals("parquet/data.parquet", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("parquet", "data.parquet")));
        assertEquals("arrow/data.arrow", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("arrow", "data.arrow")));
        assertEquals("orc/data.orc", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("orc", "data.orc")));
        assertEquals("custom/my.file", DataFormatAwareStoreDirectory.toFileIdentifier(new FileMetadata("custom", "my.file")));
    }

    // --- listAll includes files from all formats with correct identifiers ---

    public void testListAll_returnsCorrectIdentifiers() throws IOException {
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("lucene");
        }
        try (IndexOutput out = dataFormatAwareStoreDirectory.createOutput("parquet/data.parquet", IOContext.DEFAULT)) {
            out.writeString("parquet");
        }

        String[] files = dataFormatAwareStoreDirectory.listAll();
        List<String> fileList = Arrays.asList(files);

        // Lucene files should appear as plain names (no prefix)
        assertTrue("Lucene file listed as plain name", fileList.contains("_0.si"));
        assertFalse("Lucene file should NOT have lucene/ prefix", fileList.contains("lucene/_0.si"));

        // Non-lucene files should appear with serialized "format/file" form
        assertTrue("Parquet file listed with serialized form", fileList.contains("parquet/data.parquet"));
    }

    // --- getDataFormat comprehensive ---

    public void testGetDataFormat_comprehensive() {
        // Plain filenames → "lucene"
        assertEquals("lucene", dataFormatAwareStoreDirectory.getDataFormat("_0.si"));
        assertEquals("lucene", dataFormatAwareStoreDirectory.getDataFormat("_0.cfs"));
        assertEquals("lucene", dataFormatAwareStoreDirectory.getDataFormat("_0.cfe"));
        assertEquals("lucene", dataFormatAwareStoreDirectory.getDataFormat("segments_1"));
        assertEquals("lucene", dataFormatAwareStoreDirectory.getDataFormat("write.lock"));

        // Prefixed filenames → format name
        assertEquals("parquet", dataFormatAwareStoreDirectory.getDataFormat("parquet/data.parquet"));
        assertEquals("arrow", dataFormatAwareStoreDirectory.getDataFormat("arrow/data.arrow"));
        assertEquals("orc", dataFormatAwareStoreDirectory.getDataFormat("orc/data.orc"));
        assertEquals("custom", dataFormatAwareStoreDirectory.getDataFormat("custom/myfile.dat"));
    }
}
