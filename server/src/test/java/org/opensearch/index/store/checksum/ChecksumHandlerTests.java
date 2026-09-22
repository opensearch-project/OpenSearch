/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Unit tests for {@link FormatChecksumStrategy} implementations:
 * {@link LuceneChecksumHandler} and {@link GenericCRC32ChecksumHandler}.
 */
public class ChecksumHandlerTests extends OpenSearchTestCase {

    // ═══════════════════════════════════════════════════════════════
    // GenericCRC32ChecksumHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGenericCRC32ChecksumHandler_EmptyFile() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        try (IndexOutput out = dir.createOutput("empty.dat", IOContext.DEFAULT)) {
            // write nothing
        }

        long checksum = handler.computeChecksum(dir, "empty.dat");
        CRC32 crc32 = new CRC32();
        assertEquals("CRC32 of empty file should match", crc32.getValue(), checksum);
    }

    public void testGenericCRC32ChecksumHandler_LargeFile() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        // Write a file larger than the 8192 buffer size
        byte[] data = new byte[20000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        try (IndexOutput out = dir.createOutput("large.dat", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksum = handler.computeChecksum(dir, "large.dat");

        CRC32 crc32 = new CRC32();
        crc32.update(data);
        assertEquals("CRC32 of large file should match", crc32.getValue(), checksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // LuceneChecksumHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testLuceneChecksumHandler_ComputeChecksum() throws IOException {
        LuceneChecksumHandler handler = new LuceneChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        try (IndexOutput out = dir.createOutput("lucene_test.si", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, "TestCodec", 1);
            out.writeString("some lucene data");
            CodecUtil.writeFooter(out);
        }

        long checksum = handler.computeChecksum(dir, "lucene_test.si");
        assertTrue("Lucene checksum should be non-zero", checksum != 0);
    }

    // ═══════════════════════════════════════════════════════════════
    // FormatChecksumStrategy default method Tests
    // ═══════════════════════════════════════════════════════════════

    public void testComputeChecksum_UploadChecksumString() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dir.createOutput("default_upload.dat", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksum = handler.computeChecksum(dir, "default_upload.dat");
        String uploadChecksum = Long.toString(checksum);
        assertNotNull(uploadChecksum);
        Long.parseLong(uploadChecksum); // should not throw
    }

    public void testComputeChecksum_Idempotent() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        byte[] data = "idempotent test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dir.createOutput("idem.dat", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksum1 = handler.computeChecksum(dir, "idem.dat");
        long checksum2 = handler.computeChecksum(dir, "idem.dat");
        assertEquals("Checksum should be the same on repeated calls", checksum1, checksum2);
    }

    public void testRegisterChecksum_DefaultNoOp() {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        // Default registerChecksum is a no-op; should not throw
        handler.registerChecksum("file.dat", 12345L, 1L);
    }

    public void testClearChecksums_DefaultNoOp() {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        // Default clearChecksums is a no-op; should not throw
        handler.clearChecksums();
    }
}
