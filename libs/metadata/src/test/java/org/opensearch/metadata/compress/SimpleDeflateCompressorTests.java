/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.compress;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class SimpleDeflateCompressorTests extends OpenSearchTestCase {

    private final SimpleDeflateCompressor compressor = new SimpleDeflateCompressor();

    public void testCompressUncompressRoundTrip() throws IOException {
        byte[] original = "{\"test\":\"round-trip\"}".getBytes(StandardCharsets.UTF_8);
        BytesReference compressed = compressor.compress(new BytesArray(original));
        BytesReference uncompressed = compressor.uncompress(compressed);
        assertEquals(new BytesArray(original), uncompressed);
    }

    public void testIsCompressedTrue() throws IOException {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        BytesReference compressed = compressor.compress(new BytesArray(data));
        assertTrue(compressor.isCompressed(compressed));
    }

    public void testIsCompressedFalseForRawBytes() {
        assertFalse(compressor.isCompressed(new BytesArray("not compressed".getBytes(StandardCharsets.UTF_8))));
    }

    public void testIsCompressedFalseForTooShort() {
        assertFalse(compressor.isCompressed(new BytesArray(new byte[] { 'D', 'F' })));
    }

    public void testHeaderLength() {
        assertEquals(4, compressor.headerLength());
    }

    public void testStreamRoundTrip() throws IOException {
        byte[] original = "{\"stream\":\"test\"}".getBytes(StandardCharsets.UTF_8);

        // Compress via stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStream out = compressor.threadLocalOutputStream(baos)) {
            out.write(original);
        }
        byte[] compressed = baos.toByteArray();
        assertTrue(compressor.isCompressed(new BytesArray(compressed)));

        // Decompress via stream
        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        try (InputStream in = compressor.threadLocalInputStream(bais)) {
            byte[] result = in.readAllBytes();
            assertArrayEquals(original, result);
        }
    }

    public void testThreadLocalInputStreamInvalidHeader() {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] { 'X', 'Y', 'Z', '0' });
        expectThrows(IllegalArgumentException.class, () -> compressor.threadLocalInputStream(bais));
    }

    public void testName() {
        assertEquals("SIMPLE_DEFLATE", SimpleDeflateCompressor.NAME);
    }
}
