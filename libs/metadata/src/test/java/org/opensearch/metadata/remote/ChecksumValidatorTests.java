/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ChecksumValidatorTests extends OpenSearchTestCase {

    private static final String TEST_CODEC = "test-codec";
    private static final int VERSION = 1;

    private ChecksumValidator validator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validator = new ChecksumValidator();
    }

    public void testValidateAndSliceWithValidData() throws IOException {
        byte[] content = "test content data".getBytes(StandardCharsets.UTF_8);
        BytesReference blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        BytesReference extracted = validator.validateAndSlice(blobData, TEST_CODEC, VERSION, VERSION);

        assertArrayEquals(content, BytesReference.toBytes(extracted));
    }

    public void testValidateAndSliceWithEmptyContent() throws IOException {
        byte[] content = new byte[0];
        BytesReference blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        BytesReference extracted = validator.validateAndSlice(blobData, TEST_CODEC, VERSION, VERSION);

        assertEquals(0, extracted.length());
    }

    public void testValidateAndSliceWithLargeContent() throws IOException {
        byte[] content = randomByteArrayOfLength(10000);
        BytesReference blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        BytesReference extracted = validator.validateAndSlice(blobData, TEST_CODEC, VERSION, VERSION);

        assertArrayEquals(content, BytesReference.toBytes(extracted));
    }

    public void testValidateAndSliceWithNullDataThrowsException() {
        CorruptMetadataException ex = expectThrows(
            CorruptMetadataException.class,
            () -> validator.validateAndSlice(null, TEST_CODEC, VERSION, VERSION)
        );
        assertTrue(ex.getMessage().contains("Empty or null data"));
    }

    public void testValidateAndSliceWithEmptyDataThrowsException() {
        CorruptMetadataException ex = expectThrows(
            CorruptMetadataException.class,
            () -> validator.validateAndSlice(new BytesArray(new byte[0]), TEST_CODEC, VERSION, VERSION)
        );
        assertTrue(ex.getMessage().contains("Empty or null data"));
    }

    public void testValidateAndSliceWithCorruptedDataThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        byte[] blobBytes = BytesReference.toBytes(createChecksumBlob(TEST_CODEC, VERSION, content));

        // Corrupt the data
        blobBytes[blobBytes.length / 2] ^= 0xFF;

        BytesReference corrupted = new BytesArray(blobBytes);
        expectThrows(CorruptMetadataException.class, () -> validator.validateAndSlice(corrupted, TEST_CODEC, VERSION, VERSION));
    }

    public void testValidateAndSliceWithWrongCodecThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        BytesReference blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        expectThrows(CorruptMetadataException.class, () -> validator.validateAndSlice(blobData, "wrong-codec", VERSION, VERSION));
    }

    public void testValidateAndSliceWithVersionTooOldThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        BytesReference blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        expectThrows(CorruptMetadataException.class, () -> validator.validateAndSlice(blobData, TEST_CODEC, 2, 2));
    }

    public void testValidateAndSliceWithVersionTooNewThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        BytesReference blobData = createChecksumBlob(TEST_CODEC, 2, content);

        expectThrows(CorruptMetadataException.class, () -> validator.validateAndSlice(blobData, TEST_CODEC, 1, 1));
    }

    public void testRoundTripWithRandomData() throws IOException {
        for (int i = 0; i < 50; i++) {
            byte[] content = randomByteArrayOfLength(randomIntBetween(1, 5000));
            BytesReference blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

            BytesReference extracted = validator.validateAndSlice(blobData, TEST_CODEC, VERSION, VERSION);

            assertArrayEquals("Round-trip failed for iteration " + i, content, BytesReference.toBytes(extracted));
        }
    }

    /**
     * Creates a blob with Lucene checksum format: [Header][Content][Footer]
     */
    private BytesReference createChecksumBlob(String codec, int version, byte[] content) {
        try {
            ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
            IndexOutput indexOutput = new ByteBuffersIndexOutput(dataOutput, "test", "test");

            CodecUtil.writeHeader(indexOutput, codec, version);
            indexOutput.writeBytes(content, content.length);
            CodecUtil.writeFooter(indexOutput);
            indexOutput.close();

            return new BytesArray(dataOutput.toArrayCopy());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create checksum blob", e);
        }
    }
}
