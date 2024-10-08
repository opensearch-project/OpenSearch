/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TranslogTransferMetadataHandlerTests extends OpenSearchTestCase {
    private TranslogTransferMetadataHandler handler;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        handler = new TranslogTransferMetadataHandler();
    }

    /**
     * Tests the readContent method of the TranslogTransferMetadataHandler, which reads the TranslogTransferMetadata
     * from the provided IndexInput.
     *
     * @throws IOException if there is an error reading the metadata from the IndexInput
     */
    public void testReadContent() throws IOException {
        TranslogTransferMetadata expectedMetadata = getTestMetadata();

        // Operation: Read expected metadata from source input stream.
        IndexInput indexInput = new ByteArrayIndexInput("metadata file", getTestMetadataBytes(expectedMetadata));
        TranslogTransferMetadata actualMetadata = handler.readContent(indexInput);

        // Verification: Compare actual metadata read from the source input stream.
        assertEquals(expectedMetadata, actualMetadata);
    }

    /**
     * Tests the readContent method of the TranslogTransferMetadataHandler, which reads the TranslogTransferMetadata
     * that includes the generation-to-checksum map from the provided IndexInput.
     *
     * @throws IOException if there is an error reading the metadata from the IndexInput
     */
    public void testReadContentForMetadataWithgenerationToChecksumMap() throws IOException {
        TranslogTransferMetadata expectedMetadata = getTestMetadataWithGenerationToChecksumMap();

        // Operation: Read expected metadata from source input stream.
        IndexInput indexInput = new ByteArrayIndexInput("metadata file", getTestMetadataBytes(expectedMetadata));
        TranslogTransferMetadata actualMetadata = handler.readContent(indexInput);

        // Verification: Compare actual metadata read from the source input stream.
        assertEquals(expectedMetadata, actualMetadata);
    }

    /**
     * Tests the writeContent method of the TranslogTransferMetadataHandler, which writes the provided
     * TranslogTransferMetadata to the OutputStreamIndexOutput.
     *
     * @throws IOException if there is an error writing the metadata to the OutputStreamIndexOutput
     */
    public void testWriteContent() throws IOException {
        verifyWriteContent(getTestMetadata());
    }

    /**
     * Tests the writeContent method of the TranslogTransferMetadataHandler, which writes the provided
     * TranslogTransferMetadata that includes the generation-to-checksum map to the OutputStreamIndexOutput.
     *
     * @throws IOException if there is an error writing the metadata to the OutputStreamIndexOutput
     */
    public void testWriteContentWithGeneratonToChecksumMap() throws IOException {
        verifyWriteContent(getTestMetadataWithGenerationToChecksumMap());
    }

    /**
     * Verifies the writeContent method of the TranslogTransferMetadataHandler by writing the provided
     * TranslogTransferMetadata to an OutputStreamIndexOutput, and then reading it back and comparing it
     * to the original metadata.
     *
     * @param expectedMetadata the expected TranslogTransferMetadata to be written and verified
     * @throws IOException if there is an error writing or reading the metadata
     */
    private void verifyWriteContent(TranslogTransferMetadata expectedMetadata) throws IOException {
        // Operation: Write expected metadata to the target output stream.
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput actualMetadataStream = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        handler.writeContent(actualMetadataStream, expectedMetadata);
        actualMetadataStream.close();

        // Verification: Compare actual metadata written to the target output stream.
        IndexInput indexInput = new ByteArrayIndexInput("metadata file", BytesReference.toBytes(output.bytes()));
        long primaryTerm = indexInput.readLong();
        long generation = indexInput.readLong();
        long minTranslogGeneration = indexInput.readLong();
        Map<String, String> generationToPrimaryTermMapper = indexInput.readMapOfStrings();
        Map<String, String> generationToChecksumMapper = indexInput.readMapOfStrings();
        int count = generationToPrimaryTermMapper.size();
        TranslogTransferMetadata actualMetadata = new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, count);
        actualMetadata.setGenerationToPrimaryTermMapper(generationToPrimaryTermMapper);
        actualMetadata.setGenerationToChecksumMapper(generationToChecksumMapper);
        assertEquals(expectedMetadata, actualMetadata);
    }

    private TranslogTransferMetadata getTestMetadata() {
        long primaryTerm = 3;
        long generation = 500;
        long minTranslogGeneration = 300;
        Map<String, String> generationToPrimaryTermMapper = new HashMap<>();
        generationToPrimaryTermMapper.put("300", "1");
        generationToPrimaryTermMapper.put("400", "2");
        generationToPrimaryTermMapper.put("500", "3");
        int count = generationToPrimaryTermMapper.size();
        TranslogTransferMetadata metadata = new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, count);
        metadata.setGenerationToPrimaryTermMapper(generationToPrimaryTermMapper);

        return metadata;
    }

    private TranslogTransferMetadata getTestMetadataWithGenerationToChecksumMap() {
        TranslogTransferMetadata metadata = getTestMetadata();
        Map<String, String> generationToChecksumMapper = Map.of(
            String.valueOf(300),
            String.valueOf(1234),
            String.valueOf(400),
            String.valueOf(4567)
        );
        metadata.setGenerationToChecksumMapper(generationToChecksumMapper);
        return metadata;
    }

    /**
     * Creates a byte array representation of the provided TranslogTransferMetadata instance, which includes
     * the primary term, generation, minimum translog generation, generation-to-primary term mapping, and
     * generation-to-checksum mapping (if available).
     *
     * @param metadata the TranslogTransferMetadata instance to be converted to a byte array
     * @return the byte array representation of the TranslogTransferMetadata
     * @throws IOException if there is an error writing the metadata to the byte array
     */
    private byte[] getTestMetadataBytes(TranslogTransferMetadata metadata) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096)) {
            indexOutput.writeLong(metadata.getPrimaryTerm());
            indexOutput.writeLong(metadata.getGeneration());
            indexOutput.writeLong(metadata.getMinTranslogGeneration());
            indexOutput.writeMapOfStrings(metadata.getGenerationToPrimaryTermMapper());
            if (metadata.getGenerationToChecksumMapper() != null) indexOutput.writeMapOfStrings(metadata.getGenerationToChecksumMapper());
        }
        return BytesReference.toBytes(output.bytes());
    }
}
