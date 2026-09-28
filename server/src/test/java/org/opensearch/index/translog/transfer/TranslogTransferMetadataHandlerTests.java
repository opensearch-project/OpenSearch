/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
     * Metadata written by a node that predates the generation-to-checksum map ends with the primary-term map,
     * followed only by the codec footer. Read through the same versioned wrapper the transfer manager uses, it
     * must yield an empty checksum map rather than misinterpreting the footer bytes.
     */
    public void testReadLegacyMetadataThroughCodecWrapperYieldsEmptyChecksumMap() throws IOException {
        TranslogTransferMetadata legacy = getTestMetadata();
        BytesStreamOutput output = new BytesStreamOutput();
        try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096)) {
            CodecUtil.writeHeader(indexOutput, TranslogTransferMetadata.METADATA_CODEC, TranslogTransferMetadata.CURRENT_VERSION);
            indexOutput.writeLong(legacy.getPrimaryTerm());
            indexOutput.writeLong(legacy.getGeneration());
            indexOutput.writeLong(legacy.getMinTranslogGeneration());
            indexOutput.writeMapOfStrings(legacy.getGenerationToPrimaryTermMapper());
            CodecUtil.writeFooter(indexOutput);
        }

        TranslogTransferMetadata actual = codecWrapper().readStream(
            new ByteArrayIndexInput("metadata file", BytesReference.toBytes(output.bytes()))
        );
        assertEquals(legacy, actual);
        assertEquals(legacy.getGenerationToPrimaryTermMapper(), actual.getGenerationToPrimaryTermMapper());
        assertEquals(Map.of(), actual.getGenerationToChecksumMapper());
    }

    /**
     * The trailing map is inferred from the remaining byte count, so what is read is validated before it is trusted.
     * An entry whose key is not a generation the primary-term map knows, or whose value is not a checksum, means the
     * bytes were not written as a checksum map; the map is dropped and every generation is downloaded, rather than
     * the metadata being misread.
     */
    public void testImplausibleTrailingMapIsDroppedRatherThanTrusted() throws IOException {
        TranslogTransferMetadata base = getTestMetadata();
        Map<String, String> unknownGeneration = Map.of("300", "1234", "999", "5678");
        Map<String, String> nonNumericChecksum = Map.of("300", "1234", "400", "not-a-checksum");
        for (Map<String, String> implausible : List.of(unknownGeneration, nonNumericChecksum)) {
            TranslogTransferMetadata actual = codecWrapper().readStream(
                new ByteArrayIndexInput("metadata file", writeThroughCodec(base, implausible, new byte[0]))
            );
            assertEquals(base.getGenerationToPrimaryTermMapper(), actual.getGenerationToPrimaryTermMapper());
            assertEquals(Map.of(), actual.getGenerationToChecksumMapper());
        }
        // The same entries with a valid key set are accepted, so the rejection above is about content, not shape.
        TranslogTransferMetadata accepted = codecWrapper().readStream(
            new ByteArrayIndexInput("metadata file", writeThroughCodec(base, Map.of("300", "1234", "400", "5678"), new byte[0]))
        );
        assertEquals(Map.of("300", "1234", "400", "5678"), accepted.getGenerationToChecksumMapper());
    }

    /**
     * Anything after the checksum map other than the codec footer violates the last-field invariant; the map is
     * dropped rather than assumed to be complete or correct.
     */
    public void testTrailingBytesAfterChecksumMapDropTheMap() throws IOException {
        TranslogTransferMetadata base = getTestMetadata();
        Map<String, String> checksums = Map.of("300", "1234", "400", "5678");
        TranslogTransferMetadata actual = codecWrapper().readStream(
            new ByteArrayIndexInput("metadata file", writeThroughCodec(base, checksums, randomByteArrayOfLength(randomIntBetween(1, 64))))
        );
        assertEquals(base.getGenerationToPrimaryTermMapper(), actual.getGenerationToPrimaryTermMapper());
        assertEquals(Map.of(), actual.getGenerationToChecksumMapper());
    }

    /**
     * Writes {@code base} through the codec wrapper's header/footer with {@code checksums} appended as the trailing
     * map and {@code extra} bytes placed between that map and the footer.
     */
    private static byte[] writeThroughCodec(TranslogTransferMetadata base, Map<String, String> checksums, byte[] extra) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096)) {
            CodecUtil.writeHeader(indexOutput, TranslogTransferMetadata.METADATA_CODEC, TranslogTransferMetadata.CURRENT_VERSION);
            indexOutput.writeLong(base.getPrimaryTerm());
            indexOutput.writeLong(base.getGeneration());
            indexOutput.writeLong(base.getMinTranslogGeneration());
            indexOutput.writeMapOfStrings(base.getGenerationToPrimaryTermMapper());
            indexOutput.writeMapOfStrings(checksums);
            indexOutput.writeBytes(extra, extra.length);
            CodecUtil.writeFooter(indexOutput);
        }
        return BytesReference.toBytes(output.bytes());
    }

    /**
     * Full round trip through the versioned codec wrapper, with and without checksum entries, must preserve both maps.
     */
    public void testCodecWrapperRoundTripPreservesChecksumMap() throws IOException {
        for (TranslogTransferMetadata expected : List.of(getTestMetadata(), getTestMetadataWithGenerationToChecksumMap())) {
            BytesStreamOutput output = new BytesStreamOutput();
            try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096)) {
                codecWrapper().writeStream(indexOutput, expected);
            }
            TranslogTransferMetadata actual = codecWrapper().readStream(
                new ByteArrayIndexInput("metadata file", BytesReference.toBytes(output.bytes()))
            );
            assertEquals(expected, actual);
            assertEquals(expected.getGenerationToPrimaryTermMapper(), actual.getGenerationToPrimaryTermMapper());
            Map<String, String> expectedChecksums = expected.getGenerationToChecksumMapper() == null
                ? Map.of()
                : expected.getGenerationToChecksumMapper();
            assertEquals(expectedChecksums, actual.getGenerationToChecksumMapper());
        }
    }

    private static VersionedCodecStreamWrapper<TranslogTransferMetadata> codecWrapper() {
        return new VersionedCodecStreamWrapper<>(
            new TranslogTransferMetadataHandlerFactory(),
            TranslogTransferMetadata.CURRENT_VERSION,
            TranslogTransferMetadata.CURRENT_VERSION,
            TranslogTransferMetadata.METADATA_CODEC
        );
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
