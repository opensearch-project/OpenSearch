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
import org.junit.Before;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;

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

    public void testReadContent() throws IOException {
        TranslogTransferMetadata expectedMetadata = getTestMetadata();

        // Operation: Read expected metadata from source input stream.
        IndexInput indexInput = new ByteArrayIndexInput("metadata file", getTestMetadataBytes());
        TranslogTransferMetadata actualMetadata = handler.readContent(indexInput);

        // Verification: Compare actual metadata read from the source input stream.
        assertEquals(expectedMetadata, actualMetadata);
    }

    public void testWriteContent() throws IOException {
        TranslogTransferMetadata expectedMetadata = getTestMetadata();

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
        int count = generationToPrimaryTermMapper.size();
        TranslogTransferMetadata actualMetadata = new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, count);
        actualMetadata.setGenerationToPrimaryTermMapper(generationToPrimaryTermMapper);
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

    private byte[] getTestMetadataBytes() throws IOException {
        TranslogTransferMetadata metadata = getTestMetadata();

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        indexOutput.writeLong(metadata.getPrimaryTerm());
        indexOutput.writeLong(metadata.getGeneration());
        indexOutput.writeLong(metadata.getMinTranslogGeneration());
        Map<String, String> generationToPrimaryTermMapper = metadata.getGenerationToPrimaryTermMapper();
        indexOutput.writeMapOfStrings(generationToPrimaryTermMapper);
        indexOutput.close();

        return BytesReference.toBytes(output.bytes());
    }
}
