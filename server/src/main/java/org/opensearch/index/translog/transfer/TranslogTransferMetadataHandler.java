/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.io.IndexIOStreamHandler;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handler for {@link TranslogTransferMetadata}
 *
 * @opensearch.internal
 */
public class TranslogTransferMetadataHandler implements IndexIOStreamHandler<TranslogTransferMetadata> {

    /**
     * Implements logic to read content from file input stream {@code indexInput} and parse into {@link TranslogTransferMetadata}
     *
     * @param indexInput file input stream
     * @return content parsed to {@link TranslogTransferMetadata}
     */
    @Override
    public TranslogTransferMetadata readContent(IndexInput indexInput) throws IOException {
        long primaryTerm = indexInput.readLong();
        long generation = indexInput.readLong();
        long minTranslogGeneration = indexInput.readLong();
        Map<String, String> generationToPrimaryTermMapper = indexInput.readMapOfStrings();

        int count = generationToPrimaryTermMapper.size();
        TranslogTransferMetadata metadata = new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, count);
        metadata.setGenerationToPrimaryTermMapper(generationToPrimaryTermMapper);

        // We set the GenerationToChecksumMapper only if it is present in the file.
        // Else we initialise it with an empty map.
        try {
            Map<String, String> generationToChecksumMapper = indexInput.readMapOfStrings();
            metadata.setGenerationToChecksumMapper(generationToChecksumMapper);
        } catch (EOFException ignored) {
            metadata.setGenerationToChecksumMapper(Map.of());
        }

        return metadata;
    }

    /**
     * Implements logic to write content from {@code content} to file output stream {@code indexOutput}
     *
     * @param indexOutput file input stream
     * @param content metadata content to be written
     */
    @Override
    public void writeContent(IndexOutput indexOutput, TranslogTransferMetadata content) throws IOException {
        indexOutput.writeLong(content.getPrimaryTerm());
        indexOutput.writeLong(content.getGeneration());
        indexOutput.writeLong(content.getMinTranslogGeneration());
        if (content.getGenerationToPrimaryTermMapper() != null) {
            indexOutput.writeMapOfStrings(content.getGenerationToPrimaryTermMapper());
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
        // Write the generation to checksum mapping at the end.
        if (content.getGenerationToChecksumMapper() != null) {
            indexOutput.writeMapOfStrings(content.getGenerationToChecksumMapper());
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
    }
}
