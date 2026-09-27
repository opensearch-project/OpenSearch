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
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.io.IndexIOStreamHandler;

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

        // The generation-to-checksum map was appended to the format after the primary-term map. Metadata written
        // before that ends right here, followed only by the codec footer, so its presence is decided by how many
        // bytes remain rather than by probing for EOF. Older readers stop after the primary-term map and never
        // look at the extra map, which keeps the file readable in both directions during a rolling upgrade.
        if (indexInput.length() - indexInput.getFilePointer() > CodecUtil.footerLength()) {
            metadata.setGenerationToChecksumMapper(indexInput.readMapOfStrings());
        } else {
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
        // The generation-to-checksum map is always written last so that readers can detect its presence by the
        // number of bytes left before the codec footer (see readContent).
        if (content.getGenerationToChecksumMapper() != null) {
            indexOutput.writeMapOfStrings(content.getGenerationToChecksumMapper());
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
    }
}
