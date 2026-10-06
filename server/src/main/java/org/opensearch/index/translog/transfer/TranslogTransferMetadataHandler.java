/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(TranslogTransferMetadataHandler.class);

    /**
     * Marker written immediately before the generation-to-checksum map. Its presence, not the number of bytes left
     * before the codec footer, is what tells a reader the map is there. Visible for testing.
     */
    static final int CHECKSUM_MAP_MARKER = 0x434B534D; // "CKSM"

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

        // The generation-to-checksum map was appended to the version-1 layout after the primary-term map, preceded
        // by CHECKSUM_MAP_MARKER. Metadata written before that ends right here, followed only by the codec footer.
        // Older readers stop after the primary-term map and never look at the extra bytes, which keeps the file
        // readable in both directions during a rolling upgrade; a version bump could not offer that, because the
        // metadata stream wrapper in TranslogTransferManager accepts only [CURRENT_VERSION, CURRENT_VERSION].
        //
        // The marker makes the map's presence explicit: bytes that do not start with it are not a checksum map,
        // whatever their length. Anything read is still validated before it is trusted (keys are generations inside
        // this metadata's own range that the primary-term map knows, values are checksums, nothing but the codec
        // footer follows). The map only ever lets a download be skipped, so on any doubt it is dropped and every
        // generation is downloaded, which is the pre-existing behaviour. A field added to version 1 by mistake
        // therefore cannot be misread as the map; it can only disable the optimisation on new readers, which
        // TranslogTransferMetadataHandlerTests#testVersionOneLayoutEndsWithTheChecksumMap catches.
        metadata.setGenerationToChecksumMapper(readChecksumMapIfPresent(indexInput, metadata));

        return metadata;
    }

    private static Map<String, String> readChecksumMapIfPresent(IndexInput indexInput, TranslogTransferMetadata metadata)
        throws IOException {
        if (indexInput.length() - indexInput.getFilePointer() <= CodecUtil.footerLength()) {
            // Written before the checksum map existed.
            return Map.of();
        }
        int marker = indexInput.readInt();
        if (marker != CHECKSUM_MAP_MARKER) {
            logger.warn(
                "ignoring trailing bytes in translog metadata [{}]: expected checksum map marker [{}] but found [{}]",
                indexInput,
                Integer.toHexString(CHECKSUM_MAP_MARKER),
                Integer.toHexString(marker)
            );
            return Map.of();
        }
        Map<String, String> generationToChecksumMapper = indexInput.readMapOfStrings();
        long trailingBytes = indexInput.length() - indexInput.getFilePointer();
        if (trailingBytes > CodecUtil.footerLength()) {
            logger.warn(
                "ignoring generation-to-checksum map in translog metadata [{}]: [{}] unexpected bytes follow it",
                indexInput,
                trailingBytes - CodecUtil.footerLength()
            );
            return Map.of();
        }
        Map<String, String> generationToPrimaryTermMapper = metadata.getGenerationToPrimaryTermMapper();
        for (Map.Entry<String, String> entry : generationToChecksumMapper.entrySet()) {
            if (isKnownGeneration(entry.getKey(), metadata, generationToPrimaryTermMapper) == false || isLong(entry.getValue()) == false) {
                logger.warn(
                    "ignoring generation-to-checksum map in translog metadata [{}]: entry [{}={}] is not a known generation and checksum",
                    indexInput,
                    entry.getKey(),
                    entry.getValue()
                );
                return Map.of();
            }
        }
        return generationToChecksumMapper;
    }

    private static boolean isKnownGeneration(
        String key,
        TranslogTransferMetadata metadata,
        Map<String, String> generationToPrimaryTermMapper
    ) {
        if (isLong(key) == false || generationToPrimaryTermMapper.containsKey(key) == false) {
            return false;
        }
        long generation = Long.parseLong(key);
        return generation >= metadata.getMinTranslogGeneration() && generation <= metadata.getGeneration();
    }

    private static boolean isLong(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
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
        // The generation-to-checksum map is written last, announced by CHECKSUM_MAP_MARKER so that readers identify
        // it explicitly (see readContent). Do not add fields to this version; a new field needs a CURRENT_VERSION
        // bump with a reader-first rollout.
        indexOutput.writeInt(CHECKSUM_MAP_MARKER);
        if (content.getGenerationToChecksumMapper() != null) {
            indexOutput.writeMapOfStrings(content.getGenerationToChecksumMapper());
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
    }
}
