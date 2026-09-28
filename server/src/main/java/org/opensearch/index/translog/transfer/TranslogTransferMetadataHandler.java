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
        //
        // FORMAT INVARIANT: this map must remain the LAST field of the version-1 layout. The remaining-byte check
        // cannot distinguish it from any other trailing bytes, so appending a further field to version 1 would be
        // read as (part of) this map by new nodes and would corrupt the layout for both sides. Any further field
        // requires bumping TranslogTransferMetadata.CURRENT_VERSION with a reader-first rollout: the metadata
        // stream wrapper in TranslogTransferManager accepts only [CURRENT_VERSION, CURRENT_VERSION], so a version
        // written by an upgraded node is unreadable by an older one until that older release already accepts it.
        // (TranslogTransferMetadataHandlerTests#testReadLegacyMetadataThroughCodecWrapperYieldsEmptyChecksumMap
        // guards the legacy-tail case.)
        //
        // Because the map is inferred rather than tagged, what is read is validated before it is trusted: every
        // key must be a generation inside this metadata's own range that the primary-term map also knows, every
        // value must be a checksum, and nothing but the codec footer may follow. The map only ever lets a download
        // be skipped, so on any doubt it is dropped and every generation is downloaded, which is the pre-existing
        // behaviour.
        metadata.setGenerationToChecksumMapper(readChecksumMapIfPresent(indexInput, metadata));

        return metadata;
    }

    private static Map<String, String> readChecksumMapIfPresent(IndexInput indexInput, TranslogTransferMetadata metadata)
        throws IOException {
        if (indexInput.length() - indexInput.getFilePointer() <= CodecUtil.footerLength()) {
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
        // The generation-to-checksum map is always written last so that readers can detect its presence by the
        // number of bytes left before the codec footer (see readContent). Do not append anything after it within
        // this version; a new field needs a CURRENT_VERSION bump.
        if (content.getGenerationToChecksumMapper() != null) {
            indexOutput.writeMapOfStrings(content.getGenerationToChecksumMapper());
        } else {
            indexOutput.writeMapOfStrings(new HashMap<>());
        }
    }
}
