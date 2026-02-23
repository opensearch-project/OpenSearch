/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.remote.RemoteWriteableEntity;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.index.model.IndexMetadataModel;

import java.io.IOException;
import java.io.InputStream;

/**
 * Handles reading IndexMetadataModel from remote storage.
 * <p>
 * This class implements {@link RemoteWriteableEntity} to deserialize IndexMetadataModel
 * from remote blob storage. It handles:
 * <ul>
 *   <li>Lucene checksum validation via {@link ChecksumValidator}</li>
 *   <li>Optional decompression of compressed data</li>
 *   <li>SMILE XContent parsing to IndexMetadataModel</li>
 * </ul>
 * <p>
 * Note: This class is read-only by design. The serialize() method throws
 * {@link UnsupportedOperationException}. For serialization, use RemoteIndexMetadata
 * in the server module.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RemoteIndexMetadataModel implements RemoteWriteableEntity<IndexMetadataModel> {

    /**
     * Codec name used for index metadata format.
     * Must match the codec name used by RemoteIndexMetadata in the server module.
     */
    public static final String INDEX_METADATA_CODEC = "index-metadata";

    /**
     * Minimum supported codec version.
     */
    public static final int MIN_CODEC_VERSION = 1;

    /**
     * Maximum supported codec version.
     * This should match INDEX_METADATA_CURRENT_CODEC_VERSION in RemoteIndexMetadata.
     */
    public static final int MAX_CODEC_VERSION = 2;

    private final Compressor compressor;
    private final ChecksumValidator checksumValidator;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Creates a new RemoteIndexMetadataModel without compression support.
     */
    public RemoteIndexMetadataModel() {
        this(null, NamedXContentRegistry.EMPTY);
    }

    /**
     * Creates a new RemoteIndexMetadataModel with optional compression support.
     *
     * @param compressor the compressor to use for decompression, or null if no compression
     */
    public RemoteIndexMetadataModel(Compressor compressor) {
        this(compressor, NamedXContentRegistry.EMPTY);
    }

    /**
     * Creates a new RemoteIndexMetadataModel with optional compression support and XContent registry.
     * <p>
     * The XContent registry is required for parsing server-specific types like RolloverInfo
     * which use NamedXContentRegistry for Condition types.
     *
     * @param compressor the compressor to use for decompression, or null if no compression
     * @param xContentRegistry the registry for named XContent types (e.g., rollover conditions)
     */
    public RemoteIndexMetadataModel(Compressor compressor, NamedXContentRegistry xContentRegistry) {
        this.compressor = compressor;
        this.xContentRegistry = xContentRegistry;
        this.checksumValidator = new ChecksumValidator();
    }

    /**
     * Deserializes an IndexMetadataModel from the given input stream.
     * <p>
     * The input stream is expected to contain data in the Lucene checksum format:
     * [Header: codec name + version][Content (possibly compressed)][Footer: checksum]
     * <p>
     * The content is expected to be in SMILE XContent format.
     *
     * @param inputStream the input stream containing the serialized data
     * @return the deserialized IndexMetadataModel
     * @throws IOException if deserialization fails
     */
    @Override
    public IndexMetadataModel deserialize(InputStream inputStream) throws IOException {
        BytesReference bytes = new BytesArray(inputStream.readAllBytes());

        // Validate checksum and extract content slice (zero-copy)
        BytesReference content = checksumValidator.validateAndSlice(bytes, INDEX_METADATA_CODEC, MIN_CODEC_VERSION, MAX_CODEC_VERSION);

        // Decompress if needed
        if (compressor != null && compressor.isCompressed(content)) {
            content = compressor.uncompress(content);
        }

        // Parse XContent (SMILE format)
        try (
            XContentParser parser = XContentType.SMILE.xContent()
                .createParser(xContentRegistry, DeprecationHandler.IGNORE_DEPRECATIONS, BytesReference.toBytes(content))
        ) {
            return IndexMetadataModel.fromXContent(parser);
        }
    }

    /**
     * Serialization is not supported - this class is read-only.
     * <p>
     * RemoteIndexMetadataModel is designed for reading index metadata from remote storage
     * in standalone applications. For serialization, use RemoteIndexMetadata in the server module.
     *
     * @return never returns
     * @throws UnsupportedOperationException always, as this class is read-only
     */
    @Override
    public InputStream serialize() throws IOException {
        throw new UnsupportedOperationException("RemoteIndexMetadataModel is read-only. Use RemoteIndexMetadata for serialization.");
    }
}
