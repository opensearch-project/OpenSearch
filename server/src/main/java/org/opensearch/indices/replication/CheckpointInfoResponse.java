/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Response returned from a {@link SegmentReplicationSource} that includes the file metadata, and SegmentInfos
 * associated with a particular {@link ReplicationCheckpoint}. The {@link SegmentReplicationSource} may determine that
 * the requested {@link ReplicationCheckpoint} is behind and return a different {@link ReplicationCheckpoint} in this response.
 *
 * @opensearch.internal
 */
public class CheckpointInfoResponse extends TransportResponse {

    private static final Version FORMAT_AWARE_VERSION = Version.V_3_1_0;

    private final ReplicationCheckpoint checkpoint;
    private final Map<String, StoreFileMetadata> legacyMetadataMap;
    private final Map<FileMetadata, StoreFileMetadata> formatAwareMetadataMap;
    private final byte[] infosBytes;

    // Constructor with legacy metadata map for backward compatibility (used by tests)
    public CheckpointInfoResponse(
        final ReplicationCheckpoint checkpoint,
        final Map<String, StoreFileMetadata> metadataMap,
        final byte[] infosBytes
    ) {
        this.checkpoint = checkpoint;
        this.legacyMetadataMap = metadataMap;
        this.formatAwareMetadataMap = convertLegacyToFormatAware(metadataMap);
        this.infosBytes = infosBytes;
    }

    // Constructor using checkpoint's metadata
    public CheckpointInfoResponse(final ReplicationCheckpoint checkpoint, final byte[] infosBytes) {
        this.checkpoint = checkpoint;
        this.infosBytes = infosBytes;
        this.formatAwareMetadataMap = checkpoint.getFormatAwareMetadataMap();
        this.legacyMetadataMap = checkpoint.getMetadataMap();
    }

    public CheckpointInfoResponse(StreamInput in) throws IOException {
        this.checkpoint = new ReplicationCheckpoint(in);

        if (in.getVersion().onOrAfter(FORMAT_AWARE_VERSION)) {
            // Read format-aware metadata directly
            this.formatAwareMetadataMap = in.readMap(
                streamInput -> new FileMetadata(streamInput.readString(), streamInput.readString()),
                StoreFileMetadata::new
            );
            // Build legacy map for backward compatibility
            this.legacyMetadataMap = new HashMap<>();
            for (Map.Entry<FileMetadata, StoreFileMetadata> entry : formatAwareMetadataMap.entrySet()) {
                legacyMetadataMap.put(entry.getKey().file(), entry.getValue());
            }
        } else {
            // Read legacy format and convert
            this.legacyMetadataMap = in.readMap(StreamInput::readString, StoreFileMetadata::new);
            this.formatAwareMetadataMap = convertLegacyToFormatAware(legacyMetadataMap);
        }

        this.infosBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkpoint.writeTo(out);

        if (out.getVersion().onOrAfter(FORMAT_AWARE_VERSION)) {
            // Write format-aware metadata
            out.writeMap(formatAwareMetadataMap,
                (keyOut, fileMetadata) -> {
                    keyOut.writeString(fileMetadata.dataFormat());
                    keyOut.writeString(fileMetadata.file());
                },
                (valueOut, storeFileMetadata) -> storeFileMetadata.writeTo(valueOut)
            );
        } else {
            // Write legacy format for backward compatibility
            out.writeMap(legacyMetadataMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        }

        out.writeByteArray(infosBytes);
    }

    /**
     * Converts legacy String-based metadata map to format-aware FileMetadata-based map.
     */
    private static Map<FileMetadata, StoreFileMetadata> convertLegacyToFormatAware(Map<String, StoreFileMetadata> legacyMap) {
        Map<FileMetadata, StoreFileMetadata> formatAwareMap = new HashMap<>();
        for (Map.Entry<String, StoreFileMetadata> entry : legacyMap.entrySet()) {
            String fileName = entry.getKey();
            StoreFileMetadata storeMetadata = entry.getValue();

            // Use the dataFormat from StoreFileMetadata if available, otherwise default to "lucene"
            String dataFormat = storeMetadata.dataFormat() != null ? storeMetadata.dataFormat() : "lucene";
            FileMetadata fileMetadata = new FileMetadata(dataFormat, fileName);
            formatAwareMap.put(fileMetadata, storeMetadata);
        }
        return formatAwareMap;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Returns the legacy metadata map for backward compatibility.
     * Format information may be lost in this conversion.
     *
     * @deprecated Use getFormatAwareMetadataMap() instead to preserve format information
     */
    @Deprecated
    public Map<String, StoreFileMetadata> getMetadataMap() {
        return legacyMetadataMap;
    }

    /**
     * Returns the format-aware metadata map that preserves format information.
     * This is the preferred method for accessing file metadata.
     */
    public Map<FileMetadata, StoreFileMetadata> getFormatAwareMetadataMap() {
        return formatAwareMetadataMap;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }
}
