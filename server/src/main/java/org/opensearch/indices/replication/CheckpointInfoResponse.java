/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Response returned from a {@link SegmentReplicationSource} that includes the file metadata, and SegmentInfos
 * associated with a particular {@link ReplicationCheckpoint}. The {@link SegmentReplicationSource} may determine that
 * the requested {@link ReplicationCheckpoint} is behind and return a different {@link ReplicationCheckpoint} in this response.
 *
 * @opensearch.internal
 */
public class CheckpointInfoResponse extends TransportResponse {

    private final ReplicationCheckpoint checkpoint;
    private final Map<String, StoreFileMetadata> metadataMap;
    private final byte[] infosBytes;

    public CheckpointInfoResponse(
        final ReplicationCheckpoint checkpoint,
        final Map<String, StoreFileMetadata> metadataMap,
        final byte[] infosBytes
    ) {
        this.checkpoint = checkpoint;
        this.metadataMap = metadataMap;
        this.infosBytes = infosBytes;
    }

    public CheckpointInfoResponse(StreamInput in) throws IOException {
        this.checkpoint = new ReplicationCheckpoint(in);
        this.metadataMap = in.readMap(StreamInput::readString, StoreFileMetadata::new);
        this.infosBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkpoint.writeTo(out);
        out.writeMap(metadataMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        out.writeByteArray(infosBytes);
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public Map<String, StoreFileMetadata> getMetadataMap() {
        return metadataMap;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }
}
