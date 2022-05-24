/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Set;

/**
 * Response returned from a {@link SegmentReplicationSource} that includes the file metadata, and SegmentInfos
 * associated with a particular {@link ReplicationCheckpoint}. The {@link SegmentReplicationSource} may determine that
 * the requested {@link ReplicationCheckpoint} is behind and return a different {@link ReplicationCheckpoint} in this response.
 *
 * @opensearch.internal
 */
public class CheckpointInfoResponse extends TransportResponse {

    private final ReplicationCheckpoint checkpoint;
    private final Store.MetadataSnapshot snapshot;
    private final byte[] infosBytes;
    // pendingDeleteFiles are segments that have been merged away in the latest in memory SegmentInfos
    // but are still referenced by the latest commit point (Segments_N).
    private final Set<StoreFileMetadata> pendingDeleteFiles;

    public CheckpointInfoResponse(
        final ReplicationCheckpoint checkpoint,
        final Store.MetadataSnapshot snapshot,
        final byte[] infosBytes,
        final Set<StoreFileMetadata> additionalFiles
    ) {
        this.checkpoint = checkpoint;
        this.snapshot = snapshot;
        this.infosBytes = infosBytes;
        this.pendingDeleteFiles = additionalFiles;
    }

    public CheckpointInfoResponse(StreamInput in) throws IOException {
        this.checkpoint = new ReplicationCheckpoint(in);
        this.snapshot = new Store.MetadataSnapshot(in);
        this.infosBytes = in.readByteArray();
        this.pendingDeleteFiles = in.readSet(StoreFileMetadata::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkpoint.writeTo(out);
        snapshot.writeTo(out);
        out.writeByteArray(infosBytes);
        out.writeCollection(pendingDeleteFiles);
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public Store.MetadataSnapshot getSnapshot() {
        return snapshot;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }

    public Set<StoreFileMetadata> getPendingDeleteFiles() {
        return pendingDeleteFiles;
    }
}
