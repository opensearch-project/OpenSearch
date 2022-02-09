/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

public class TransportCheckpointInfoResponse extends TransportResponse {

    private final ReplicationCheckpoint checkpoint;
    private final Store.MetadataSnapshot snapshot;
    private final byte[] infosBytes;

    public TransportCheckpointInfoResponse(final ReplicationCheckpoint checkpoint,
                                           final Store.MetadataSnapshot snapshot,
                                           final byte[] infosBytes) {
        this.checkpoint = checkpoint;
        this.snapshot = snapshot;
        this.infosBytes = infosBytes;
    }

    public TransportCheckpointInfoResponse(StreamInput in) throws IOException {
        this.checkpoint = new ReplicationCheckpoint(in);
        this.snapshot = new Store.MetadataSnapshot(in);
        this.infosBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkpoint.writeTo(out);
        snapshot.writeTo(out);
        out.writeByteArray(infosBytes);
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

}
