/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.List;

/**
 * TransportRequest to fetch segments from another Node.
 *
 * @opensearch.internal
 */
public class GetFilesRequest extends SegmentReplicationTransportRequest {

    private final List<StoreFileMetadata> filesToFetch;
    private final ReplicationCheckpoint checkpoint;

    public GetFilesRequest(
        long replicationId,
        String targetAllocationId,
        DiscoveryNode targetNode,
        List<StoreFileMetadata> filesToFetch,
        ReplicationCheckpoint checkpoint
    ) {
        super(replicationId, targetAllocationId, targetNode);
        this.filesToFetch = filesToFetch;
        this.checkpoint = checkpoint;
    }

    public GetFilesRequest(StreamInput in) throws IOException {
        super(in);
        this.filesToFetch = in.readList(StoreFileMetadata::new);
        this.checkpoint = new ReplicationCheckpoint(in);
    }

    public List<StoreFileMetadata> getFilesToFetch() {
        return filesToFetch;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(filesToFetch);
        checkpoint.writeTo(out);
    }
}
