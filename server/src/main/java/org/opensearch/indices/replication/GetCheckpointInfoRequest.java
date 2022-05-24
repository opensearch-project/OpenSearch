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

import java.io.IOException;

/**
 * Request object for fetching Segment Metadata from a {@link ReplicationCheckpoint}
 *
 * @opensearch.internal
 */
public class GetCheckpointInfoRequest extends SegmentReplicationTransportRequest {

    private final ReplicationCheckpoint checkpoint;

    public GetCheckpointInfoRequest(StreamInput in) throws IOException {
        super(in);
        checkpoint = new ReplicationCheckpoint(in);
    }

    public GetCheckpointInfoRequest(
        long replicationId,
        String targetAllocationId,
        DiscoveryNode targetNode,
        ReplicationCheckpoint checkpoint
    ) {
        super(replicationId, targetAllocationId, targetNode);
        this.checkpoint = checkpoint;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        checkpoint.writeTo(out);
    }
}
