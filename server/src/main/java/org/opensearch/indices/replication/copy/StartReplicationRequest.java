/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class StartReplicationRequest extends SegmentReplicationTransportRequest {

    private final ReplicationCheckpoint checkpoint;

    public StartReplicationRequest(StreamInput in) throws IOException {
        super(in);
        checkpoint = new ReplicationCheckpoint(in);
    }

    public StartReplicationRequest(long replicationId,
                                   String targetAllocationId,
                                   DiscoveryNode targetNode,
                                   ReplicationCheckpoint checkpoint) {
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
