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
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Base request for a Segment Replication Transport request.
 *
 * @opensearch.internal
 */
public abstract class SegmentReplicationTransportRequest extends TransportRequest {

    private final long replicationId;
    private final String targetAllocationId;
    private final DiscoveryNode targetNode;

    protected SegmentReplicationTransportRequest(StreamInput in) throws IOException {
        super(in);
        replicationId = in.readLong();
        targetAllocationId = in.readString();
        targetNode = new DiscoveryNode(in);
    }

    protected SegmentReplicationTransportRequest(long replicationId, String targetAllocationId, DiscoveryNode targetNode) {
        this.replicationId = replicationId;
        this.targetAllocationId = targetAllocationId;
        this.targetNode = targetNode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(replicationId);
        out.writeString(targetAllocationId);
        targetNode.writeTo(out);
    }

    public long getReplicationId() {
        return replicationId;
    }

    public String getTargetAllocationId() {
        return targetAllocationId;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }
}
