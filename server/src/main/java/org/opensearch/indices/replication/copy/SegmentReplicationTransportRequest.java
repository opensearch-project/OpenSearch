/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

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

    protected SegmentReplicationTransportRequest(long replicationId, String targetAllocationId, DiscoveryNode discoveryNode) {
        this.replicationId = replicationId;
        this.targetAllocationId = targetAllocationId;
        this.targetNode = discoveryNode;
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
