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
import org.opensearch.index.shard.ShardId;

import java.io.IOException;

public class TrackShardRequest extends SegmentReplicationTransportRequest {
    private ShardId shardId;

    public TrackShardRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
    }

    public TrackShardRequest(ShardId shardId, String targetAllocationId, DiscoveryNode discoveryNode) {
        super(-1, targetAllocationId, discoveryNode);
        this.shardId = shardId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
    }
}
