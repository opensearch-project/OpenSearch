/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

public class SegmentReplicationSourceFactory {

    private TransportService transportService;
    private RecoverySettings recoverySettings;
    private ClusterService clusterService;

    public SegmentReplicationSourceFactory(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    public SegmentReplicationSource get(IndexShard shard) {
        return new PeerReplicationSource(
            transportService,
            recoverySettings,
            clusterService.localNode(),
            getPrimaryNode(shard.shardId()),
            shard.routingEntry().allocationId().getId()
        );
    }

    private DiscoveryNode getPrimaryNode(ShardId shardId) {
        ClusterState state = clusterService.state();
        ShardRouting primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
        return state.nodes().get(primaryShard.currentNodeId());
    }
}
