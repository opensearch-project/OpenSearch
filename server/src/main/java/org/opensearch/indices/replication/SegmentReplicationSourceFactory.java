/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

/**
 * Factory to build {@link SegmentReplicationSource} used by {@link SegmentReplicationTargetService}.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceFactory {

    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

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
        if (shard.indexSettings().isSegRepWithRemoteEnabled()) {
            return new RemoteStoreReplicationSource(shard);
        } else {
            return new PrimaryShardReplicationSource(
                shard.recoveryState().getTargetNode(),
                shard.routingEntry().allocationId().getId(),
                transportService,
                recoverySettings,
                getPrimaryNode(shard.shardId())
            );
        }
    }

    private DiscoveryNode getPrimaryNode(ShardId shardId) {
        ShardRouting primaryShard = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        return clusterService.state().nodes().get(primaryShard.currentNodeId());
    }
}
