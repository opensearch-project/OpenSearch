/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing if
 * it is not the primary and has replication mode as {@link ReplicationMode#FULL_REPLICATION}.
 *
 * @opensearch.internal
 */
public class FanoutReplicationProxy<ReplicaRequest extends ReplicationRequest<ReplicaRequest>> extends ReplicationProxy<ReplicaRequest> {

    public FanoutReplicationProxy(ReplicationOperation.Replicas<ReplicaRequest> replicasProxy) {
        super(replicasProxy);
    }

    @Override
    protected void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        ReplicationMode replicationMode,
        BiConsumer<
            Consumer<ActionListener<ReplicationOperation.ReplicaResponse>>,
            ReplicationProxyRequest<ReplicaRequest>> performOnReplicasProxyBiConsumer
    ) {
        if (replicationMode == ReplicationMode.FULL_REPLICATION) {
            Consumer<ActionListener<ReplicationOperation.ReplicaResponse>> replicasProxyConsumer = (listener) -> {
                getReplicasProxy().performOn(
                    proxyRequest.getShardRouting(),
                    proxyRequest.getReplicaRequest(),
                    proxyRequest.getPrimaryTerm(),
                    proxyRequest.getGlobalCheckpoint(),
                    proxyRequest.getMaxSeqNoOfUpdatesOrDeletes(),
                    listener
                );
            };
            performOnReplicasProxyBiConsumer.accept(replicasProxyConsumer, proxyRequest);
        }
    }

    @Override
    ReplicationMode determineReplicationMode(ShardRouting shardRouting, ShardRouting primaryRouting) {
        return shardRouting.isSameAllocation(primaryRouting) == false ? ReplicationMode.FULL_REPLICATION : ReplicationMode.NO_REPLICATION;
    }
}
