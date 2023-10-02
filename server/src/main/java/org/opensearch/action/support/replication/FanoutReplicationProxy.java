/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.action.support.replication.ReplicationOperation.ReplicaResponse;
import org.opensearch.action.support.replication.ReplicationOperation.Replicas;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.action.ActionListener;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing if
 * it is not the primary and has replication mode as {@link ReplicationMode#FULL_REPLICATION}.
 *
 * @opensearch.internal
 */
public class FanoutReplicationProxy<ReplicaRequest extends ReplicationRequest<ReplicaRequest>> extends ReplicationProxy<ReplicaRequest> {

    public FanoutReplicationProxy(Replicas<ReplicaRequest> replicasProxy) {
        super(replicasProxy);
    }

    @Override
    protected void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        ReplicationMode replicationMode,
        BiConsumer<Consumer<ActionListener<ReplicaResponse>>, ReplicationProxyRequest<ReplicaRequest>> performOnReplicaConsumer
    ) {
        assert replicationMode == ReplicationMode.FULL_REPLICATION : "FanoutReplicationProxy allows only full replication mode";
        performOnReplicaConsumer.accept(getReplicasProxyConsumer(fullReplicationProxy, proxyRequest), proxyRequest);
    }

    @Override
    ReplicationMode determineReplicationMode(ShardRouting shardRouting, ShardRouting primaryRouting) {
        return shardRouting.isSameAllocation(primaryRouting) == false ? ReplicationMode.FULL_REPLICATION : ReplicationMode.NO_REPLICATION;
    }
}
