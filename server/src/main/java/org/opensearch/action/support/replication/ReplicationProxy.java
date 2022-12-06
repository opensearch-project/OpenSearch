/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.seqno.ReplicationTracker.ReplicationMode;
import org.opensearch.index.shard.ReplicationGroup.ReplicationModeAwareShardRouting;

import java.util.function.Consumer;

/**
 * Used for performing any replication operation on replicas. Depending on the implementation, the replication call
 * can fanout or stops here.
 *
 * @opensearch.internal
 */
public abstract class ReplicationProxy<ReplicaRequest> {

    /**
     * Depending on the actual implementation and the passed {@link ReplicationModeAwareShardRouting}, the replication
     * mode is determined using which the replication request is performed on the replica or not.
     *
     * @param replicationProxyRequest          replication proxy request
     * @param originalPerformOnReplicaConsumer original performOnReplica method passed as consumer
     */
    public void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> replicationProxyRequest,
        Consumer<ReplicationProxyRequest<ReplicaRequest>> originalPerformOnReplicaConsumer
    ) {
        ReplicationMode replicationMode = determineReplicationMode(
            replicationProxyRequest.getReplicationModeAwareShardRouting(),
            replicationProxyRequest.getPrimaryRouting()
        );
        // If the replication modes are 1. Logical replication or 2. Primary term validation, we let the call get performed on the
        // replica shard.
        if (replicationMode == ReplicationMode.FULL_REPLICATION || replicationMode == ReplicationMode.PRIMARY_TERM_VALIDATION) {
            originalPerformOnReplicaConsumer.accept(replicationProxyRequest);
        }
    }

    /**
     * Determines what is the replication mode basis the constructor arguments of the implementation and the current
     * replication mode aware shard routing.
     *
     * @param shardRouting   replication mode aware ShardRouting
     * @param primaryRouting primary ShardRouting
     * @return the determined replication mode.
     */
    abstract ReplicationMode determineReplicationMode(
        final ReplicationModeAwareShardRouting shardRouting,
        final ShardRouting primaryRouting
    );
}
