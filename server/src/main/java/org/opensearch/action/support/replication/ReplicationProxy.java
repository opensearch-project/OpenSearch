/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;

import java.util.function.BiConsumer;

/**
 * Used for performing any replication operation on replicas. Depending on the implementation, the replication call
 * can fanout or stops here.
 *
 * @opensearch.internal
 */
public abstract class ReplicationProxy<ReplicaRequest> {

    /**
     * Depending on the actual implementation and the passed {@link ReplicationMode}, the replication
     * mode is determined using which the replication request is performed on the replica or not.
     *
     * @param proxyRequest                     replication proxy request
     * @param originalPerformOnReplicaConsumer original performOnReplica method passed as consumer
     */
    public void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        BiConsumer<ReplicationProxyRequest<ReplicaRequest>, ReplicationMode> originalPerformOnReplicaConsumer
    ) {
        ReplicationMode replicationMode = determineReplicationMode(proxyRequest.getShardRouting(), proxyRequest.getPrimaryRouting());
        // If the replication modes are 1. Logical replication or 2. Primary term validation, we let the call get performed on the
        // replica shard.
        if (replicationMode == ReplicationMode.FULL_REPLICATION || replicationMode == ReplicationMode.PRIMARY_TERM_VALIDATION) {
            originalPerformOnReplicaConsumer.accept(proxyRequest, replicationMode);
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
    abstract ReplicationMode determineReplicationMode(final ShardRouting shardRouting, final ShardRouting primaryRouting);
}
