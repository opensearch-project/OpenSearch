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
 * Used for performing any replication operation on replicas. Depending on the implementation, the replication call
 * can fanout or stops here.
 *
 * @opensearch.internal
 */
public abstract class ReplicationProxy<ReplicaRequest extends ReplicationRequest<ReplicaRequest>> {

    /**
     * This is the replicas proxy which is used for full replication.
     */
    protected final Replicas<ReplicaRequest> fullReplicationProxy;

    public ReplicationProxy(Replicas<ReplicaRequest> fullReplicationProxy) {
        this.fullReplicationProxy = fullReplicationProxy;
    }

    /**
     * Depending on the actual implementation and the passed {@link ReplicationMode}, the replication
     * mode is determined using which the replication request is performed on the replica or not.
     *
     * @param proxyRequest                     replication proxy request
     * @param performOnReplicaConsumer performOnReplicasProxy
     */
    final void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        BiConsumer<Consumer<ActionListener<ReplicaResponse>>, ReplicationProxyRequest<ReplicaRequest>> performOnReplicaConsumer
    ) {
        ReplicationMode replicationMode = determineReplicationMode(proxyRequest.getShardRouting(), proxyRequest.getPrimaryRouting());
        // If the replication modes are 1. Logical replication or 2. Primary term validation, we let the call get performed on the
        // replica shard.
        if (replicationMode == ReplicationMode.NO_REPLICATION) {
            return;
        }
        performOnReplicaProxy(proxyRequest, replicationMode, performOnReplicaConsumer);
    }

    /**
     * The implementor can decide the {@code Consumer<ActionListener<ReplicationOperation.ReplicaResponse>>} basis the
     * proxyRequest and replicationMode. This will ultimately make the calls to replica.
     *
     * @param proxyRequest                     replication proxy request
     * @param replicationMode                  replication mode
     * @param performOnReplicaConsumer performOnReplicasProxy
     */
    protected abstract void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        ReplicationMode replicationMode,
        BiConsumer<Consumer<ActionListener<ReplicaResponse>>, ReplicationProxyRequest<ReplicaRequest>> performOnReplicaConsumer
    );

    /**
     * Determines what is the replication mode basis the constructor arguments of the implementation and the current
     * replication mode aware shard routing.
     *
     * @param shardRouting   replication mode aware ShardRouting
     * @param primaryRouting primary ShardRouting
     * @return the determined replication mode.
     */
    abstract ReplicationMode determineReplicationMode(final ShardRouting shardRouting, final ShardRouting primaryRouting);

    protected Consumer<ActionListener<ReplicaResponse>> getReplicasProxyConsumer(
        Replicas<ReplicaRequest> proxy,
        ReplicationProxyRequest<ReplicaRequest> proxyRequest
    ) {
        return (listener) -> proxy.performOn(
            proxyRequest.getShardRouting(),
            proxyRequest.getReplicaRequest(),
            proxyRequest.getPrimaryTerm(),
            proxyRequest.getGlobalCheckpoint(),
            proxyRequest.getMaxSeqNoOfUpdatesOrDeletes(),
            listener
        );
    }
}
