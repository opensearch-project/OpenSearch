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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing basis
 * the shard routing's replication mode and replication override policy.
 *
 * @opensearch.internal
 */
public class ReplicationModeAwareProxy<ReplicaRequest extends ReplicationRequest<ReplicaRequest>> extends ReplicationProxy<ReplicaRequest> {

    private final ReplicationMode replicationModeOverride;
    private final ReplicationOperation.Replicas<ReplicaRequest> primaryTermValidationProxy;

    public ReplicationModeAwareProxy(
        ReplicationMode replicationModeOverride,
        ReplicationOperation.Replicas<ReplicaRequest> replicasProxy,
        ReplicationOperation.Replicas<ReplicaRequest> primaryTermValidationProxy
    ) {
        super(replicasProxy);
        this.replicationModeOverride = Objects.requireNonNull(replicationModeOverride);
        this.primaryTermValidationProxy = Objects.requireNonNull(primaryTermValidationProxy);
    }

    @Override
    protected void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        ReplicationMode replicationMode,
        BiConsumer<
            Consumer<ActionListener<ReplicationOperation.ReplicaResponse>>,
            ReplicationProxyRequest<ReplicaRequest>> performOnReplicasProxyBiConsumer
    ) {
        assert replicationMode == ReplicationMode.FULL_REPLICATION || replicationMode == ReplicationMode.PRIMARY_TERM_VALIDATION;

        Consumer<ActionListener<ReplicationOperation.ReplicaResponse>> replicasProxyConsumer;
        if (replicationMode == ReplicationMode.FULL_REPLICATION) {
            replicasProxyConsumer = (listener) -> {
                getReplicasProxy().performOn(
                    proxyRequest.getShardRouting(),
                    proxyRequest.getReplicaRequest(),
                    proxyRequest.getPrimaryTerm(),
                    proxyRequest.getGlobalCheckpoint(),
                    proxyRequest.getMaxSeqNoOfUpdatesOrDeletes(),
                    listener
                );
            };
        } else {
            replicasProxyConsumer = (listener) -> {
                getPrimaryTermValidationProxy().performOn(
                    proxyRequest.getShardRouting(),
                    proxyRequest.getReplicaRequest(),
                    proxyRequest.getPrimaryTerm(),
                    proxyRequest.getGlobalCheckpoint(),
                    proxyRequest.getMaxSeqNoOfUpdatesOrDeletes(),
                    listener
                );
            };
        }
        performOnReplicasProxyBiConsumer.accept(replicasProxyConsumer, proxyRequest);
    }

    @Override
    ReplicationMode determineReplicationMode(ShardRouting shardRouting, ShardRouting primaryRouting) {

        // If the current routing is the primary, then it does not need to be replicated
        if (shardRouting.isSameAllocation(primaryRouting)) {
            return ReplicationMode.NO_REPLICATION;
        }

        if (primaryRouting.relocating() && shardRouting.isSameAllocation(primaryRouting.getTargetRelocatingShard())) {
            return ReplicationMode.FULL_REPLICATION;
        }

        return replicationModeOverride;
    }

    public ReplicationOperation.Replicas<ReplicaRequest> getPrimaryTermValidationProxy() {
        return primaryTermValidationProxy;
    }
}
