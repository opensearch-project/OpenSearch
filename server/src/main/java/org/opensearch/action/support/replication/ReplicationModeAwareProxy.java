/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.action.support.replication.ReplicationOperation.ReplicaResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.action.ActionListener;

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

    /**
     * This ReplicasProxy is used for performing primary term validation.
     */
    private final ReplicationOperation.Replicas<ReplicaRequest> primaryTermValidationProxy;

    private final DiscoveryNodes discoveryNodes;

    private final boolean isRemoteEnabled;

    public ReplicationModeAwareProxy(
        ReplicationMode replicationModeOverride,
        DiscoveryNodes discoveryNodes,
        ReplicationOperation.Replicas<ReplicaRequest> replicasProxy,
        ReplicationOperation.Replicas<ReplicaRequest> primaryTermValidationProxy,
        boolean remoteIndexSettingsEnabled
    ) {
        super(replicasProxy);
        this.replicationModeOverride = Objects.requireNonNull(replicationModeOverride);
        this.primaryTermValidationProxy = Objects.requireNonNull(primaryTermValidationProxy);
        this.discoveryNodes = discoveryNodes;
        this.isRemoteEnabled = remoteIndexSettingsEnabled;
    }

    @Override
    protected void performOnReplicaProxy(
        ReplicationProxyRequest<ReplicaRequest> proxyRequest,
        ReplicationMode replicationMode,
        BiConsumer<Consumer<ActionListener<ReplicaResponse>>, ReplicationProxyRequest<ReplicaRequest>> performOnReplicaConsumer
    ) {
        assert replicationMode == ReplicationMode.FULL_REPLICATION || replicationMode == ReplicationMode.PRIMARY_TERM_VALIDATION;

        Consumer<ActionListener<ReplicaResponse>> replicasProxyConsumer;
        if (replicationMode == ReplicationMode.FULL_REPLICATION) {
            replicasProxyConsumer = getReplicasProxyConsumer(fullReplicationProxy, proxyRequest);
        } else {
            replicasProxyConsumer = getReplicasProxyConsumer(primaryTermValidationProxy, proxyRequest);
        }
        performOnReplicaConsumer.accept(replicasProxyConsumer, proxyRequest);
    }

    @Override
    ReplicationMode determineReplicationMode(ShardRouting shardRouting, ShardRouting primaryRouting) {
        // If the current routing is the primary, then it does not need to be replicated
        if (shardRouting.isSameAllocation(primaryRouting)) {
            return ReplicationMode.NO_REPLICATION;
        }
        // Perform full replication during primary relocation
        if (primaryRouting.relocating() && shardRouting.isSameAllocation(primaryRouting.getTargetRelocatingShard())) {
            return ReplicationMode.FULL_REPLICATION;
        }
        /*
        Only applicable during remote store migration.
        During the migration process, remote based index settings will not be enabled,
        thus we will rely on node attributes to figure out the replication mode
         */
        if (isRemoteEnabled == false) {
            DiscoveryNode targetNode = discoveryNodes.get(shardRouting.currentNodeId());
            if (targetNode != null && targetNode.isRemoteStoreNode() == false) {
                // Perform full replication if replica is hosted on a non-remote node.
                return ReplicationMode.FULL_REPLICATION;
            }
        }
        return replicationModeOverride;
    }
}
