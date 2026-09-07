/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.metadata.AutoExpandReplicas;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

/**
 * This allocation decider ensures that replica-only nodes follow strict allocation rules:
 * <ul>
 *   <li>Replica-only nodes never host primary shards</li>
 *   <li>Replica-only nodes only host replica shards from indices with auto_expand_replicas: 0-all</li>
 *   <li>Primary shards are never promoted on replica-only nodes</li>
 *   <li>Regular data nodes are not affected by replica-only node presence</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class ReplicaOnlyAllocationDecider extends AllocationDecider {

    public static final String NAME = "replica_only";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // CRITICAL: Never allow primary allocation to replica-only nodes, even with force
        if (node.node().isReplicaOnlyNode()) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "primary shard [%s] cannot be force allocated to replica-only node [%s]",
                shardRouting.shardId(),
                node.nodeId()
            );
        }
        return allocation.decision(Decision.YES, NAME, "node is not a replica-only node");
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (!node.isReplicaOnlyNode()) {
            // Regular data nodes participate in auto-expand for all indices
            return allocation.decision(Decision.YES, NAME, "node [%s] is a data node, eligible for auto-expand", node.getId());
        }

        // Replica-only nodes only participate in 0-all auto-expand
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(indexMetadata.getSettings());
        boolean isAutoExpandAll = autoExpandReplicas.isEnabled()
            && autoExpandReplicas.getMaxReplicas() == Integer.MAX_VALUE
            && autoExpandReplicas.toString().startsWith("0-");

        if (isAutoExpandAll) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "replica-only node [%s] is eligible for auto-expand replicas from index [%s] with auto_expand_replicas: 0-all",
                node.getId(),
                indexMetadata.getIndex().getName()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "replica-only node [%s] is not eligible for index [%s] without auto_expand_replicas: 0-all",
                node.getId(),
                indexMetadata.getIndex().getName()
            );
        }
    }

    private Decision canAllocate(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        boolean isReplicaOnlyNode = node.isReplicaOnlyNode();

        // Case 1: Primary shard allocation
        if (shardRouting.primary()) {
            if (isReplicaOnlyNode) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "primary shard [%s] cannot be allocated to replica-only node [%s]",
                    shardRouting.shardId(),
                    node.getId()
                );
            }
            // Allow primaries on regular data nodes
            return allocation.decision(Decision.YES, NAME, "node [%s] is a data node, can host primary shard", node.getId());
        }

        // Case 2: Replica shard allocation
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(indexMetadata.getSettings());
        boolean isAutoExpandAll = autoExpandReplicas.isEnabled()
            && autoExpandReplicas.getMaxReplicas() == Integer.MAX_VALUE
            && autoExpandReplicas.toString().startsWith("0-");

        if (isReplicaOnlyNode) {
            if (isAutoExpandAll) {
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "replica shard [%s] from auto-expand (0-all) index can be allocated to replica-only node [%s]",
                    shardRouting.shardId(),
                    node.getId()
                );
            } else {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "replica shard [%s] cannot be allocated to replica-only node [%s] "
                        + "because index [%s] does not have auto_expand_replicas: 0-all",
                    shardRouting.shardId(),
                    node.getId(),
                    indexMetadata.getIndex().getName()
                );
            }
        }

        // Regular data nodes can host any replica
        return allocation.decision(Decision.YES, NAME, "node [%s] is a data node, can host replica shard", node.getId());
    }
}
