/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

/**
 * A search replica can be allocated to only nodes with attribute searchonly:true,
 * other shard types will not be allocated to these nodes.
 * @opensearch.internal
 */
public class SearchReplicaAllocationDecider extends AllocationDecider {

    public static final String NAME = "search_replica_allocation";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node.node(), allocation);
    }

    private Decision canAllocate(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        boolean isSearchReplica = shardRouting.isSearchOnly();

        if (node.isSearchDedicatedDataNode() && isSearchReplica) {
            return allocation.decision(Decision.YES, NAME, "node is search dedicated data node and shard is search replica");
        } else if (!node.isSearchDedicatedDataNode() && !isSearchReplica) {
            return allocation.decision(Decision.YES, NAME, "node is not search dedicated data node and shard is replica");
        } else if (node.isSearchDedicatedDataNode() && !isSearchReplica) {
            return allocation.decision(Decision.NO, NAME, "Node is a search dedicated data node but shard is not a search replica");
        } else {
            return allocation.decision(Decision.NO, NAME, "Node is not a search dedicated data node but shard is a search replica");
        }
    }
}
