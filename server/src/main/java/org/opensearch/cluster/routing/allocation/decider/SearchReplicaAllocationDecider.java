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
 * A search replica can be allocated to only nodes with a search role
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

        if ((node.isSearchNode() && isSearchReplica) || (!node.isSearchNode() && !isSearchReplica)) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "node and shard are compatible. node: [%s], is search node: [%s], shard: [%s]",
                node.getId(),
                node.isSearchNode(),
                shardRouting.shortSummary()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "node and shard are compatible. node: [%s], is search node: [%s], shard: [%s]",
                node.getId(),
                node.isSearchNode(),
                shardRouting.shortSummary()
            );
        }
    }
}
