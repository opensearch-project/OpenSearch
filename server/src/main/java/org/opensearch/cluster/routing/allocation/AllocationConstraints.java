/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsBalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.cluster.routing.allocation.RebalanceConstraints.PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID;
import static org.opensearch.cluster.routing.allocation.RebalanceConstraints.PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_WEIGHT;
import static org.opensearch.cluster.routing.allocation.RebalanceConstraints.isPrimaryShardsPerIndexPerNodeBreached;

/**
 * Allocation constraints specify conditions which, if breached, reduce the
 * priority of a node for receiving unassigned shard allocations.
 *
 * @opensearch.internal
 */
public class AllocationConstraints {

    public final static long INDEX_SHARD_PER_NODE_BREACH_WEIGHT = 100000L;

    /**
     *
     * This constraint is only applied for unassigned shards to avoid overloading a newly added node.
     * Weight calculation in other scenarios like shard movement and re-balancing remain unaffected by this constraint.
     */
    public final static String INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID = "index.shard.breach.constraint";
    private Map<String, Constraint> constraintSet;

    public void updateAllocationConstraint(String constraint, boolean enable) {
        this.constraintSet.get(constraint).setEnable(enable);
    }

    public long weight(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
        Constraint.ConstraintParams params = new Constraint.ConstraintParams(balancer, node, index);
        return params.weight(constraintSet);
    }

    public AllocationConstraints() {
        this.constraintSet = new HashMap<>();
        this.constraintSet.putIfAbsent(
            INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID,
            new Constraint(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, isIndexShardsPerNodeBreached(), INDEX_SHARD_PER_NODE_BREACH_WEIGHT)
        );
        this.constraintSet.putIfAbsent(
            PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID,
            new Constraint(
                PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID,
                isPrimaryShardsPerIndexPerNodeBreached(),
                PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_WEIGHT
            )
        );
    }

    /**
     * Constraint to control number of shards of an index allocated on a single
     * node.
     *
     * In current weight function implementation, when a node has significantly
     * fewer shards than other nodes (e.g. during single new node addition or node
     * replacement), its weight is much less than other nodes. All shard allocations
     * at this time tend to land on the new node with skewed weight. This breaks
     * index level balance in the cluster, by creating all shards of the same index
     * on one node, often resulting in a hotspot on that node.
     *
     * This constraint is breached when balancer attempts to allocate more than
     * average shards per index per node.
     */
    public static Predicate<Constraint.ConstraintParams> isIndexShardsPerNodeBreached() {
        return (params) -> {
            int currIndexShardsOnNode = params.getNode().numShards(params.getIndex());
            int allowedIndexShardsPerNode = (int) Math.ceil(params.getBalancer().avgShardsPerNode(params.getIndex()));
            return (currIndexShardsOnNode >= allowedIndexShardsPerNode);
        };
    }
}
