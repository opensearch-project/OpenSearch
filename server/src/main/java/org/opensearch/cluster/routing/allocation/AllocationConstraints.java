/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsBalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Allocation constraints specify conditions which, if breached, reduce the
 * priority of a node for receiving shard allocations.
 *
 * @opensearch.internal
 */
public class AllocationConstraints {
    public final long CONSTRAINT_WEIGHT = 1000000L;
    private List<Predicate<ConstraintParams>> constraintPredicates;

    public AllocationConstraints() {
        this.constraintPredicates = new ArrayList<>(1);
        this.constraintPredicates.add(isIndexShardsPerNodeBreached());
    }

    class ConstraintParams {
        private ShardsBalancer balancer;
        private BalancedShardsAllocator.ModelNode node;
        private String index;

        ConstraintParams(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
            this.balancer = balancer;
            this.node = node;
            this.index = index;
        }
    }

    /**
     * Evaluates configured allocation constraint predicates for given node - index
     * combination; and returns a weight value based on the number of breached
     * constraints.
     *
     * Constraint weight should be added to the weight calculated via weight
     * function, to reduce priority of allocating on nodes with breached
     * constraints.
     *
     * This weight function is used only in case of unassigned shards to avoid overloading a newly added node.
     * Weight calculation in other scenarios like shard movement and re-balancing remain unaffected by this function.
     */
    public long weight(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
        int constraintsBreached = 0;
        ConstraintParams params = new ConstraintParams(balancer, node, index);
        for (Predicate<ConstraintParams> predicate : constraintPredicates) {
            if (predicate.test(params)) {
                constraintsBreached++;
            }
        }
        return constraintsBreached * CONSTRAINT_WEIGHT;
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
    private Predicate<ConstraintParams> isIndexShardsPerNodeBreached() {
        return (params) -> {
            int currIndexShardsOnNode = params.node.numShards(params.index);
            int allowedIndexShardsPerNode = (int) Math.ceil(params.balancer.avgShardsPerNode(params.index));
            return (currIndexShardsOnNode >= allowedIndexShardsPerNode);
        };
    }

}
