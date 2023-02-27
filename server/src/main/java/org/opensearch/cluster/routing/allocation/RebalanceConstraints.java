/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsBalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.PREFER_PER_INDEX_PRIMARY_SHARD_BALANCE;

/**
 * Constraints applied during rebalancing round; specify conditions which, if breached, reduce the
 * priority of a node for receiving shard relocations.
 *
 * @opensearch.internal
 */
public class RebalanceConstraints {
    public final static String PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID = PREFER_PER_INDEX_PRIMARY_SHARD_BALANCE.getKey();
    private Map<String, Constraint> constraints;

    public RebalanceConstraints() {
        this.constraints = new HashMap<>();
        this.constraints.putIfAbsent(
            PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID,
            new Constraint(PREFER_PRIMARY_SHARD_BALANCE_NODE_BREACH_ID, isPrimaryShardsPerIndexPerNodeBreached())
        );
    }

    public void updateRebalanceConstraint(String constraint, boolean enable) {
        this.constraints.get(constraint).setEnable(enable);
    }

    public long weight(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
        Constraint.ConstraintParams params = new Constraint.ConstraintParams(balancer, node, index);
        return params.weight(constraints);
    }

    /**
     * When primary balance is preferred, add node constraint of average primary shards per node to give the node a
     * higher weight resulting in lesser chances of being target of unassigned shard allocation or rebalancing target node
     */
    public static Predicate<Constraint.ConstraintParams> isPrimaryShardsPerIndexPerNodeBreached() {
        return (params) -> {
            int currPrimaryShardsOnNode = params.getNode().numPrimaryShards(params.getIndex());
            int allowedPrimaryShardsPerNode = (int) Math.ceil(params.getBalancer().avgPrimaryShardsPerNode(params.getIndex()));
            return currPrimaryShardsOnNode > allowedPrimaryShardsPerNode;
        };
    }
}
