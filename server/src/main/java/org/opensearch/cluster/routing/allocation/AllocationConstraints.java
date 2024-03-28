/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsBalancer;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.isIndexShardsPerNodeBreached;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.isPerIndexPrimaryShardsPerNodeBreached;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.isPrimaryShardsPerNodeBreached;

/**
 * Allocation constraints specify conditions which, if breached, reduce the priority of a node for receiving unassigned
 * shard allocations. Weight calculation in other scenarios like shard movement and re-balancing remain unaffected by
 * this constraint.
 *
 * @opensearch.internal
 */
public class AllocationConstraints {
    private Map<String, Constraint> constraints;

    public AllocationConstraints() {
        this.constraints = new HashMap<>();
        this.constraints.put(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, new Constraint(isIndexShardsPerNodeBreached()));
        this.constraints.put(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, new Constraint(isPerIndexPrimaryShardsPerNodeBreached()));
        this.constraints.put(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, new Constraint(isPrimaryShardsPerNodeBreached(0.0f)));
    }

    public void updateAllocationConstraint(String constraint, boolean enable) {
        this.constraints.get(constraint).setEnable(enable);
    }

    public long weight(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
        Constraint.ConstraintParams params = new Constraint.ConstraintParams(balancer, node, index);
        return params.weight(constraints);
    }
}
