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

import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.isPerIndexPrimaryShardsPerNodeBreached;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.isPrimaryShardsPerNodeBreached;

/**
 * Constraints applied during rebalancing round; specify conditions which, if breached, reduce the
 * priority of a node for receiving shard relocations.
 *
 * @opensearch.internal
 */
public class RebalanceConstraints {

    private Map<String, Constraint> constraints;

    public RebalanceConstraints(RebalanceParameter rebalanceParameter) {
        this.constraints = new HashMap<>();
        this.constraints.put(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, new Constraint(isPerIndexPrimaryShardsPerNodeBreached()));
        this.constraints.put(
            CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID,
            new Constraint(isPrimaryShardsPerNodeBreached(rebalanceParameter.getPreferPrimaryBalanceBuffer()))
        );
    }

    public void updateRebalanceConstraint(String constraint, boolean enable) {
        this.constraints.get(constraint).setEnable(enable);
    }

    public long weight(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
        Constraint.ConstraintParams params = new Constraint.ConstraintParams(balancer, node, index);
        return params.weight(constraints);
    }
}
