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

import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CONSTRAINT_WEIGHT;

/**
 * Defines a constraint useful to de-prioritize certain nodes as target of unassigned shards used in {@link AllocationConstraints} or
 * re-balancing target used in {@link RebalanceConstraints}
 *
 * @opensearch.internal
 */
public class Constraint implements Predicate<Constraint.ConstraintParams> {

    private boolean enable;
    private Predicate<ConstraintParams> predicate;

    public Constraint(Predicate<ConstraintParams> constraintPredicate) {
        this.predicate = constraintPredicate;
    }

    @Override
    public boolean test(ConstraintParams constraintParams) {
        return this.enable && predicate.test(constraintParams);
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    static class ConstraintParams {
        private ShardsBalancer balancer;
        private BalancedShardsAllocator.ModelNode node;
        private String index;

        ConstraintParams(ShardsBalancer balancer, BalancedShardsAllocator.ModelNode node, String index) {
            this.balancer = balancer;
            this.node = node;
            this.index = index;
        }

        public ShardsBalancer getBalancer() {
            return balancer;
        }

        public BalancedShardsAllocator.ModelNode getNode() {
            return node;
        }

        public String getIndex() {
            return index;
        }

        /**
         * Evaluates configured allocation constraint predicates for given node - index
         * combination; and returns a weight value based on the number of breached
         * constraints.
         * <p>
         * Constraint weight should be added to the weight calculated via weight
         * function, to reduce priority of allocating on nodes with breached
         * constraints.
         * </p>
         */
        public long weight(Map<String, Constraint> constraints) {
            long totalConstraintWeight = 0;
            for (Constraint constraint : constraints.values()) {
                if (constraint.test(this)) {
                    totalConstraintWeight += CONSTRAINT_WEIGHT;
                }
            }
            return totalConstraintWeight;
        }
    }
}
