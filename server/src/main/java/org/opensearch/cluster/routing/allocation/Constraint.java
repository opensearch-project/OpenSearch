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
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Defines a constraint useful to de-prioritize certain nodes as target of unassigned shards used in {@link AllocationConstraints} or
 * re-balancing target used in {@link RebalanceConstraints}
 *
 * @opensearch.internal
 */
public class Constraint implements Predicate<Constraint.ConstraintParams> {
    private String name;

    private long weight;

    private boolean enable;
    private Predicate<ConstraintParams> predicate;

    public Constraint(String name, Predicate<ConstraintParams> constraintPredicate, long weight) {
        this.name = name;
        this.weight = weight;
        this.predicate = constraintPredicate;
        this.enable = false;
    }

    @Override
    public boolean test(ConstraintParams constraintParams) {
        return this.enable && predicate.test(constraintParams);
    }

    public String getName() {
        return name;
    }

    public long getWeight() {
        return weight;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Constraint that = (Constraint) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
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
        public long weight(Map<String, Constraint> constraintSet) {
            long totalConstraintWeight = 0;
            for (Constraint constraint : constraintSet.values()) {
                if (constraint.test(this)) {
                    totalConstraintWeight += constraint.getWeight();
                }
            }
            return totalConstraintWeight;
        }
    }
}
