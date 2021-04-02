/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.Setting;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Allocation constraints specify conditions which, if breached, reduce the priority of a node
 * for receiving shard allocations.
 */
public class AllocationConstraints {
    public final long CONSTRAINT_WEIGHT = 1000000L;
    public static final Setting<ConstraintMode> INDEX_SHARDS_PER_NODE_CONSTRAINT_SETTING =
            new Setting<>("aes.cluster.routing.allocation.constraint.index_shards_per_node", ConstraintMode.NONE.toString(),
                    ConstraintMode::parse, Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final ConstraintMode indexShardsPerNodeConstraint;
    private List<Predicate<ConstraintParams>> constraintPredicates;


    public AllocationConstraints(ConstraintMode indexShardsPerNodeConstraint) {
        this.indexShardsPerNodeConstraint = indexShardsPerNodeConstraint;
        this.constraintPredicates = new ArrayList<>(1);
        this.constraintPredicates.add(isIndexShardsPerNodeBreached());
    }

    public ConstraintMode getIndexShardsPerNodeConstraint() {
        return indexShardsPerNodeConstraint;
    }

    class ConstraintParams {
        private BalancedShardsAllocator.Balancer balancer;
        private BalancedShardsAllocator.ModelNode node;
        private String index;
        private ConstraintMode invocationMode;

        ConstraintParams(BalancedShardsAllocator.Balancer balancer, BalancedShardsAllocator.ModelNode node,
                        String index, ConstraintMode invocationMode) {
            this.balancer = balancer;
            this.node = node;
            this.index = index;
            this.invocationMode = invocationMode;
        }
    }

    /**
     * Evaluates configured allocation constraint predicates for given node - index combination; and returns
     * a weight value based on the number of breached constraints.
     *
     * Constraint weight should be added to the weight calculated via weight function, to reduce priority
     * of allocating on nodes with breached constraints.
     */
    public long weight(BalancedShardsAllocator.Balancer balancer, BalancedShardsAllocator.ModelNode node,
                       String index, ConstraintMode invocationMode) {
        int constraintsBreached = 0;
        ConstraintParams params = new ConstraintParams(balancer, node, index, invocationMode);
        for (Predicate<ConstraintParams> predicate: constraintPredicates) {
            if (predicate.test(params)) {
                constraintsBreached++;
            }
        }
        return constraintsBreached * CONSTRAINT_WEIGHT;
    }

    /**
     * Constraint to control number of shards of an index allocated on a single node.
     *
     * In current weight function implementation, when a node has significantly fewer shards than other nodes (e.g. during
     * single new node addition or node replacement), its weight is much less than other nodes. All shard allocations
     * at this time tend to land on the new node with skewed weight. This breaks index level balance in the cluster,
     * by creating all shards of the same index on one node, often resulting in a hotspot on that node.
     *
     * This constraint is breached when balancer attempts to allocate more than average shards per index per node.
     */
    private Predicate<ConstraintParams> isIndexShardsPerNodeBreached() {
        return (params) -> {
            int currIndexShardsOnNode = params.node.numShards(params.index);
            int allowedIndexShardsPerNode = (int) Math.ceil(params.balancer.avgShardsPerNode(params.index));
            return (params.invocationMode != ConstraintMode.NONE &&
                indexShardsPerNodeConstraint == params.invocationMode &&
                currIndexShardsOnNode >= allowedIndexShardsPerNode);
        };
    }

    /**
     * {@link ConstraintMode} values define possible modes in which allocation constraints can be enabled on
     * the balancer via cluster settings.
     *   1. {@link ConstraintMode#NONE}: "none" disables allocation constraint for all decisions.
     *   2. {@link ConstraintMode#UNASSIGNED}: "unassigned" enables constraints only for decisions to allocate unassigned shards.
     */
    public enum ConstraintMode {

        NONE,
        UNASSIGNED;

        public static ConstraintMode parse(String strValue) {
            if (strValue == null) {
                return null;
            } else {
                strValue = strValue.toUpperCase(Locale.ROOT);
                try {
                    return ConstraintMode.valueOf(strValue);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Illegal constraint.index_shards_per_node value [" + strValue + "]");
                }
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

    }
}
