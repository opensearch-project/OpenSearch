/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;

/**
 * <p>
 * A {@link ShardsBalancer} helps the {@link BalancedShardsAllocator} to perform allocation and balancing
 * operations on the cluster.
 * </p>
 *
 * @opensearch.internal
 */
public abstract class ShardsBalancer {

    /**
     * Performs allocation of unassigned shards on nodes within the cluster.
     */
    abstract void allocateUnassigned();

    /**
     * Moves shards that cannot be allocated to a node anymore.
     */
    abstract void moveShards();

    /**
     *  Balances the nodes on the cluster model.
     */
    abstract void balance();

    /**
     * Make a decision for allocating an unassigned shard.
     * @param shardRouting the shard for which the decision has to be made
     * @return the allocation decision
     */
    abstract AllocateUnassignedDecision decideAllocateUnassigned(ShardRouting shardRouting);

    /**
     * Makes a decision on whether to move a started shard to another node.
     * @param shardRouting the shard for which the decision has to be made
     * @return a move decision for the shard
     */
    abstract MoveDecision decideMove(ShardRouting shardRouting);

    /**
     * Makes a decision about moving a single shard to a different node to form a more
     * optimally balanced cluster.
     * @param shardRouting the shard for which the move decision has to be made
     * @return a move decision for the shard
     */
    abstract MoveDecision decideRebalance(ShardRouting shardRouting);

    /**
     * Returns the average of shards per node for the given index
     */
    public float avgShardsPerNode() {
        return Float.MAX_VALUE;
    }

    /**
     * Returns the global average of shards per node
     */
    public float avgShardsPerNode(String index) {
        return Float.MAX_VALUE;
    }

    /**
     * Returns the average of primary shards per node for the given index
     */
    public float avgPrimaryShardsPerNode(String index) {
        return Float.MAX_VALUE;
    }

    /**
     * Returns the average of primary shards per node
     */
    public float avgPrimaryShardsPerNode() {
        return Float.MAX_VALUE;
    }

    /**
     * Returns the average disk usage in bytes per node. The default returns {@code 0.0}
     * so that subclasses which do not track per-shard byte size contribute a neutral
     * disk-weight component ({@code node.diskUsageInBytes() - 0.0}) instead of saturating
     * the weight function with {@link Double#MAX_VALUE}.
     * <p>
     * Returned as {@code double} rather than {@code float} so that multi-TB cluster totals
     * retain enough precision for the subsequent ratio computation in
     * {@link BalancedShardsAllocator.WeightFunction#weight}.
     */
    public double avgDiskUsageInBytesPerNode() {
        return 0.0;
    }

}
