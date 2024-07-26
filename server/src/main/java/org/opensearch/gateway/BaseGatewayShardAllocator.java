/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An abstract class that implements basic functionality for allocating
 * shards to nodes based on shard copies that already exist in the cluster.
 * <p>
 * Individual implementations of this class are responsible for providing
 * the logic to determine to which nodes (if any) those shards are allocated.
 *
 * @opensearch.internal
 */
public abstract class BaseGatewayShardAllocator {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Allocate an unassigned shard to nodes (if any) where valid copies of the shard already exist.
     * It is up to the individual implementations of {@link #makeAllocationDecision(ShardRouting, RoutingAllocation, Logger)}
     * to make decisions on assigning shards to nodes.
     *
     * @param shardRouting                the shard to allocate
     * @param allocation                  the allocation state container object
     * @param unassignedAllocationHandler handles the allocation of the current shard
     */
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        final AllocateUnassignedDecision allocateUnassignedDecision = makeAllocationDecision(shardRouting, allocation, logger);
        executeDecision(shardRouting, allocateUnassignedDecision, allocation, unassignedAllocationHandler);
    }

    protected void allocateUnassignedBatchOnTimeout(Set<ShardId> shardIds, RoutingAllocation allocation, boolean primary) {
        if (shardIds.isEmpty()) {
            return;
        }
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            ShardRouting unassignedShard = iterator.next();
            AllocateUnassignedDecision allocationDecision;
            if (unassignedShard.primary() == primary && shardIds.contains(unassignedShard.shardId())) {
                allocationDecision = AllocateUnassignedDecision.throttle(null);
                executeDecision(unassignedShard, allocationDecision, allocation, iterator);
            }
        }
    }

    protected void executeDecision(
        ShardRouting shardRouting,
        AllocateUnassignedDecision allocateUnassignedDecision,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (allocateUnassignedDecision.isDecisionTaken() == false) {
            // no decision was taken by this allocator
            return;
        }

        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            unassignedAllocationHandler.initialize(
                allocateUnassignedDecision.getTargetNode().getId(),
                allocateUnassignedDecision.getAllocationId(),
                getExpectedShardSize(shardRouting, allocation),
                allocation.changes()
            );
        } else {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    protected long getExpectedShardSize(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return allocation.snapshotShardSizeInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else {
                return ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
            }
        } else {
            return allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
    }

    /**
     * Make a decision on the allocation of an unassigned shard.  This method is used by
     * {@link #allocateUnassigned(ShardRouting, RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler)} to make decisions
     * about whether or not the shard can be allocated by this allocator and if so, to which node it will be allocated.
     *
     * @param unassignedShard the unassigned shard to allocate
     * @param allocation      the current routing state
     * @param logger          the logger
     * @return an {@link AllocateUnassignedDecision} with the final decision of whether to allocate and details of the decision
     */
    public abstract AllocateUnassignedDecision makeAllocationDecision(
        ShardRouting unassignedShard,
        RoutingAllocation allocation,
        Logger logger
    );

    /**
     * Builds decisions for all nodes in the cluster, so that the explain API can provide information on
     * allocation decisions for each node, while still waiting to allocate the shard (e.g. due to fetching shard data).
     */
    protected static List<NodeAllocationResult> buildDecisionsForAllNodes(ShardRouting shard, RoutingAllocation allocation) {
        List<NodeAllocationResult> results = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            results.add(new NodeAllocationResult(node.node(), null, decision));
        }
        return results;
    }
}
