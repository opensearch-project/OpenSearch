/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.AsyncShardFetch.FetchResult;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.NodeGatewayStartedShard;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PrimaryShardBatchAllocator is similar to {@link org.opensearch.gateway.PrimaryShardAllocator} only difference is
 * that it can allocate multiple unassigned primary shards wherein PrimaryShardAllocator can only allocate single
 * unassigned shard.
 * The primary shard batch allocator allocates multiple unassigned primary shards to nodes that hold
 * valid copies of the unassigned primaries.  It does this by iterating over all unassigned
 * primary shards in the routing table and fetching shard metadata from each node in the cluster
 * that holds a copy of the shard.  The shard metadata from each node is compared against the
 * set of valid allocation IDs and for all valid shard copies (if any), the primary shard batch allocator
 * executes the allocation deciders to chose a copy to assign the primary shard to.
 * <p>
 * Note that the PrimaryShardBatchAllocator does *not* allocate primaries on index creation
 * (see {@link org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator}),
 * nor does it allocate primaries when a primary shard failed and there is a valid replica
 * copy that can immediately be promoted to primary, as this takes place in {@link RoutingNodes#failShard}.
 *
 * @opensearch.internal
 */
public abstract class PrimaryShardBatchAllocator extends PrimaryShardAllocator {

    abstract protected FetchResult<NodeGatewayStartedShardsBatch> fetchData(
        List<ShardRouting> eligibleShards,
        List<ShardRouting> inEligibleShards,
        RoutingAllocation allocation
    );

    protected FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(
        ShardRouting shard,
        RoutingAllocation allocation
    ) {
        logger.error("fetchData for single shard called via batch allocator, shard id {}", shard.shardId());
        throw new IllegalStateException("PrimaryShardBatchAllocator should only be used for a batch of shards");
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard, RoutingAllocation allocation, Logger logger) {
        AllocateUnassignedDecision decision = getInEligibleShardDecision(unassignedShard, allocation);
        if (decision != null) {
            return decision;
        }
        final FetchResult<NodeGatewayStartedShardsBatch> shardsState = fetchData(
            List.of(unassignedShard),
            Collections.emptyList(),
            allocation
        );
        List<NodeGatewayStartedShard> nodeGatewayStartedShards = adaptToNodeShardStates(unassignedShard, shardsState);
        return getAllocationDecision(unassignedShard, allocation, nodeGatewayStartedShards, logger);
    }

    /**
     * Allocate Batch of unassigned shard  to nodes where valid copies of the shard already exists
     *
     * @param shardRoutings the shards to allocate
     * @param allocation    the allocation state container object
     */
    public void allocateUnassignedBatch(List<ShardRouting> shardRoutings, RoutingAllocation allocation) {
        logger.trace("Starting shard allocation execution for unassigned primary shards: {}", shardRoutings.size());
        HashMap<ShardId, AllocateUnassignedDecision> ineligibleShardAllocationDecisions = new HashMap<>();
        List<ShardRouting> eligibleShards = new ArrayList<>();
        List<ShardRouting> inEligibleShards = new ArrayList<>();
        // identify ineligible shards
        for (ShardRouting shard : shardRoutings) {
            AllocateUnassignedDecision decision = getInEligibleShardDecision(shard, allocation);
            if (decision != null) {
                ineligibleShardAllocationDecisions.put(shard.shardId(), decision);
                inEligibleShards.add(shard);
            } else {
                eligibleShards.add(shard);
            }
        }

        // only fetch data for eligible shards
        final FetchResult<NodeGatewayStartedShardsBatch> shardsState = fetchData(eligibleShards, inEligibleShards, allocation);

        Set<ShardRouting> batchShardRoutingSet = new HashSet<>(shardRoutings);
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            ShardRouting unassignedShard = iterator.next();
            AllocateUnassignedDecision allocationDecision;

            if (unassignedShard.primary() && batchShardRoutingSet.contains(unassignedShard)) {
                if (ineligibleShardAllocationDecisions.containsKey(unassignedShard.shardId())) {
                    allocationDecision = ineligibleShardAllocationDecisions.get(unassignedShard.shardId());
                } else {
                    List<NodeGatewayStartedShard> nodeShardStates = adaptToNodeShardStates(unassignedShard, shardsState);
                    allocationDecision = getAllocationDecision(unassignedShard, allocation, nodeShardStates, logger);
                }
                executeDecision(unassignedShard, allocationDecision, allocation, iterator);
            }
        }
        logger.trace("Finished shard allocation execution for unassigned primary shards: {}", shardRoutings.size());
    }

    protected void allocateUnassignedBatchOnTimeout(List<ShardRouting> shardRoutings, RoutingAllocation allocation) {
        Set<ShardRouting> batchShardRoutingSet = new HashSet<>(shardRoutings);
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            ShardRouting unassignedShard = iterator.next();
            AllocateUnassignedDecision allocationDecision;
            if (unassignedShard.primary() && batchShardRoutingSet.contains(unassignedShard)) {
                allocationDecision = new AllocateUnassignedDecision(UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED, null, null, null, false, 0L, 0L);
                executeDecision(unassignedShard, allocationDecision, allocation, iterator);
            }
        }
    }

    /**
     * Transforms {@link FetchResult} of {@link NodeGatewayStartedShardsBatch} to {@link List} of {@link TransportNodesListGatewayStartedShards.NodeGatewayStartedShards}.
     * <p>
     * Returns null if {@link FetchResult} does not have any data.
     * <p>
     * shardsState contain the Data, there key is DiscoveryNode but value is Map of ShardId
     * and NodeGatewayStartedShardsBatch so to get one shard level data (from all the nodes), we'll traverse the map
     * and construct the nodeShardState along the way before making any allocation decision. As metadata for a
     * particular shard is needed from all the discovery nodes.
     *
     * @param unassignedShard unassigned shard
     * @param shardsState fetch data result for the whole batch
     * @return shard state returned from each node
     */
    private static List<NodeGatewayStartedShard> adaptToNodeShardStates(
        ShardRouting unassignedShard,
        FetchResult<NodeGatewayStartedShardsBatch> shardsState
    ) {
        if (!shardsState.hasData()) {
            return null;
        }
        List<NodeGatewayStartedShard> nodeShardStates = new ArrayList<>();
        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> nodeResponses = shardsState.getData();

        // build data for a shard from all the nodes
        nodeResponses.forEach((node, nodeGatewayStartedShardsBatch) -> {
            GatewayStartedShard shardData = nodeGatewayStartedShardsBatch.getNodeGatewayStartedShardsBatch().get(unassignedShard.shardId());
            nodeShardStates.add(
                new NodeGatewayStartedShard(
                    shardData.allocationId(),
                    shardData.primary(),
                    shardData.replicationCheckpoint(),
                    shardData.storeException(),
                    node
                )
            );
        });
        return nodeShardStates;
    }
}
