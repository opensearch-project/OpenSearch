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
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.gateway.AsyncShardFetch.FetchResult;
import org.opensearch.gateway.TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShard;
import org.opensearch.gateway.TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShardsBatch;

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
        Set<ShardRouting> shardsEligibleForFetch,
        Set<ShardRouting> inEligibleShards,
        RoutingAllocation allocation
    );

    protected FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(
        ShardRouting shard,
        RoutingAllocation allocation
    ) {
        logger.error("fetchData for single shard called via batch allocator");
        throw new IllegalStateException("PrimaryShardBatchAllocator should only be used for a batch of shards");
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard, RoutingAllocation allocation, Logger logger) {
        return makeAllocationDecision(new HashSet<>(Collections.singletonList(unassignedShard)), allocation, logger).get(unassignedShard);
    }

    /**
     * Build allocation decisions for all the shards present in the batch identified by batchId.
     *
     * @param shards     set of shards given for allocation
     * @param allocation current allocation of all the shards
     * @param logger     logger used for logging
     * @return shard to allocation decision map
     */
    @Override
    public HashMap<ShardRouting, AllocateUnassignedDecision> makeAllocationDecision(
        Set<ShardRouting> shards,
        RoutingAllocation allocation,
        Logger logger
    ) {
        HashMap<ShardRouting, AllocateUnassignedDecision> shardAllocationDecisions = new HashMap<>();
        Set<ShardRouting> eligibleShards = new HashSet<>();
        Set<ShardRouting> inEligibleShards = new HashSet<>();
        // identify ineligible shards
        for (ShardRouting shard : shards) {
            AllocateUnassignedDecision decision = getInEligibleShardDecision(shard, allocation);
            if (decision != null) {
                inEligibleShards.add(shard);
                shardAllocationDecisions.put(shard, decision);
            } else {
                eligibleShards.add(shard);
            }
        }
        // Do not call fetchData if there are no eligible shards
        if (eligibleShards.isEmpty()) {
            return shardAllocationDecisions;
        }
        // only fetch data for eligible shards
        final FetchResult<NodeGatewayStartedShardsBatch> shardsState = fetchData(eligibleShards, inEligibleShards, allocation);

        // process the received data
        for (ShardRouting unassignedShard : eligibleShards) {
            List<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> nodeShardStates = adaptToNodeShardStates(
                unassignedShard,
                shardsState
            );
            // get allocation decision for this shard
            shardAllocationDecisions.put(unassignedShard, getAllocationDecision(unassignedShard, allocation, nodeShardStates, logger));
        }
        return shardAllocationDecisions;
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
    private static List<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> adaptToNodeShardStates(
        ShardRouting unassignedShard,
        FetchResult<NodeGatewayStartedShardsBatch> shardsState
    ) {
        if (!shardsState.hasData()) {
            return null;
        }
        List<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> nodeShardStates = new ArrayList<>();
        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> nodeResponses = shardsState.getData();

        // build data for a shard from all the nodes
        nodeResponses.forEach((node, nodeGatewayStartedShardsBatch) -> {
            NodeGatewayStartedShard shardData = nodeGatewayStartedShardsBatch.getNodeGatewayStartedShardsBatch()
                .get(unassignedShard.shardId());
            nodeShardStates.add(
                new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(
                    node,
                    shardData.allocationId(),
                    shardData.primary(),
                    shardData.replicationCheckpoint(),
                    shardData.storeException()
                )
            );
        });
        return nodeShardStates;
    }
}
