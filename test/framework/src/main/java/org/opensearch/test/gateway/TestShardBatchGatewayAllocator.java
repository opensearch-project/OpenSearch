/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.gateway;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.AsyncShardFetch;
import org.opensearch.gateway.PrimaryShardBatchAllocator;
import org.opensearch.gateway.ReplicaShardBatchAllocator;
import org.opensearch.gateway.ShardsBatchGatewayAllocator;
import org.opensearch.gateway.TransportNodesListGatewayStartedBatchShards;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TestShardBatchGatewayAllocator extends ShardsBatchGatewayAllocator {

    Map<String /* node id */, Map<ShardId, ShardRouting>> knownAllocations = new HashMap<>();
    DiscoveryNodes currentNodes = DiscoveryNodes.EMPTY_NODES;
    Map<String, ReplicationCheckpoint> shardIdNodeToReplicationCheckPointMap = new HashMap<>();

    PrimaryShardBatchAllocator primaryBatchShardAllocator = new PrimaryShardBatchAllocator() {
        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShardsBatch> fetchData(
            Set<ShardRouting> shardsEligibleForFetch,
            Set<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            Map<DiscoveryNode, TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShardsBatch> foundShards = new HashMap<>();
            HashMap<ShardId, Set<String>> shardsToIgnoreNodes = new HashMap<>();
            for (Map.Entry<String, Map<ShardId, ShardRouting>> entry : knownAllocations.entrySet()) {
                String nodeId = entry.getKey();
                Map<ShardId, ShardRouting> shardsOnNode = entry.getValue();
                HashMap<ShardId, TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShards> adaptedResponse = new HashMap<>();

                for (ShardRouting shardRouting : shardsEligibleForFetch) {
                    ShardId shardId = shardRouting.shardId();
                    Set<String> ignoreNodes = allocation.getIgnoreNodes(shardId);

                    if (shardsOnNode.containsKey(shardId) && ignoreNodes.contains(nodeId) == false && currentNodes.nodeExists(nodeId)) {
                        TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShards nodeShard =
                            new TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShards(
                                shardRouting.allocationId().getId(),
                                shardRouting.primary(),
                                getReplicationCheckpoint(shardId, nodeId)
                            );
                        adaptedResponse.put(shardId, nodeShard);
                        shardsToIgnoreNodes.put(shardId, ignoreNodes);
                    }
                    foundShards.put(
                        currentNodes.get(nodeId),
                        new TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShardsBatch(
                            currentNodes.get(nodeId),
                            adaptedResponse
                        )
                    );
                }
            }
            return new AsyncShardFetch.FetchResult<>(foundShards, shardsToIgnoreNodes);
        }
    };

    ReplicaShardBatchAllocator replicaBatchShardAllocator = new ReplicaShardBatchAllocator() {

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> fetchData(
            Set<ShardRouting> shardsEligibleForFetch,
            Set<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            return new AsyncShardFetch.FetchResult<>(Collections.emptyMap(), Collections.emptyMap());
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return true;
        }
    };

    @Override
    public void allocateUnassignedBatch(RoutingAllocation allocation, boolean primary) {
        currentNodes = allocation.nodes();
        innerAllocateUnassignedBatch(allocation, primaryBatchShardAllocator, replicaBatchShardAllocator, primary);
    }

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    public Set<String> createAndUpdateBatches(RoutingAllocation allocation, boolean primary) {
        return super.createAndUpdateBatches(allocation, primary);
    }

    public void safelyRemoveShardFromBatch(ShardRouting shard) {
        super.safelyRemoveShardFromBatch(shard);
    }

    public void safelyRemoveShardFromBothBatch(ShardRouting shardRouting) {
        super.safelyRemoveShardFromBothBatch(shardRouting);
    }

    public String getBatchId(ShardRouting shard, boolean primary) {
        return super.getBatchId(shard, primary);
    }

    public Map<String, ShardsBatchGatewayAllocator.ShardsBatch> getBatchIdToStartedShardBatch() {
        return batchIdToStartedShardBatch;
    }

    public Map<String, ShardsBatchGatewayAllocator.ShardsBatch> getBatchIdToStoreShardBatch() {
        return batchIdToStoreShardBatch;
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        return super.explainUnassignedShardAllocation(unassignedShard, routingAllocation);
    }

    protected ReplicationCheckpoint getReplicationCheckpoint(ShardId shardId, String nodeName) {
        return shardIdNodeToReplicationCheckPointMap.getOrDefault(getReplicationCheckPointKey(shardId, nodeName), null);
    }

    public String getReplicationCheckPointKey(ShardId shardId, String nodeName) {
        return shardId.toString() + "_" + nodeName;
    }
}
