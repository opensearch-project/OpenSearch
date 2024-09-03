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
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.util.BatchRunnableExecutor;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.AsyncShardFetch;
import org.opensearch.gateway.PrimaryShardBatchAllocator;
import org.opensearch.gateway.ReplicaShardBatchAllocator;
import org.opensearch.gateway.ShardsBatchGatewayAllocator;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestShardBatchGatewayAllocator extends ShardsBatchGatewayAllocator {

    CountDownLatch latch;

    public TestShardBatchGatewayAllocator() {

    }

    public TestShardBatchGatewayAllocator(CountDownLatch latch, RerouteService rerouteService) {
        super(rerouteService);
        this.latch = latch;
    }

    public TestShardBatchGatewayAllocator(long maxBatchSize) {
        super(maxBatchSize);
    }

    Map<String /* node id */, Map<ShardId, ShardRouting>> knownAllocations = new HashMap<>();
    DiscoveryNodes currentNodes = DiscoveryNodes.EMPTY_NODES;
    Map<String, ReplicationCheckpoint> shardIdNodeToReplicationCheckPointMap = new HashMap<>();

    PrimaryShardBatchAllocator primaryBatchShardAllocator = new PrimaryShardBatchAllocator() {
        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> fetchData(
            List<ShardRouting> eligibleShards,
            List<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            Map<DiscoveryNode, TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> foundShards = new HashMap<>();
            HashMap<ShardId, Set<String>> shardsToIgnoreNodes = new HashMap<>();
            for (Map.Entry<String, Map<ShardId, ShardRouting>> entry : knownAllocations.entrySet()) {
                String nodeId = entry.getKey();
                Map<ShardId, ShardRouting> shardsOnNode = entry.getValue();
                HashMap<ShardId, TransportNodesGatewayStartedShardHelper.GatewayStartedShard> adaptedResponse = new HashMap<>();

                for (ShardRouting shardRouting : eligibleShards) {
                    ShardId shardId = shardRouting.shardId();
                    Set<String> ignoreNodes = allocation.getIgnoreNodes(shardId);

                    if (shardsOnNode.containsKey(shardId) && ignoreNodes.contains(nodeId) == false && currentNodes.nodeExists(nodeId)) {
                        TransportNodesGatewayStartedShardHelper.GatewayStartedShard nodeShard =
                            new TransportNodesGatewayStartedShardHelper.GatewayStartedShard(
                                shardsOnNode.get(shardId).allocationId().getId(),
                                shardsOnNode.get(shardId).primary(),
                                getReplicationCheckpoint(shardId, nodeId)
                            );
                        adaptedResponse.put(shardId, nodeShard);
                        shardsToIgnoreNodes.put(shardId, ignoreNodes);
                    }
                    foundShards.put(
                        currentNodes.get(nodeId),
                        new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch(
                            currentNodes.get(nodeId),
                            adaptedResponse
                        )
                    );
                }
            }
            return new AsyncShardFetch.FetchResult<>(foundShards, shardsToIgnoreNodes);
        }

        @Override
        protected void allocateUnassignedBatchOnTimeout(Set<ShardId> shardIds, RoutingAllocation allocation, boolean primary) {
            for (int i = 0; i < shardIds.size(); i++) {
                latch.countDown();
            }
        }
    };

    ReplicaShardBatchAllocator replicaBatchShardAllocator = new ReplicaShardBatchAllocator() {

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> fetchData(
            List<ShardRouting> eligibleShards,
            List<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            return new AsyncShardFetch.FetchResult<>(Collections.emptyMap(), Collections.emptyMap());
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return true;
        }

        @Override
        protected void allocateUnassignedBatchOnTimeout(Set<ShardId> shardIds, RoutingAllocation allocation, boolean primary) {
            for (int i = 0; i < shardIds.size(); i++) {
                latch.countDown();
            }
        }
    };

    @Override
    public BatchRunnableExecutor allocateAllUnassignedShards(RoutingAllocation allocation, boolean primary) {
        currentNodes = allocation.nodes();
        return innerAllocateUnassignedBatch(allocation, primaryBatchShardAllocator, replicaBatchShardAllocator, primary);
    }

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    public Set<String> createAndUpdateBatches(RoutingAllocation allocation, boolean primary) {
        return super.createAndUpdateBatches(allocation, primary);
    }

    public void safelyRemoveShardFromBatch(ShardRouting shard) {
        super.safelyRemoveShardFromBatch(shard, shard.primary());
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
