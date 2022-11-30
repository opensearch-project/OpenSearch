/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import com.carrotsearch.hppc.ObjectIntHashMap;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.opensearch.common.Randomness;
import org.opensearch.cluster.routing.RecoverySource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A {@link RemoteShardsBalancer} used by the {@link BalancedShardsAllocator} to perform allocation operations
 * for remote shards within the cluster.
 *
 * @opensearch.internal
 */
public final class RemoteShardsBalancer extends ShardsBalancer {
    private final Logger logger;
    private final RoutingAllocation allocation;
    private final RoutingNodes routingNodes;

    public RemoteShardsBalancer(Logger logger, RoutingAllocation allocation) {
        this.logger = logger;
        this.allocation = allocation;
        this.routingNodes = allocation.routingNodes();
    }

    /**
     * Allocates unassigned remote shards on the routing node which are filtered using
     * {@link #groupUnassignedShardsByIndex}
     */
    @Override
    void allocateUnassigned() {
        unassignIgnoredRemoteShards(allocation);
        if (routingNodes.unassigned().isEmpty()) {
            logger.debug("No unassigned remote shards found.");
            return;
        }

        Queue<RoutingNode> nodeQueue = getShuffledRemoteNodes();
        if (nodeQueue.isEmpty()) {
            logger.debug("No remote searcher nodes available for unassigned remote shards.");
            failUnattemptedShards();
            return;
        }

        Map<String, UnassignedIndexShards> unassignedShardMap = groupUnassignedShardsByIndex();
        allocateUnassignedPrimaries(nodeQueue, unassignedShardMap);
        allocateUnassignedReplicas(nodeQueue, unassignedShardMap);
        ignoreRemainingShards(unassignedShardMap);
    }

    /**
     * Performs shard movement for incompatible remote shards
     */
    @Override
    void moveShards() {
        Queue<RoutingNode> eligibleNodes = new ArrayDeque<>();
        Queue<RoutingNode> excludedNodes = new ArrayDeque<>();
        classifyNodesForShardMovement(eligibleNodes, excludedNodes);

        if (excludedNodes.isEmpty()) {
            logger.debug("No excluded nodes found. Returning...");
            return;
        }

        while (!eligibleNodes.isEmpty() && !excludedNodes.isEmpty()) {
            RoutingNode sourceNode = excludedNodes.poll();
            for (ShardRouting ineligibleShard : sourceNode) {
                if (ineligibleShard.started() == false) {
                    continue;
                }

                if (!RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(ineligibleShard, allocation))) {
                    continue;
                }

                if (eligibleNodes.isEmpty()) {
                    break;
                }

                tryShardMovementToEligibleNode(eligibleNodes, ineligibleShard);
            }
        }
    }

    /**
     * Classifies the nodes into eligible and excluded depending on whether node is able or unable for shard assignment
     * @param eligibleNodes contains the list of classified nodes eligible to accept shards
     * @param excludedNodes contains the list of classified nodes that are unable for assigning shards
     */
    private void classifyNodesForShardMovement(Queue<RoutingNode> eligibleNodes, Queue<RoutingNode> excludedNodes) {
        List<RoutingNode> remoteRoutingNodes = getRemoteRoutingNodes();
        int throttledNodeCount = 0;
        for (RoutingNode node : remoteRoutingNodes) {
            Decision nodeDecision = allocation.deciders().canAllocateAnyShardToNode(node, allocation);
            /* canAllocateAnyShardToNode decision can be THROTTLE for throttled nodes. To classify
             * as excluded nodes, we look for Decision.Type.NO
             */
            if (nodeDecision.type() == Decision.Type.NO) {
                excludedNodes.add(node);
            } else if (nodeDecision.type() == Decision.Type.YES) {
                eligibleNodes.add(node);
            } else {
                throttledNodeCount++;
            }
            logger.debug(
                "Excluded Node Count: [{}], Eligible Node Count: [{}], Throttled Node Count: [{}]",
                excludedNodes.size(),
                eligibleNodes.size(),
                throttledNodeCount
            );
        }
    }

    /**
     * Tries to move a shard assigned to an excluded node to an eligible node.
     *
     * @param eligibleNodes set of nodes that are still accepting shards
     * @param shard the ineligible shard to be moved
     */
    private void tryShardMovementToEligibleNode(Queue<RoutingNode> eligibleNodes, ShardRouting shard) {
        Set<String> nodesCheckedForShard = new HashSet<>();
        while (!eligibleNodes.isEmpty()) {
            RoutingNode targetNode = eligibleNodes.poll();
            Decision currentShardDecision = allocation.deciders().canAllocate(shard, targetNode, allocation);

            if (currentShardDecision.type() == Decision.Type.YES) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Moving shard: {} from node: [{}] to node: [{}]",
                        shardShortSummary(shard),
                        shard.currentNodeId(),
                        targetNode.nodeId()
                    );
                }
                routingNodes.relocateShard(
                    shard,
                    targetNode.nodeId(),
                    allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    allocation.changes()
                );
                eligibleNodes.offer(targetNode);
                break;
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Cannot move shard: {} to node: [{}]. Decisions: [{}]",
                        shardShortSummary(shard),
                        targetNode.nodeId(),
                        currentShardDecision.getDecisions()
                    );
                }

                Decision nodeLevelDecision = allocation.deciders().canAllocateAnyShardToNode(targetNode, allocation);
                if (nodeLevelDecision.type() == Decision.Type.YES) {
                    logger.debug("Node: [{}] can still accept shards. Adding it back to the queue.", targetNode.nodeId());
                    eligibleNodes.offer(targetNode);
                    nodesCheckedForShard.add(targetNode.nodeId());
                } else {
                    logger.debug("Node: [{}] cannot accept any more shards. Removing it from queue.", targetNode.nodeId());
                }

                // Break out if all nodes in the queue have been checked for this shard
                if (eligibleNodes.stream().allMatch(rn -> nodesCheckedForShard.contains(rn.nodeId()))) {
                    break;
                }
            }
        }
    }

    /**
     * Performs heuristic, naive weight-based balancing for remote shards within the cluster by using average nodes per
     * cluster as the metric for shard distribution.
     * It does so without accounting for the local shards located on any nodes within the cluster.
     */
    @Override
    void balance() {
        List<RoutingNode> remoteRoutingNodes = getRemoteRoutingNodes();
        logger.trace("Performing balancing for remote shards.");

        if (remoteRoutingNodes.isEmpty()) {
            logger.info("No eligible remote nodes found to perform balancing");
            return;
        }

        ObjectIntHashMap<String> nodePrimaryShardCount = calculateNodePrimaryShardCount(remoteRoutingNodes);
        int totalPrimaryShardCount = Arrays.stream(nodePrimaryShardCount.values).sum();

        totalPrimaryShardCount += routingNodes.unassigned().getNumPrimaries();
        int avgPrimaryPerNode = (totalPrimaryShardCount + routingNodes.size() - 1) / routingNodes.size();

        ArrayDeque<RoutingNode> sourceNodes = new ArrayDeque<>();
        ArrayDeque<RoutingNode> targetNodes = new ArrayDeque<>();
        for (RoutingNode node : remoteRoutingNodes) {
            if (nodePrimaryShardCount.get(node.nodeId()) > avgPrimaryPerNode) {
                sourceNodes.add(node);
            } else if (nodePrimaryShardCount.get(node.nodeId()) < avgPrimaryPerNode) {
                targetNodes.add(node);
            }
        }

        while (!sourceNodes.isEmpty() && !targetNodes.isEmpty()) {
            RoutingNode sourceNode = sourceNodes.poll();
            tryRebalanceNode(sourceNode, targetNodes, avgPrimaryPerNode, nodePrimaryShardCount);
        }
    }

    /**
     * Calculates the total number of primary shards per node.
     * @param remoteRoutingNodes routing nodes for which the aggregation needs to be performed
     * @return map of node id to primary shard count
     */
    private ObjectIntHashMap<String> calculateNodePrimaryShardCount(List<RoutingNode> remoteRoutingNodes) {
        ObjectIntHashMap<String> primaryShardCount = new ObjectIntHashMap<>();
        for (RoutingNode node : remoteRoutingNodes) {
            int totalPrimaryShardsPerNode = 0;
            for (ShardRouting shard : node) {
                if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation))
                    && shard.primary()
                    && (shard.initializing() || shard.started())) {
                    totalPrimaryShardsPerNode++;
                }
            }
            primaryShardCount.put(node.nodeId(), totalPrimaryShardsPerNode);
        }
        return primaryShardCount;
    }

    @Override
    AllocateUnassignedDecision decideAllocateUnassigned(ShardRouting shardRouting) {
        throw new UnsupportedOperationException("remote shards balancer does not support decision operations");
    }

    @Override
    MoveDecision decideMove(ShardRouting shardRouting) {
        throw new UnsupportedOperationException("remote shards balancer does not support decision operations");
    }

    @Override
    MoveDecision decideRebalance(ShardRouting shardRouting) {
        throw new UnsupportedOperationException("remote shards balancer does not support decision operations");
    }

    /**
     * Groups unassigned shards within the allocation based on the index.
     * @return {@link UnassignedIndexShards} grouped by index name
     */
    public Map<String, UnassignedIndexShards> groupUnassignedShardsByIndex() {
        HashMap<String, UnassignedIndexShards> unassignedShardMap = new HashMap<>();
        for (ShardRouting shard : routingNodes.unassigned().drain()) {
            String index = shard.getIndexName();
            if (!RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation))) {
                routingNodes.unassigned().add(shard);
                continue;
            }
            if (!unassignedShardMap.containsKey(index)) {
                unassignedShardMap.put(index, new UnassignedIndexShards());
            }
            unassignedShardMap.get(index).addShard(shard);
        }
        return unassignedShardMap;
    }

    /**
     * Unassigned shards from {@link LocalShardsBalancer} are ignored since the balancer cannot allocate remote shards.
     * Prior to allocation operations done by {@link RemoteShardsBalancer}, the ignored remote shards are moved back to
     * unassigned status.
     */
    private void unassignIgnoredRemoteShards(RoutingAllocation routingAllocation) {
        RoutingNodes.UnassignedShards unassignedShards = routingAllocation.routingNodes().unassigned();
        for (ShardRouting shard : unassignedShards.drainIgnored()) {
            RoutingPool pool = RoutingPool.getShardPool(shard, routingAllocation);
            if (pool == RoutingPool.REMOTE_CAPABLE && shard.unassigned() && (shard.primary() || !shard.unassignedInfo().isDelayed())) {
                ShardRouting unassignedShard = shard;
                // Shard when moved to an unassigned state updates the recovery source to be ExistingStoreRecoverySource
                // Remote shards do not have an existing store to recover from and can be recovered from an empty source
                // to re-fetch any shard blocks from the repository.
                if (shard.primary()) {
                    if (!RecoverySource.Type.SNAPSHOT.equals(shard.recoverySource().getType())) {
                        unassignedShard = shard.updateUnassigned(shard.unassignedInfo(), RecoverySource.EmptyStoreRecoverySource.INSTANCE);
                    }
                }

                unassignedShards.add(unassignedShard);
            } else {
                unassignedShards.ignoreShard(shard, shard.unassignedInfo().getLastAllocationStatus(), routingAllocation.changes());
            }
        }
    }

    private void allocateUnassignedPrimaries(Queue<RoutingNode> nodeQueue, Map<String, UnassignedIndexShards> unassignedShardMap) {
        allocateUnassignedShards(true, nodeQueue, unassignedShardMap);
    }

    private void allocateUnassignedReplicas(Queue<RoutingNode> nodeQueue, Map<String, UnassignedIndexShards> unassignedShardMap) {
        allocateUnassignedShards(false, nodeQueue, unassignedShardMap);
    }

    private void ignoreRemainingShards(Map<String, UnassignedIndexShards> unassignedShardMap) {
        for (UnassignedIndexShards indexShards : unassignedShardMap.values()) {
            for (ShardRouting shard : indexShards.getPrimaries()) {
                routingNodes.unassigned().ignoreShard(shard, UnassignedInfo.AllocationStatus.DECIDERS_NO, allocation.changes());
            }
            for (ShardRouting shard : indexShards.getReplicas()) {
                routingNodes.unassigned().ignoreShard(shard, UnassignedInfo.AllocationStatus.DECIDERS_NO, allocation.changes());
            }
        }
    }

    private void allocateUnassignedShards(
        boolean primaries,
        Queue<RoutingNode> nodeQueue,
        Map<String, UnassignedIndexShards> unassignedShardMap
    ) {
        logger.debug("Allocating unassigned {}. Nodes available in queue: [{}]", (primaries ? "primaries" : "replicas"), nodeQueue.size());

        // Iterate through all shards index by index and allocate them
        for (String index : unassignedShardMap.keySet()) {
            if (nodeQueue.isEmpty()) {
                break;
            }

            UnassignedIndexShards indexShards = unassignedShardMap.get(index);
            Queue<ShardRouting> shardsToAllocate = primaries ? indexShards.getPrimaries() : indexShards.getReplicas();
            if (shardsToAllocate.isEmpty()) {
                continue;
            }
            logger.debug("Allocating shards for index: [{}]", index);

            while (!shardsToAllocate.isEmpty() && !nodeQueue.isEmpty()) {
                ShardRouting shard = shardsToAllocate.poll();
                if (shard.assignedToNode()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Shard: {} already assigned to node: [{}]", shardShortSummary(shard), shard.currentNodeId());
                    }
                    continue;
                }

                Decision shardLevelDecision = allocation.deciders().canAllocate(shard, allocation);
                if (shardLevelDecision.type() == Decision.Type.NO) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Ignoring shard: [{}] as is cannot be allocated to any node. Shard level decisions: [{}][{}].",
                            shardShortSummary(shard),
                            shardLevelDecision.getDecisions(),
                            shardLevelDecision.getExplanation()
                        );
                    }
                    routingNodes.unassigned().ignoreShard(shard, UnassignedInfo.AllocationStatus.DECIDERS_NO, allocation.changes());
                    continue;
                }

                tryAllocateUnassignedShard(nodeQueue, shard);
            }
        }
    }

    /**
     * Tries to allocate an unassigned shard to one of the nodes within the node queue.
     * @param nodeQueue ordered list of nodes to try allocation
     * @param shard the unassigned shard which needs to be allocated
     */
    private void tryAllocateUnassignedShard(Queue<RoutingNode> nodeQueue, ShardRouting shard) {
        boolean allocated = false;
        boolean throttled = false;
        Set<String> nodesCheckedForShard = new HashSet<>();
        while (!nodeQueue.isEmpty()) {
            RoutingNode node = nodeQueue.poll();
            Decision allocateDecision = allocation.deciders().canAllocate(shard, node, allocation);
            nodesCheckedForShard.add(node.nodeId());
            if (allocateDecision.type() == Decision.Type.YES) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Assigned shard [{}] to [{}]", shardShortSummary(shard), node.nodeId());
                }
                final long shardSize = DiskThresholdDecider.getExpectedShardSize(
                    shard,
                    ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                    allocation.clusterInfo(),
                    allocation.snapshotShardSizeInfo(),
                    allocation.metadata(),
                    allocation.routingTable()
                );
                ShardRouting initShard = routingNodes.initializeShard(shard, node.nodeId(), null, shardSize, allocation.changes());
                nodeQueue.offer(node);
                allocated = true;
                break;
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Cannot allocate shard: {} on node [{}]. Decisions: [{}]",
                        shardShortSummary(shard),
                        node.nodeId(),
                        allocateDecision.getDecisions()
                    );
                }
                throttled = throttled || allocateDecision.type() == Decision.Type.THROTTLE;

                Decision nodeLevelDecision = allocation.deciders().canAllocateAnyShardToNode(node, allocation);
                if (nodeLevelDecision.type() == Decision.Type.YES) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Node: [{}] can still accept shards, retaining it in queue - [{}]",
                            node.nodeId(),
                            nodeLevelDecision.getDecisions()
                        );
                    }
                    nodeQueue.offer(node);
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Cannot allocate any shard to node: [{}]. Removing from queue. Node level decisions: [{}],[{}]",
                            node.nodeId(),
                            nodeLevelDecision.getDecisions(),
                            nodeLevelDecision.getExplanation()
                        );
                    }
                }

                // Break out if all nodes in the queue have been checked for this shard
                if (nodeQueue.stream().allMatch(rn -> nodesCheckedForShard.contains(rn.nodeId()))) {
                    throttled = true;
                    break;
                }
            }
        }

        if (!allocated) {
            UnassignedInfo.AllocationStatus status = throttled
                ? UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED
                : UnassignedInfo.AllocationStatus.DECIDERS_NO;
            routingNodes.unassigned().ignoreShard(shard, status, allocation.changes());
        }
    }

    private void tryRebalanceNode(
        RoutingNode sourceNode,
        ArrayDeque<RoutingNode> targetNodes,
        int avgPrimary,
        ObjectIntHashMap<String> primaryCount
    ) {
        long shardsToBalance = primaryCount.get(sourceNode.nodeId()) - avgPrimary;
        assert shardsToBalance >= 0 : "Shards to balance should be greater than 0, but found negative";
        Iterator<ShardRouting> shardIterator = sourceNode.copyShards().iterator();
        Set<String> nodesCheckedForRelocation = new HashSet<>();

        // Try to relocate the valid shards on the sourceNode, one at a time;
        // until either sourceNode is balanced OR no more active primary shard available OR all the target nodes are exhausted
        while (shardsToBalance > 0 && shardIterator.hasNext() && !targetNodes.isEmpty()) {
            // Find an active primary shard to relocate
            ShardRouting shard = shardIterator.next();
            if (!shard.started() || !shard.primary() || !RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation))) {
                continue;
            }

            while (!targetNodes.isEmpty()) {
                // Find a valid target node that can accommodate the current shard relocation
                RoutingNode targetNode = targetNodes.poll();
                if (primaryCount.get(targetNode.nodeId()) >= avgPrimary) {
                    logger.trace("Avg shard limit reached for node: [{}]. Removing from queue.", targetNode.nodeId());
                    continue;
                }

                // Try relocate the shard on the target node
                Decision rebalanceDecision = tryRelocateShard(shard, targetNode);

                if (rebalanceDecision.type() == Decision.Type.YES) {
                    shardsToBalance--;
                    primaryCount.addTo(targetNode.nodeId(), 1);
                    targetNodes.offer(targetNode);
                    break;

                    // If the relocation attempt failed for the shard, check if the target node can accommodate any other shard; else remove
                    // the target node from the target list
                } else {
                    Decision nodeDecision = allocation.deciders().canAllocateAnyShardToNode(targetNode, allocation);
                    if (nodeDecision.type() == Decision.Type.YES) {
                        targetNodes.offer(targetNode);
                        nodesCheckedForRelocation.add(targetNode.nodeId());
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                "Cannot allocate any shard to node: [{}]. Removing from queue. Node level decisions: [{}],[{}]",
                                targetNode.nodeId(),
                                nodeDecision.getDecisions(),
                                nodeDecision.toString()
                            );
                        }
                    }
                }

                // If all the target nodes are exhausted for the current shard; skip to next shard
                if (targetNodes.stream().allMatch(node -> nodesCheckedForRelocation.contains(node.nodeId()))) {
                    break;
                }
            }
        }
    }

    /**
     * For every primary shard for which this method is invoked,
     * swap is attempted with the destination node in case replica shard is present.
     * In case replica is not present, relocation of the shard id performed.
     */
    private Decision tryRelocateShard(ShardRouting shard, RoutingNode destinationNode) {
        // Check if there is already a replica for the shard on the destination node.
        // Then we can directly swap the replica with the primary shards.
        // Invariant: We only allow swap relocation on remote shards.
        ShardRouting replicaShard = destinationNode.getByShardId(shard.shardId());
        if (replicaShard != null) {
            assert !replicaShard.primary() : "Primary Shard found while expected Replica during shard rebalance";
            return executeSwapShard(shard, replicaShard, allocation);
        }

        // Since no replica present on the destinationNode; try relocating the shard to the destination node
        Decision allocationDecision = allocation.deciders().canAllocate(shard, destinationNode, allocation);
        Decision rebalanceDecision = allocation.deciders().canRebalance(shard, allocation);
        logger.trace(
            "Relocating shard [{}] from node [{}] to node [{}]. AllocationDecision: [{}]. AllocationExplanation: [{}]. "
                + "RebalanceDecision: [{}]. RebalanceExplanation: [{}]",
            shard.id(),
            shard.currentNodeId(),
            destinationNode.nodeId(),
            allocationDecision.type(),
            allocationDecision.toString(),
            rebalanceDecision.type(),
            rebalanceDecision.toString()
        );

        // Perform the relocation of allocation and rebalance decisions are YES
        if ((allocationDecision.type() == Decision.Type.YES) && (rebalanceDecision.type() == Decision.Type.YES)) {
            final long shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            ShardRouting targetShard = routingNodes.relocateShard(shard, destinationNode.nodeId(), shardSize, allocation.changes()).v2();
            logger.info("Relocated shard [{}] to node [{}] during primary Rebalance", shard, targetShard.currentNodeId());
            return Decision.YES;
        }

        if ((allocationDecision.type() == Decision.Type.THROTTLE) || (rebalanceDecision.type() == Decision.Type.THROTTLE)) {
            return Decision.THROTTLE;
        }

        return Decision.NO;
    }

    private Decision executeSwapShard(ShardRouting primaryShard, ShardRouting replicaShard, RoutingAllocation allocation) {
        if (!replicaShard.started()) {
            return new Decision.Single(Decision.Type.NO);
        }

        allocation.routingNodes().swapPrimaryWithReplica(logger, primaryShard, replicaShard, allocation.changes());
        return new Decision.Single(Decision.Type.YES);
    }

    private void failUnattemptedShards() {
        RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shard.unassignedInfo();
            if (shard.primary() && unassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT) {
                unassignedIterator.updateUnassigned(
                    new UnassignedInfo(
                        unassignedInfo.getReason(),
                        unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(),
                        unassignedInfo.getNumFailedAllocations(),
                        unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(),
                        unassignedInfo.isDelayed(),
                        UnassignedInfo.AllocationStatus.DECIDERS_NO,
                        Collections.emptySet()
                    ),
                    shard.recoverySource(),
                    allocation.changes()
                );
            }
        }
    }

    private Queue<RoutingNode> getShuffledRemoteNodes() {
        List<RoutingNode> nodeList = getRemoteRoutingNodes();
        Randomness.shuffle(nodeList);
        return new ArrayDeque<>(nodeList);
    }

    /**
     * Filters out and returns the list of {@link RoutingPool#REMOTE_CAPABLE} nodes from the routing nodes in cluster.
     * @return list of {@link RoutingPool#REMOTE_CAPABLE} routing nodes.
     */
    private List<RoutingNode> getRemoteRoutingNodes() {
        List<RoutingNode> nodeList = new ArrayList<>();
        for (RoutingNode rNode : routingNodes) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getNodePool(rNode))) {
                nodeList.add(rNode);
            }
        }
        return nodeList;
    }

    /**
     * {@link UnassignedIndexShards} maintains a queue of unassigned remote shards for allocation operations within
     * the cluster.
     *
     * @opensearch.internal
     */
    public static class UnassignedIndexShards {
        private final Queue<ShardRouting> primaries = new ArrayDeque<>();
        private final Queue<ShardRouting> replicas = new ArrayDeque<>();

        public void addShard(ShardRouting shard) {
            if (shard.primary()) {
                primaries.add(shard);
            } else {
                replicas.add(shard);
            }
        }

        public Queue<ShardRouting> getPrimaries() {
            return primaries;
        }

        public Queue<ShardRouting> getReplicas() {
            return replicas;
        }
    }

    private String shardShortSummary(ShardRouting shard) {
        return "[" + shard.getIndexName() + "]" + "[" + shard.getId() + "]" + "[" + (shard.primary() ? "p" : "r") + "]";
    }

}
