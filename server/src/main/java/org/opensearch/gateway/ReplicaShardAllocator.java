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

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult.ShardStoreInfo;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

/**
 * Allocates replica shards
 *
 * @opensearch.internal
 */
public abstract class ReplicaShardAllocator extends BaseGatewayShardAllocator {
    protected boolean shouldSkipFetchForRecovery(ShardRouting shard) {
        if (shard.primary()) {
            return true;
        }
        if (shard.initializing() == false) {
            return true;
        }
        if (shard.relocatingNodeId() != null) {
            return true;
        }
        if (shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
            return true;
        }
        return false;
    }

    protected Runnable cancelExistingRecoveryForBetterMatch(
        ShardRouting shard,
        RoutingAllocation allocation,
        Map<DiscoveryNode, StoreFilesMetadata> nodeShardStores
    ) {
        if (nodeShardStores == null) {
            logger.trace("{}: fetching new stores for initializing shard", shard);
            return null;
        }
        Metadata metadata = allocation.metadata();
        RoutingNodes routingNodes = allocation.routingNodes();
        ShardRouting primaryShard = allocation.routingNodes().activePrimary(shard.shardId());
        if (primaryShard == null) {
            logger.trace("{}: no active primary shard found or allocated, letting actual allocation figure it out", shard);
            return null;
        }
        assert primaryShard.currentNodeId() != null;
        final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());

        final StoreFilesMetadata primaryStore = findStore(primaryNode, nodeShardStores);
        if (primaryStore == null) {
            // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
            // just let the recovery find it out, no need to do anything about it for the initializing shard
            logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
            return null;
        }

        MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, true, primaryNode, primaryStore, nodeShardStores, false);
        if (matchingNodes.getNodeWithHighestMatch() != null) {
            DiscoveryNode currentNode = allocation.nodes().get(shard.currentNodeId());
            DiscoveryNode nodeWithHighestMatch = matchingNodes.getNodeWithHighestMatch();
            // current node will not be in matchingNodes as it is filtered away by SameShardAllocationDecider
            if (currentNode.equals(nodeWithHighestMatch) == false
                && matchingNodes.canPerformNoopRecovery(nodeWithHighestMatch)
                && canPerformOperationBasedRecovery(primaryStore, nodeShardStores, currentNode) == false) {
                // we found a better match that can perform noop recovery, cancel the existing allocation.
                logger.debug(
                    "cancelling allocation of replica on [{}], can perform a noop recovery on node [{}]",
                    currentNode,
                    nodeWithHighestMatch
                );
                final Set<String> failedNodeIds = shard.unassignedInfo() == null
                    ? Collections.emptySet()
                    : shard.unassignedInfo().getFailedNodeIds();
                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    UnassignedInfo.Reason.REALLOCATED_REPLICA,
                    "existing allocation of replica to ["
                        + currentNode
                        + "] cancelled, can perform a noop recovery on ["
                        + nodeWithHighestMatch
                        + "]",
                    null,
                    0,
                    allocation.getCurrentNanoTime(),
                    System.currentTimeMillis(),
                    false,
                    UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                    failedNodeIds
                );
                // don't cancel shard in the loop as it will cause a ConcurrentModificationException
                return () -> routingNodes.failShard(
                    logger,
                    shard,
                    unassignedInfo,
                    metadata.getIndexSafe(shard.index()),
                    allocation.changes()
                );
            }
        }
        return null;
    }

    /**
     * Process existing recoveries of replicas and see if we need to cancel them if we find a better
     * match. Today, a better match is one that can perform a no-op recovery while the previous recovery
     * has to copy segment files.
     */
    public void processExistingRecoveries(RoutingAllocation allocation) {
        RoutingNodes routingNodes = allocation.routingNodes();
        List<Runnable> shardCancellationActions = new ArrayList<>();
        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shard : routingNode) {
                if (shouldSkipFetchForRecovery(shard)) {
                    continue;
                }

                AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetchData(shard, allocation);
                Map<DiscoveryNode, StoreFilesMetadata> nodeShardStores = convertToNodeStoreFilesMetadataMap(shardStores);

                Runnable cancellationAction = cancelExistingRecoveryForBetterMatch(shard, allocation, nodeShardStores);
                if (cancellationAction != null) {
                    shardCancellationActions.add(cancellationAction);
                }
            }
        }
        for (Runnable action : shardCancellationActions) {
            action.run();
        }
    }

    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     */
    protected boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() == false // must be a replica
            && shard.unassigned() // must be unassigned
            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
            && shard.unassignedInfo().getReason() != UnassignedInfo.Reason.INDEX_CREATED;
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(
        final ShardRouting unassignedShard,
        final RoutingAllocation allocation,
        final Logger logger
    ) {
        if (isResponsibleFor(unassignedShard) == false) {
            // this allocator is not responsible for deciding on this shard
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
        Tuple<Decision, Map<String, NodeAllocationResult>> result = canBeAllocatedToAtLeastOneNode(unassignedShard, allocation);
        Decision allocateDecision = result.v1();
        if (allocateDecision.type() != Decision.Type.YES
            && (allocation.debugDecision() == false || hasInitiatedFetching(unassignedShard) == false)) {
            // only return early if we are not in explain mode, or we are in explain mode but we have not
            // yet attempted to fetch any shard data
            logger.trace("{}: ignoring allocation, can't be allocated on any node", unassignedShard);
            return AllocateUnassignedDecision.no(
                UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()),
                result.v2() != null ? new ArrayList<>(result.v2().values()) : null
            );
        }

        AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetchData(unassignedShard, allocation);
        Map<DiscoveryNode, StoreFilesMetadata> nodeShardStores = convertToNodeStoreFilesMetadataMap(shardStores);
        return getAllocationDecision(unassignedShard, allocation, nodeShardStores, result, logger);
    }

    protected AllocateUnassignedDecision getAllocationDecision(
        ShardRouting unassignedShard,
        RoutingAllocation allocation,
        Map<DiscoveryNode, StoreFilesMetadata> nodeShardStores,
        Tuple<Decision, Map<String, NodeAllocationResult>> allocationDecision,
        Logger logger
    ) {
        if (nodeShardStores == null) {
            // node shard stores is null when we don't have data yet and still fetching the shard stores
            logger.trace("{}: ignoring allocation, still fetching shard stores", unassignedShard);
            allocation.setHasPendingAsyncFetch();
            List<NodeAllocationResult> nodeDecisions = null;
            if (allocation.debugDecision()) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }
        final RoutingNodes routingNodes = allocation.routingNodes();
        final boolean explain = allocation.debugDecision();
        ShardRouting primaryShard = routingNodes.activePrimary(unassignedShard.shardId());
        if (primaryShard == null) {
            assert explain : "primary should only be null here if we are in explain mode, so we didn't "
                + "exit early when canBeAllocatedToAtLeastOneNode didn't return a YES decision";
            return AllocateUnassignedDecision.no(
                UnassignedInfo.AllocationStatus.fromDecision(allocationDecision.v1().type()),
                new ArrayList<>(allocationDecision.v2().values())
            );
        }
        assert primaryShard.currentNodeId() != null;
        final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
        final StoreFilesMetadata primaryStore = findStore(primaryNode, nodeShardStores);
        if (primaryStore == null) {
            // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
            // we want to let the replica be allocated in order to expose the actual problem with the primary that the replica
            // will try and recover from
            // Note, this is the existing behavior, as exposed in running CorruptFileTest#testNoPrimaryData
            logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", unassignedShard);
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        MatchingNodes matchingNodes = findMatchingNodes(
            unassignedShard,
            allocation,
            false,
            primaryNode,
            primaryStore,
            nodeShardStores,
            explain
        );
        assert explain == false || matchingNodes.nodeDecisions != null : "in explain mode, we must have individual node decisions";

        List<NodeAllocationResult> nodeDecisions = augmentExplanationsWithStoreInfo(allocationDecision.v2(), matchingNodes.nodeDecisions);
        if (allocationDecision.v1().type() != Decision.Type.YES) {
            return AllocateUnassignedDecision.no(
                UnassignedInfo.AllocationStatus.fromDecision(allocationDecision.v1().type()),
                nodeDecisions
            );
        } else if (matchingNodes.getNodeWithHighestMatch() != null) {
            RoutingNode nodeWithHighestMatch = allocation.routingNodes().node(matchingNodes.getNodeWithHighestMatch().getId());
            // we only check on THROTTLE since we checked before on NO
            Decision decision = allocation.deciders().canAllocate(unassignedShard, nodeWithHighestMatch, allocation);
            if (decision.type() == Decision.Type.THROTTLE) {
                logger.debug(
                    "[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard,
                    nodeWithHighestMatch.node()
                );
                // we are throttling this, as we have enough other shards to allocate to this node, so ignore it for now
                return AllocateUnassignedDecision.throttle(nodeDecisions);
            } else {
                logger.debug(
                    "[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard,
                    nodeWithHighestMatch.node()
                );
                // we found a match
                return AllocateUnassignedDecision.yes(nodeWithHighestMatch.node(), null, nodeDecisions, true);
            }
        } else if (matchingNodes.hasAnyData() == false && unassignedShard.unassignedInfo().isDelayed()) {
            // if we didn't manage to find *any* data (regardless of matching sizes), and the replica is
            // unassigned due to a node leaving, so we delay allocation of this replica to see if the
            // node with the shard copy will rejoin so we can re-use the copy it has
            logger.debug("{}: allocation of [{}] is delayed", unassignedShard.shardId(), unassignedShard);
            long remainingDelayMillis = 0L;
            long totalDelayMillis = 0L;
            if (explain) {
                UnassignedInfo unassignedInfo = unassignedShard.unassignedInfo();
                Metadata metadata = allocation.metadata();
                IndexMetadata indexMetadata = metadata.index(unassignedShard.index());
                totalDelayMillis = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetadata.getSettings()).getMillis();
                long remainingDelayNanos = unassignedInfo.getRemainingDelay(System.nanoTime(), indexMetadata.getSettings());
                remainingDelayMillis = TimeValue.timeValueNanos(remainingDelayNanos).millis();
            }
            return AllocateUnassignedDecision.delayed(remainingDelayMillis, totalDelayMillis, nodeDecisions);
        }

        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    /**
     * Determines if the shard can be allocated on at least one node based on the allocation deciders.
     * <p>
     * Returns the best allocation decision for allocating the shard on any node (i.e. YES if at least one
     * node decided YES, THROTTLE if at least one node decided THROTTLE, and NO if none of the nodes decided
     * YES or THROTTLE).  If in explain mode, also returns the node-level explanations as the second element
     * in the returned tuple.
     */
    protected static Tuple<Decision, Map<String, NodeAllocationResult>> canBeAllocatedToAtLeastOneNode(
        ShardRouting shard,
        RoutingAllocation allocation
    ) {
        Decision madeDecision = Decision.NO;
        final boolean explain = allocation.debugDecision();
        Map<String, NodeAllocationResult> nodeDecisions = explain ? new HashMap<>() : null;
        for (final DiscoveryNode cursor : allocation.nodes().getDataNodes().values()) {
            RoutingNode node = allocation.routingNodes().node(cursor.getId());
            if (node == null) {
                continue;
            }
            // if we can't allocate it on a node, ignore it, for example, this handles
            // cases for only allocating a replica after a primary
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            if (decision.type() == Decision.Type.YES && madeDecision.type() != Decision.Type.YES) {
                if (explain) {
                    madeDecision = decision;
                } else {
                    return Tuple.tuple(decision, null);
                }
            } else if (madeDecision.type() == Decision.Type.NO && decision.type() == Decision.Type.THROTTLE) {
                madeDecision = decision;
            }
            if (explain) {
                nodeDecisions.put(node.nodeId(), new NodeAllocationResult(node.node(), null, decision));
            }
        }
        return Tuple.tuple(madeDecision, nodeDecisions);
    }

    /**
     * Takes the store info for nodes that have a shard store and adds them to the node decisions,
     * leaving the node explanations untouched for those nodes that do not have any store information.
     */
    private static List<NodeAllocationResult> augmentExplanationsWithStoreInfo(
        Map<String, NodeAllocationResult> nodeDecisions,
        Map<String, NodeAllocationResult> withShardStores
    ) {
        if (nodeDecisions == null || withShardStores == null) {
            return null;
        }
        List<NodeAllocationResult> augmented = new ArrayList<>();
        for (Map.Entry<String, NodeAllocationResult> entry : nodeDecisions.entrySet()) {
            if (withShardStores.containsKey(entry.getKey())) {
                augmented.add(withShardStores.get(entry.getKey()));
            } else {
                augmented.add(entry.getValue());
            }
        }
        return augmented;
    }

    /**
     * Finds the store for the assigned shard in the fetched data, returns null if none is found.
     */
    private static StoreFilesMetadata findStore(DiscoveryNode node, Map<DiscoveryNode, StoreFilesMetadata> data) {
        if (!data.containsKey(node)) {
            return null;
        }
        return data.get(node);
    }

    private MatchingNodes findMatchingNodes(
        ShardRouting shard,
        RoutingAllocation allocation,
        boolean noMatchFailedNodes,
        DiscoveryNode primaryNode,
        StoreFilesMetadata primaryStore,
        Map<DiscoveryNode, StoreFilesMetadata> data,
        boolean explain
    ) {
        Map<DiscoveryNode, MatchingNode> matchingNodes = new HashMap<>();
        Map<String, NodeAllocationResult> nodeDecisions = explain ? new HashMap<>() : null;
        for (Map.Entry<DiscoveryNode, StoreFilesMetadata> nodeStoreEntry : data.entrySet()) {
            DiscoveryNode discoNode = nodeStoreEntry.getKey();
            if (noMatchFailedNodes
                && shard.unassignedInfo() != null
                && shard.unassignedInfo().getFailedNodeIds().contains(discoNode.getId())) {
                continue;
            }
            StoreFilesMetadata storeFilesMetadata = nodeStoreEntry.getValue();
            // we don't have any files at all, it is an empty index
            if (storeFilesMetadata.isEmpty()) {
                continue;
            }

            RoutingNode node = allocation.routingNodes().node(discoNode.getId());
            if (node == null) {
                continue;
            }

            // check if we can allocate on that node...
            // we only check for NO, since if this node is THROTTLING and it has enough "same data"
            // then we will try and assign it next time
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            MatchingNode matchingNode = null;
            if (explain) {
                matchingNode = computeMatchingNode(primaryNode, primaryStore, discoNode, storeFilesMetadata);
                ShardStoreInfo shardStoreInfo = new ShardStoreInfo(matchingNode.matchingBytes);
                nodeDecisions.put(node.nodeId(), new NodeAllocationResult(discoNode, shardStoreInfo, decision));
            }

            if (decision.type() == Decision.Type.NO) {
                continue;
            }

            if (matchingNode == null) {
                matchingNode = computeMatchingNode(primaryNode, primaryStore, discoNode, storeFilesMetadata);
            }
            matchingNodes.put(discoNode, matchingNode);
            if (logger.isTraceEnabled()) {
                if (matchingNode.isNoopRecovery) {
                    logger.trace("{}: node [{}] can perform a noop recovery", shard, discoNode.getName());
                } else if (matchingNode.retainingSeqNo >= 0) {
                    logger.trace(
                        "{}: node [{}] can perform operation-based recovery with retaining sequence number [{}]",
                        shard,
                        discoNode.getName(),
                        matchingNode.retainingSeqNo
                    );
                } else {
                    logger.trace(
                        "{}: node [{}] has [{}/{}] bytes of re-usable data",
                        shard,
                        discoNode.getName(),
                        new ByteSizeValue(matchingNode.matchingBytes),
                        matchingNode.matchingBytes
                    );
                }
            }
        }

        return new MatchingNodes(matchingNodes, nodeDecisions);
    }

    private Map<DiscoveryNode, StoreFilesMetadata> convertToNodeStoreFilesMetadataMap(
        AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> data
    ) {
        if (data.hasData() == false) {
            // if we don't have data yet return null
            return null;
        }
        return data.getData()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().storeFilesMetadata()));
    }

    private static long computeMatchingBytes(StoreFilesMetadata primaryStore, StoreFilesMetadata storeFilesMetadata) {
        long sizeMatched = 0;
        for (StoreFileMetadata storeFileMetadata : storeFilesMetadata) {
            String metadataFileName = storeFileMetadata.name();
            if (primaryStore.fileExists(metadataFileName) && primaryStore.file(metadataFileName).isSame(storeFileMetadata)) {
                sizeMatched += storeFileMetadata.length();
            }
        }
        return sizeMatched;
    }

    private static boolean hasMatchingSyncId(StoreFilesMetadata primaryStore, StoreFilesMetadata replicaStore) {
        String primarySyncId = primaryStore.syncId();
        return primarySyncId != null && primarySyncId.equals(replicaStore.syncId());
    }

    private static MatchingNode computeMatchingNode(
        DiscoveryNode primaryNode,
        StoreFilesMetadata primaryStore,
        DiscoveryNode replicaNode,
        StoreFilesMetadata replicaStore
    ) {
        final long retainingSeqNoForPrimary = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(primaryNode);
        final long retainingSeqNoForReplica = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(replicaNode);
        final boolean isNoopRecovery = (retainingSeqNoForReplica >= retainingSeqNoForPrimary && retainingSeqNoForPrimary >= 0)
            || hasMatchingSyncId(primaryStore, replicaStore);
        final long matchingBytes = computeMatchingBytes(primaryStore, replicaStore);
        return new MatchingNode(matchingBytes, retainingSeqNoForReplica, isNoopRecovery);
    }

    private static boolean canPerformOperationBasedRecovery(
        StoreFilesMetadata primaryStore,
        Map<DiscoveryNode, StoreFilesMetadata> shardStores,
        DiscoveryNode targetNode
    ) {
        final StoreFilesMetadata targetNodeStore = shardStores.get(targetNode);
        if (targetNodeStore == null || targetNodeStore.isEmpty()) {
            return false;
        }
        if (hasMatchingSyncId(primaryStore, targetNodeStore)) {
            return true;
        }
        return primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(targetNode) >= 0;
    }

    protected abstract AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation);

    /**
     * Returns a boolean indicating whether fetching shard data has been triggered at any point for the given shard.
     */
    protected abstract boolean hasInitiatedFetching(ShardRouting shard);

    /**
     * A class to enacapsulate the details regarding the a MatchNode for shard assignment
     */
    protected static class MatchingNode {
        static final Comparator<MatchingNode> COMPARATOR = Comparator.<MatchingNode, Boolean>comparing(m -> m.isNoopRecovery)
            .thenComparing(m -> m.retainingSeqNo)
            .thenComparing(m -> m.matchingBytes);

        final long matchingBytes;
        final long retainingSeqNo;
        final boolean isNoopRecovery;

        MatchingNode(long matchingBytes, long retainingSeqNo, boolean isNoopRecovery) {
            this.matchingBytes = matchingBytes;
            this.retainingSeqNo = retainingSeqNo;
            this.isNoopRecovery = isNoopRecovery;
        }

        boolean anyMatch() {
            return isNoopRecovery || retainingSeqNo >= 0 || matchingBytes > 0;
        }
    }

    static class MatchingNodes {
        private final Map<DiscoveryNode, MatchingNode> matchingNodes;
        private final DiscoveryNode nodeWithHighestMatch;
        @Nullable
        private final Map<String, NodeAllocationResult> nodeDecisions;

        MatchingNodes(Map<DiscoveryNode, MatchingNode> matchingNodes, @Nullable Map<String, NodeAllocationResult> nodeDecisions) {
            this.matchingNodes = matchingNodes;
            this.nodeDecisions = nodeDecisions;
            this.nodeWithHighestMatch = matchingNodes.entrySet()
                .stream()
                .filter(e -> e.getValue().anyMatch())
                .max(Comparator.comparing(Map.Entry::getValue, MatchingNode.COMPARATOR))
                .map(Map.Entry::getKey)
                .orElse(null);
        }

        /**
         * Returns the node with the highest "non zero byte" match compared to
         * the primary.
         */
        @Nullable
        public DiscoveryNode getNodeWithHighestMatch() {
            return this.nodeWithHighestMatch;
        }

        boolean canPerformNoopRecovery(DiscoveryNode node) {
            final MatchingNode matchingNode = matchingNodes.get(node);
            return matchingNode.isNoopRecovery;
        }

        /**
         * Did we manage to find any data, regardless how well they matched or not.
         */
        public boolean hasAnyData() {
            return matchingNodes.isEmpty() == false;
        }
    }
}
