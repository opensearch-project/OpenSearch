/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.gateway.AsyncBatchShardFetch.FetchResult;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

public abstract class ReplicaShardBatchAllocator extends BaseGatewayShardAllocator {

    /**
     * Process existing recoveries of replicas and see if we need to cancel them if we find a better
     * match. Today, a better match is one that can perform a no-op recovery while the previous recovery
     * has to copy segment files.
     */
    public void processExistingRecoveries(RoutingAllocation allocation, List<Set<ShardRouting>> shardBatches) {
        Metadata metadata = allocation.metadata();
        RoutingNodes routingNodes = allocation.routingNodes();
        List<Runnable> shardCancellationActions = new ArrayList<>();
        for (Set<ShardRouting> shardBatch : shardBatches) {
            Set<ShardRouting> eligibleFetchShards = new HashSet<>();
            Set<ShardRouting> ineligibleShards = new HashSet<>();
            for (ShardRouting shard : shardBatch) {
                if (shard.primary()) {
                    ineligibleShards.add(shard);
                    continue;
                }
                if (shard.initializing() == false) {
                    ineligibleShards.add(shard);
                    continue;
                }
                if (shard.relocatingNodeId() != null) {
                    ineligibleShards.add(shard);
                    continue;
                }

                // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
                if (shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                    ineligibleShards.add(shard);
                    continue;
                }
                eligibleFetchShards.add(shard);
            }
            AsyncBatchShardFetch.FetchResult <NodeStoreFilesMetadataBatch> shardState = fetchData(eligibleFetchShards, ineligibleShards, allocation);
            if (shardState.hasData()) {
                logger.trace("{}: fetching new stores for initializing shard batch", eligibleFetchShards);
                continue; // still fetching
            }
            for (ShardRouting shard: eligibleFetchShards) {
                ShardRouting primaryShard = allocation.routingNodes().activePrimary(shard.shardId());
                assert primaryShard != null : "the replica shard can be allocated on at least one node, so there must be an active primary";
                assert primaryShard.currentNodeId() != null;
                final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
                final TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore= findStore(primaryNode, shardState, shard);
                if (primaryStore == null) {
                    // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                    // just let the recovery find it out, no need to do anything about it for the initializing shard
                    logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
                    continue;
                }
                ReplicaShardAllocator.MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, true, primaryNode, primaryStore, shardState, false);
                if (matchingNodes.getNodeWithHighestMatch() != null) {
                    DiscoveryNode currentNode = allocation.nodes().get(shard.currentNodeId());
                    DiscoveryNode nodeWithHighestMatch = matchingNodes.getNodeWithHighestMatch();
                    // current node will not be in matchingNodes as it is filtered away by SameShardAllocationDecider
                    if (currentNode.equals(nodeWithHighestMatch) == false
                        && matchingNodes.canPerformNoopRecovery(nodeWithHighestMatch)
                        && canPerformOperationBasedRecovery(primaryStore, shardState, currentNode, shard) == false) {
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
                        shardCancellationActions.add(
                            () -> routingNodes.failShard(
                                logger,
                                shard,
                                unassignedInfo,
                                metadata.getIndexSafe(shard.index()),
                                allocation.changes()
                            )
                        );
                    }
                }
            }
        }
        for (Runnable action : shardCancellationActions) {
            action.run();
        }
    }

    private static boolean isResponsibleFor(final ShardRouting shard) {
        return !shard.primary() // must be a replica
            && shard.unassigned() // must be unassigned
            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
            && shard.unassignedInfo().getReason() != UnassignedInfo.Reason.INDEX_CREATED;
    }

    abstract protected FetchResult<NodeStoreFilesMetadataBatch> fetchData(Set<ShardRouting> shardEligibleForFetch,
                                                                          Set<ShardRouting> inEligibleShards,
                                                                          RoutingAllocation allocation);

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard, RoutingAllocation allocation, Logger logger) {
        return null;
    }

    @Override
    public HashMap<ShardRouting, AllocateUnassignedDecision> makeAllocationDecision(Set<ShardRouting> shards, RoutingAllocation allocation, Logger logger) {
        HashMap<ShardRouting, AllocateUnassignedDecision> shardAllocationDecisions = new HashMap<>();
        final boolean explain = allocation.debugDecision();
        final RoutingNodes routingNodes = allocation.routingNodes();
        Set<ShardRouting> shardsEligibleForFetch = new HashSet<>();
        Set<ShardRouting> shardsNotEligibleForFetch = new HashSet<>();
        HashMap<ShardRouting, Tuple<Decision, Map<String, NodeAllocationResult>>> nodeAllocationDecisions = new HashMap<>();
        for(ShardRouting shard : shards) {
            if (!isResponsibleFor(shard)) {
                // this allocator n is not responsible for allocating this shard
                shardsNotEligibleForFetch.add(shard);
                shardAllocationDecisions.put(shard, AllocateUnassignedDecision.NOT_TAKEN);
                continue;
            }

            Tuple<Decision, Map<String, NodeAllocationResult>> result = canBeAllocatedToAtLeastOneNode(shard, allocation);
            Decision allocationDecision = result.v1();
            if (allocationDecision.type() != Decision.Type.YES && (!explain || !hasInitiatedFetching(shard))){
                // only return early if we are not in explain mode, or we are in explain mode but we have not
                // yet attempted to fetch any shard data
                logger.trace("{}: ignoring allocation, can't be allocated on any node", shard);
                shardsNotEligibleForFetch.add(shard);
                shardAllocationDecisions.put(shard,
                    AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocationDecision.type()),
                        result.v2() != null ? new ArrayList<>(result.v2().values()) : null));
                continue;
            }
            // storing the nodeDecisions in nodeAllocationDecisions if the decision is not YES
            // so that we don't have to compute the decisions again
            // ToDo: Check if we need to store or computing again will be cheaper/better
            nodeAllocationDecisions.put(shard, result);

            shardsEligibleForFetch.add(shard);
        }

        // only fetch data for eligible shards
        final FetchResult<NodeStoreFilesMetadataBatch> shardsState = fetchData(shardsEligibleForFetch, shardsNotEligibleForFetch, allocation);

        // ToDo: Analyze if we need to create hashmaps here or sequential is better
//        Map<ShardRouting, DiscoveryNode> primaryNodesMap = shardsEligibleForFetch.stream()
//            .map(x -> routingNodes.activePrimary(x.shardId()))
//            .filter(Objects::nonNull)
//            .filter(node -> node.currentNodeId() != null)
//            .collect(Collectors.toMap(Function.identity(), node -> allocation.nodes().get(node.currentNodeId())));
//
//        Map<ShardRouting, NodeStoreFilesMetadata> primaryStoreMap = findStoresBatch(primaryNodesMap, shardsState);

        for (ShardRouting unassignedShard : shardsEligibleForFetch) {
            if (!shardsState.hasData()) {
                logger.trace("{}: ignoring allocation, still fetching shard stores", unassignedShard);
                allocation.setHasPendingAsyncFetch();
                List<NodeAllocationResult> nodeDecisions = null;
                if (explain) {
                    nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
                }
                shardAllocationDecisions.put(unassignedShard,
                    AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions));
                continue;
            }
            Tuple<Decision, Map<String, NodeAllocationResult>> result = nodeAllocationDecisions.get(unassignedShard);
            ShardRouting primaryShard = routingNodes.activePrimary(unassignedShard.shardId());
            if (primaryShard == null) {
                assert explain : "primary should only be null here if we are in explain mode, so we didn't "
                    + "exit early when canBeAllocatedToAtLeastOneNode didn't return a YES decision";
                shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.no(
                    UnassignedInfo.AllocationStatus.fromDecision(result.v1().type()),
                    result.v2() != null ? new ArrayList<>(result.v2().values()) : null
                ));
                continue;
            }
            assert primaryShard.currentNodeId() != null;
            final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
            final TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore = findStore(primaryNode, shardsState, unassignedShard);
            if (primaryStore == null) {
                // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                // we want to let the replica be allocated in order to expose the actual problem with the primary that the replica
                // will try and recover from
                // Note, this is the existing behavior, as exposed in running CorruptFileTest#testNoPrimaryData
                logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", unassignedShard);
                shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.NOT_TAKEN);
            }

            // find the matching nodes
            ReplicaShardAllocator.MatchingNodes matchingNodes = findMatchingNodes(
                unassignedShard,
                allocation,
                false,
                primaryNode,
                primaryStore,
                shardsState,
                explain
            );

            assert explain == false || matchingNodes.getNodeDecisions() != null : "in explain mode, we must have individual node decisions";

            List<NodeAllocationResult> nodeDecisions = ReplicaShardAllocator.augmentExplanationsWithStoreInfo(result.v2(), matchingNodes.getNodeDecisions());
            if (result.v1().type() != Decision.Type.YES) {
                shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(result.v1().type()), nodeDecisions));
                continue;
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
                    shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.throttle(nodeDecisions));
                    continue;
                } else {
                    logger.debug(
                        "[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store",
                        unassignedShard.index(),
                        unassignedShard.id(),
                        unassignedShard,
                        nodeWithHighestMatch.node()
                    );
                    // we found a match
                    shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.yes(nodeWithHighestMatch.node(), null, nodeDecisions, true));
                    continue;
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
                shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.delayed(remainingDelayMillis, totalDelayMillis, nodeDecisions));
            }

            shardAllocationDecisions.put(unassignedShard, AllocateUnassignedDecision.NOT_TAKEN);
        }
        return shardAllocationDecisions;
    }

    private ReplicaShardAllocator.MatchingNodes findMatchingNodes(
        ShardRouting shard,
        RoutingAllocation allocation,
        boolean noMatchFailedNodes,
        DiscoveryNode primaryNode,
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore,
        FetchResult<NodeStoreFilesMetadataBatch> data,
        boolean explain
    ) {
        Map<DiscoveryNode, ReplicaShardAllocator.MatchingNode> matchingNodes = new HashMap<>();
        Map<String, NodeAllocationResult> nodeDecisions = explain ? new HashMap<>() : null;
        for (Map.Entry<DiscoveryNode, NodeStoreFilesMetadataBatch> nodeStoreEntry : data.getData().entrySet()) {
            DiscoveryNode discoNode = nodeStoreEntry.getKey();
            if (noMatchFailedNodes
                && shard.unassignedInfo() != null
                && shard.unassignedInfo().getFailedNodeIds().contains(discoNode.getId())) {
                continue;
            }
            TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata storeFilesMetadata = nodeStoreEntry.getValue()
                .getNodeStoreFilesMetadataBatch().get(shard.shardId()).storeFilesMetadata();
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
            ReplicaShardAllocator.MatchingNode matchingNode = null;
            if (explain) {
                matchingNode = computeMatchingNode(primaryNode, primaryStore, discoNode, storeFilesMetadata);
                NodeAllocationResult.ShardStoreInfo shardStoreInfo = new NodeAllocationResult.ShardStoreInfo(matchingNode.matchingBytes);
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

        return new ReplicaShardAllocator.MatchingNodes(matchingNodes, nodeDecisions);
    }

    private static ReplicaShardAllocator.MatchingNode computeMatchingNode(
        DiscoveryNode primaryNode,
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore,
        DiscoveryNode replicaNode,
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata replicaStore
    ) {
        final long retainingSeqNoForPrimary = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(primaryNode);
        final long retainingSeqNoForReplica = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(replicaNode);
        final boolean isNoopRecovery = (retainingSeqNoForReplica >= retainingSeqNoForPrimary && retainingSeqNoForPrimary >= 0)
            || hasMatchingSyncId(primaryStore, replicaStore);
        final long matchingBytes = computeMatchingBytes(primaryStore, replicaStore);
        return new ReplicaShardAllocator.MatchingNode(matchingBytes, retainingSeqNoForReplica, isNoopRecovery);
    }

    private static boolean hasMatchingSyncId(
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore,
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata replicaStore
    ) {
        String primarySyncId = primaryStore.syncId();
        return primarySyncId != null && primarySyncId.equals(replicaStore.syncId());
    }

    private static long computeMatchingBytes(
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore,
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata storeFilesMetadata
    ) {
        long sizeMatched = 0;
        for (StoreFileMetadata storeFileMetadata : storeFilesMetadata) {
            String metadataFileName = storeFileMetadata.name();
            if (primaryStore.fileExists(metadataFileName) && primaryStore.file(metadataFileName).isSame(storeFileMetadata)) {
                sizeMatched += storeFileMetadata.length();
            }
        }
        return sizeMatched;
    }

    /**
     * Determines if the shard can be allocated on at least one node based on the allocation deciders.
     *
     * Returns the best allocation decision for allocating the shard on any node (i.e. YES if at least one
     * node decided YES, THROTTLE if at least one node decided THROTTLE, and NO if none of the nodes decided
     * YES or THROTTLE).  If in explain mode, also returns the node-level explanations as the second element
     * in the returned tuple.
     */
    private static Tuple<Decision, Map<String, NodeAllocationResult>> canBeAllocatedToAtLeastOneNode(
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

    protected abstract AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation);

    protected abstract boolean hasInitiatedFetching(ShardRouting shard);

//    private static Map<ShardRouting, NodeStoreFilesMetadata> findStoresBatch(Map<ShardRouting, DiscoveryNode> shardToNodeMap,
//                                                                                              FetchResult<NodeStoreFilesMetadataBatch> data) {
//        Map<ShardRouting, NodeStoreFilesMetadata> shardStores = new HashMap<>();
//        shardToNodeMap.entrySet().forEach(entry -> {
//            NodeStoreFilesMetadataBatch nodeFilesStore = data.getData().get(entry.getValue());
//            if (nodeFilesStore == null) {
//                shardStores.put(entry.getKey(), null);
//            } else {
//                shardStores.put(entry.getKey(), nodeFilesStore.getNodeStoreFilesMetadataBatch().get(entry.getKey().shardId()));
//            }
//        });
//        return shardStores;
//    }

    private static TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata findStore(
        DiscoveryNode node,
        FetchResult<NodeStoreFilesMetadataBatch> data,
        ShardRouting shard
    ) {
        NodeStoreFilesMetadataBatch nodeFilesStore = data.getData().get(node);
        if (nodeFilesStore == null) {
            return null;
        }
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeFileStoreMetadata = nodeFilesStore.getNodeStoreFilesMetadataBatch().get(shard.shardId());
        if (nodeFileStoreMetadata.getStoreFileFetchException() != null) {
            // Do we need to throw an exception here, to handle this case differently?
            return null;
        }
        return nodeFileStoreMetadata.storeFilesMetadata();
    }

    private static boolean canPerformOperationBasedRecovery(
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata primaryStore,
        FetchResult<NodeStoreFilesMetadataBatch> data,
        DiscoveryNode targetNode,
        ShardRouting shard
    ) {
        final NodeStoreFilesMetadataBatch nodeFilesStore = data.getData().get(targetNode);
        if (nodeFilesStore == null) {
            return false;
        }
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeFileStoreMetadata = nodeFilesStore.getNodeStoreFilesMetadataBatch().get(shard.shardId());
        if (nodeFileStoreMetadata.getStoreFileFetchException() != null) {
            return false;
        }
        TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata targetNodeStore = nodeFileStoreMetadata.storeFilesMetadata();
        if (targetNodeStore == null || targetNodeStore.isEmpty()) {
            return false;
        }
        if (hasMatchingSyncId(primaryStore, targetNodeStore)) {
            return true;
        }
        return primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(targetNode) >= 0;
    }
}
