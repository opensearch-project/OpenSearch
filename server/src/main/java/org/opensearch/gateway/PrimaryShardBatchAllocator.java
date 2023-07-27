/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
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
import org.opensearch.cluster.routing.allocation.decider.Decision.Type;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.gateway.AsyncBatchShardFetch.FetchResult;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public abstract class PrimaryShardBatchAllocator extends BaseGatewayShardAllocator {
    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     */
    private static boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() // must be primary
            && shard.unassigned() // must be unassigned
            // only handle either an existing store or a snapshot recovery
            && (shard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
            || shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT);
    }

    abstract protected FetchResult<NodeGatewayStartedShardsBatch> fetchData(Set<ShardRouting> shardsEligibleForFetch,
                                                                            Set<ShardRouting> inEligibleShards,
                                                                            RoutingAllocation allocation);

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard,
                                                             RoutingAllocation allocation,
                                                             Logger logger) {

        return makeAllocationDecision(new HashSet<>(Collections.singletonList(unassignedShard)),
            allocation, logger).get(unassignedShard);
    }

    /**
     * Build allocation decisions for all the shards present in the batch identified by batchId.
     * @param shards set of shards given for allocation
     * @param allocation current allocation of all the shards
     * @param logger logger used for logging
     * @return shard to allocation decision map
     */
    @Override
    public HashMap<ShardRouting, AllocateUnassignedDecision> makeAllocationDecision(Set<ShardRouting> shards,
                                                                                    RoutingAllocation allocation,
                                                                                    Logger logger) {
        HashMap<ShardRouting, AllocateUnassignedDecision> shardAllocationDecisions = new HashMap<>();
        final boolean explain = allocation.debugDecision();
        Set<ShardRouting> shardsEligibleForFetch = new HashSet<>();
        Set<ShardRouting> shardsNotEligibleForFetch = new HashSet<>();
        // identify ineligible shards
        for (ShardRouting shard : shards) {
            AllocateUnassignedDecision decision = skipSnapshotRestore(shard, allocation);
            if (decision != null) {
                shardsNotEligibleForFetch.add(shard);
                shardAllocationDecisions.put(shard, decision);
            } else {
                shardsEligibleForFetch.add(shard);
            }
        }
        // only fetch data for eligible shards
        final FetchResult<NodeGatewayStartedShardsBatch> shardsState = fetchData(shardsEligibleForFetch, shardsNotEligibleForFetch, allocation);
        // Note : shardsState contain the Data, there key is DiscoveryNode but value is Map<ShardId,
        // NodeGatewayStartedShardsBatch> so to get one shard level data (from all the nodes), we'll traverse the map
        // and construct the nodeShardState along the way before making any allocation decision. As metadata for a
        // particular shard is needed from all the discovery nodes.

        // process the received data
        for (ShardRouting unassignedShard : shardsEligibleForFetch) {
            if (shardsState.hasData() == false) {
                // if fetching is not done, add that no decision in the resultant map
                allocation.setHasPendingAsyncFetch();
                List<NodeAllocationResult> nodeDecisions = null;
                if (explain) {
                    nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
                }
                shardAllocationDecisions.put(unassignedShard,
                    AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA,
                        nodeDecisions));
            } else {
                Map<DiscoveryNode, NodeGatewayStartedShardsBatch> nodeResponses = shardsState.getData();
                Map<DiscoveryNode, NodeGatewayStartedShards> shardData = new HashMap<>();
                // build data for a shard from all the nodes
                for (Map.Entry<DiscoveryNode, NodeGatewayStartedShardsBatch> nodeEntry : nodeResponses.entrySet()) {
                    shardData.put(nodeEntry.getKey(),
                        nodeEntry.getValue().getNodeGatewayStartedShardsBatch().get(unassignedShard.shardId()));
                }
                // get allocation decision for this shard
                shardAllocationDecisions.put(unassignedShard, getAllocationDecision(unassignedShard, allocation,
                    shardData, logger));
            }
        }
        return shardAllocationDecisions;
    }

    /**
     * Below code is very similar to {@link org.opensearch.gateway.PrimaryShardAllocator} class makeAllocationDecision,
     * only difference is that NodeGatewayStartedShards object doesn't have the DiscoveryNode object as
     * BaseNodeResponse. So, DiscoveryNode reference is passed in Map<DiscoveryNode, NodeGatewayStartedShards> so
     * corresponding DiscoveryNode object can be used for rest of the implementation. Also, DiscoveryNode object
     * reference is added in DecidedNode class to achieve same use case of accessing corresponding DiscoveryNode object.
     * @param unassignedShard unassigned shard routing
     * @param allocation routing allocation object
     * @param shardState shard metadata fetched from all data nodes
     * @param logger logger
     * @return allocation decision taken for this shard
     */
    private AllocateUnassignedDecision getAllocationDecision(ShardRouting unassignedShard, RoutingAllocation allocation,
                                                             Map<DiscoveryNode, NodeGatewayStartedShards> shardState,
                                                             Logger logger) {
        final boolean explain = allocation.debugDecision();
        // don't create a new IndexSetting object for every shard as this could cause a lot of garbage
        // on cluster restart if we allocate a boat load of shards
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(unassignedShard.index());
        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
        final boolean snapshotRestore = unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;

        assert inSyncAllocationIds.isEmpty() == false;
        // use in-sync allocation ids to select nodes
        final PrimaryShardBatchAllocator.NodeShardsResult nodeShardsResult = buildNodeShardsResult(
            unassignedShard,
            snapshotRestore,
            allocation.getIgnoreNodes(unassignedShard.shardId()),
            inSyncAllocationIds,
            shardState,
            logger
        );
        final boolean enoughAllocationsFound = nodeShardsResult.orderedAllocationCandidates.size() > 0;
        logger.debug(
            "[{}][{}]: found {} allocation candidates of {} based on allocation ids: [{}]",
            unassignedShard.index(),
            unassignedShard.id(),
            nodeShardsResult.orderedAllocationCandidates.size(),
            unassignedShard,
            inSyncAllocationIds
        );

        if (enoughAllocationsFound == false) {
            if (snapshotRestore) {
                // let BalancedShardsAllocator take care of allocating this shard
                logger.debug(
                    "[{}][{}]: missing local data, will restore from [{}]",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard.recoverySource()
                );
                return AllocateUnassignedDecision.NOT_TAKEN;
            } else {
                // We have a shard that was previously allocated, but we could not find a valid shard copy to allocate the primary.
                // We could just be waiting for the node that holds the primary to start back up, in which case the allocation for
                // this shard will be picked up when the node joins and we do another allocation reroute
                logger.debug(
                    "[{}][{}]: not allocating, number_of_allocated_shards_found [{}]",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    nodeShardsResult.allocationsFound
                );
                return AllocateUnassignedDecision.no(
                    AllocationStatus.NO_VALID_SHARD_COPY,
                    explain ? buildNodeDecisions(null, shardState, inSyncAllocationIds) : null
                );
            }
        }

        NodesToAllocate nodesToAllocate = buildNodesToAllocate(
            allocation,
            nodeShardsResult.orderedAllocationCandidates,
            unassignedShard,
            false
        );
        DiscoveryNode node = null;
        String allocationId = null;
        boolean throttled = false;
        if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
            DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
            logger.debug(
                "[{}][{}]: allocating [{}] to [{}] on primary allocation",
                unassignedShard.index(),
                unassignedShard.id(),
                unassignedShard,
                decidedNode.getNode()
            );
            node = decidedNode.getNode();
            allocationId = decidedNode.nodeShardState.allocationId();
        } else if (nodesToAllocate.throttleNodeShards.isEmpty() && !nodesToAllocate.noNodeShards.isEmpty()) {
            // The deciders returned a NO decision for all nodes with shard copies, so we check if primary shard
            // can be force-allocated to one of the nodes.
            nodesToAllocate = buildNodesToAllocate(allocation, nodeShardsResult.orderedAllocationCandidates, unassignedShard, true);
            if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
                final DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
                final NodeGatewayStartedShards nodeShardState = decidedNode.nodeShardState;
                logger.debug(
                    "[{}][{}]: allocating [{}] to [{}] on forced primary allocation",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard,
                    decidedNode.getNode()
                );
                node = decidedNode.getNode();
                allocationId = nodeShardState.allocationId();
            } else if (nodesToAllocate.throttleNodeShards.isEmpty() == false) {
                logger.debug(
                    "[{}][{}]: throttling allocation [{}] to [{}] on forced primary allocation",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard,
                    nodesToAllocate.throttleNodeShards
                );
                throttled = true;
            } else {
                logger.debug(
                    "[{}][{}]: forced primary allocation denied [{}]",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard
                );
            }
        } else {
            // we are throttling this, since we are allowed to allocate to this node but there are enough allocations
            // taking place on the node currently, ignore it for now
            logger.debug(
                "[{}][{}]: throttling allocation [{}] to [{}] on primary allocation",
                unassignedShard.index(),
                unassignedShard.id(),
                unassignedShard,
                nodesToAllocate.throttleNodeShards
            );
            throttled = true;
        }

        List<NodeAllocationResult> nodeResults = null;
        if (explain) {
            nodeResults = buildNodeDecisions(nodesToAllocate, shardState, inSyncAllocationIds);
        }
        if (allocation.hasPendingAsyncFetch()) {
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeResults);
        } else if (node != null) {
            return AllocateUnassignedDecision.yes(node, allocationId, nodeResults, false);
        } else if (throttled) {
            return AllocateUnassignedDecision.throttle(nodeResults);
        } else {
            return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeResults, true);
        }
    }

    /**
     * Skip doing fetchData call for a shard if recovery mode is snapshot. Also do not take decision if allocator is
     * not responsible for this particular shard.
     * @param unassignedShard unassigned shard routing
     * @param allocation routing allocation object
     * @return allocation decision taken for this shard
     */
    private AllocateUnassignedDecision skipSnapshotRestore(ShardRouting unassignedShard, RoutingAllocation allocation) {
        if (isResponsibleFor(unassignedShard) == false) {
            // this allocator is not responsible for allocating this shard
            return AllocateUnassignedDecision.NOT_TAKEN;
        }
        final boolean explain = allocation.debugDecision();
        if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
            && allocation.snapshotShardSizeInfo().getShardSize(unassignedShard) == null) {
            List<NodeAllocationResult> nodeDecisions = null;
            if (explain) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }
        return null;
    }

    /**
     * Builds a map of nodes to the corresponding allocation decisions for those nodes.
     */
    private static List<NodeAllocationResult> buildNodeDecisions(
        NodesToAllocate nodesToAllocate,
        Map<DiscoveryNode, NodeGatewayStartedShards> fetchedShardData,
        Set<String> inSyncAllocationIds
    ) {
        List<NodeAllocationResult> nodeResults = new ArrayList<>();
        Map<DiscoveryNode, NodeGatewayStartedShards> ineligibleShards;
        if (nodesToAllocate != null) {
            final Set<DiscoveryNode> discoNodes = new HashSet<>();
            nodeResults.addAll(
                Stream.of(nodesToAllocate.yesNodeShards, nodesToAllocate.throttleNodeShards, nodesToAllocate.noNodeShards)
                    .flatMap(Collection::stream)
                    .map(dnode -> {
                        discoNodes.add(dnode.getNode());
                        return new NodeAllocationResult(
                            dnode.getNode(),
                            shardStoreInfo(dnode.nodeShardState, inSyncAllocationIds),
                            dnode.decision
                        );
                    })
                    .collect(Collectors.toList())
            );

            ineligibleShards = fetchedShardData.entrySet()
                .stream()
                .filter(shardData -> discoNodes.contains(shardData.getKey()) == false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
            // there were no shard copies that were eligible for being assigned the allocation,
            // so all fetched shard data are ineligible shards
            ineligibleShards = fetchedShardData;
        }

        nodeResults.addAll(
            ineligibleShards.entrySet().stream()
                .map(shardData -> new NodeAllocationResult(shardData.getKey(), shardStoreInfo(shardData.getValue(),
                    inSyncAllocationIds), null))
                .collect(Collectors.toList())
        );

        return nodeResults;
    }

    private static ShardStoreInfo shardStoreInfo(NodeGatewayStartedShards nodeShardState, Set<String> inSyncAllocationIds) {
        final Exception storeErr = nodeShardState.storeException();
        final boolean inSync = nodeShardState.allocationId() != null && inSyncAllocationIds.contains(nodeShardState.allocationId());
        return new ShardStoreInfo(nodeShardState.allocationId(), inSync, storeErr);
    }

    private static final Comparator<NodeGatewayStartedShards> NO_STORE_EXCEPTION_FIRST_COMPARATOR = Comparator.comparing(
        (NodeGatewayStartedShards state) -> state.storeException() == null
    ).reversed();
    private static final Comparator<NodeGatewayStartedShards> PRIMARY_FIRST_COMPARATOR = Comparator.comparing(
        NodeGatewayStartedShards::primary
    ).reversed();

    private static final Comparator<NodeGatewayStartedShards> HIGHEST_REPLICATION_CHECKPOINT_FIRST_COMPARATOR = Comparator.comparing(
        NodeGatewayStartedShards::replicationCheckpoint,
        Comparator.nullsLast(Comparator.naturalOrder())
    );

    /**
     * Builds a list of nodes. If matchAnyShard is set to false, only nodes that have an allocation id matching
     * inSyncAllocationIds are added to the list. Otherwise, any node that has a shard is added to the list, but
     * entries with matching allocation id are always at the front of the list.
     */
    protected static NodeShardsResult buildNodeShardsResult(
        ShardRouting shard,
        boolean matchAnyShard,
        Set<String> ignoreNodes,
        Set<String> inSyncAllocationIds,
        Map<DiscoveryNode, NodeGatewayStartedShards> shardState,
        Logger logger
    ) {
        /**
         * Orders the active shards copies based on below comparators
         * 1. No store exception i.e. shard copy is readable
         * 2. Prefer previous primary shard
         * 3. Prefer shard copy with the highest replication checkpoint. It is NO-OP for doc rep enabled indices.
         */
        final Comparator<NodeGatewayStartedShards> comparator; // allocation preference
        if (matchAnyShard) {
            // prefer shards with matching allocation ids
            Comparator<NodeGatewayStartedShards> matchingAllocationsFirst = Comparator.comparing(
                (NodeGatewayStartedShards state) -> inSyncAllocationIds.contains(state.allocationId())
            ).reversed();
            comparator = matchingAllocationsFirst.thenComparing(NO_STORE_EXCEPTION_FIRST_COMPARATOR)
                .thenComparing(PRIMARY_FIRST_COMPARATOR)
                .thenComparing(HIGHEST_REPLICATION_CHECKPOINT_FIRST_COMPARATOR);
        } else {
            comparator = NO_STORE_EXCEPTION_FIRST_COMPARATOR.thenComparing(PRIMARY_FIRST_COMPARATOR)
                .thenComparing(HIGHEST_REPLICATION_CHECKPOINT_FIRST_COMPARATOR);
        }
        // TreeMap will sort the entries based on key, comparator is assigned above
        TreeMap<NodeGatewayStartedShards, DiscoveryNode> shardStatesToNode = new TreeMap<>(comparator);
        int numberOfAllocationsFound = 0;
        for (Map.Entry<DiscoveryNode, NodeGatewayStartedShards> nodeShardStateEntry : shardState.entrySet()) {
            DiscoveryNode node = nodeShardStateEntry.getKey();
            NodeGatewayStartedShards nodeShardState = nodeShardStateEntry.getValue();
            String allocationId = nodeShardState.allocationId();

            if (ignoreNodes.contains(node.getId())) {
                continue;
            }

            if (nodeShardState.storeException() == null) {
                if (allocationId == null) {
                    logger.trace("[{}] on node [{}] has no shard state information", shard, node);
                } else {
                    logger.trace("[{}] on node [{}] has allocation id [{}]", shard, node, allocationId);
                }
            } else {
                final String finalAllocationId = allocationId;
                if (nodeShardState.storeException() instanceof ShardLockObtainFailedException) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "[{}] on node [{}] has allocation id [{}] but the store can not be "
                                + "opened as it's locked, treating as valid shard",
                            shard,
                            node,
                            finalAllocationId
                        ),
                        nodeShardState.storeException()
                    );
                } else {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "[{}] on node [{}] has allocation id [{}] but the store can not be " + "opened, treating as no allocation id",
                            shard,
                            node,
                            finalAllocationId
                        ),
                        nodeShardState.storeException()
                    );
                    allocationId = null;
                }
            }

            if (allocationId != null) {
                assert nodeShardState.storeException() == null || nodeShardState.storeException() instanceof ShardLockObtainFailedException
                    : "only allow store that can be opened or that throws a ShardLockObtainFailedException while being opened but got a "
                    + "store throwing "
                    + nodeShardState.storeException();
                numberOfAllocationsFound++;
                if (matchAnyShard || inSyncAllocationIds.contains(nodeShardState.allocationId())) {
                    shardStatesToNode.put(nodeShardState, node);
                }
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace(
                "{} candidates for allocation: {}",
                shard,
                shardState.keySet().stream().map(DiscoveryNode::getName).collect(Collectors.joining(", "))
            );
        }
        return new NodeShardsResult(shardStatesToNode, numberOfAllocationsFound);
    }

    /**
     * Split the list of node shard states into groups yes/no/throttle based on allocation deciders
     */
    private static NodesToAllocate buildNodesToAllocate(
        RoutingAllocation allocation,
        TreeMap<NodeGatewayStartedShards, DiscoveryNode> shardStateToNode,
        ShardRouting shardRouting,
        boolean forceAllocate
    ) {
        List<DecidedNode> yesNodeShards = new ArrayList<>();
        List<DecidedNode> throttledNodeShards = new ArrayList<>();
        List<DecidedNode> noNodeShards = new ArrayList<>();
        for (Map.Entry<NodeGatewayStartedShards, DiscoveryNode> nodeShardState : shardStateToNode.entrySet()) {
            RoutingNode node = allocation.routingNodes().node(nodeShardState.getValue().getId());
            if (node == null) {
                continue;
            }

            Decision decision = forceAllocate
                ? allocation.deciders().canForceAllocatePrimary(shardRouting, node, allocation)
                : allocation.deciders().canAllocate(shardRouting, node, allocation);
            DecidedNode decidedNode = new DecidedNode(nodeShardState.getKey(), decision, nodeShardState.getValue());
            if (decision.type() == Type.THROTTLE) {
                throttledNodeShards.add(decidedNode);
            } else if (decision.type() == Type.NO) {
                noNodeShards.add(decidedNode);
            } else {
                yesNodeShards.add(decidedNode);
            }
        }
        return new NodesToAllocate(
            Collections.unmodifiableList(yesNodeShards),
            Collections.unmodifiableList(throttledNodeShards),
            Collections.unmodifiableList(noNodeShards)
        );
    }

    private static class NodeShardsResult {
        final TreeMap<NodeGatewayStartedShards, DiscoveryNode>  orderedAllocationCandidates;
        final int allocationsFound;

        NodeShardsResult(TreeMap<NodeGatewayStartedShards, DiscoveryNode> orderedAllocationCandidates, int allocationsFound) {
            this.orderedAllocationCandidates = orderedAllocationCandidates;
            this.allocationsFound = allocationsFound;
        }
    }

    static class NodesToAllocate {
        final List<DecidedNode> yesNodeShards;
        final List<DecidedNode> throttleNodeShards;
        final List<DecidedNode> noNodeShards;

        NodesToAllocate(List<DecidedNode> yesNodeShards, List<DecidedNode> throttleNodeShards, List<DecidedNode> noNodeShards) {
            this.yesNodeShards = yesNodeShards;
            this.throttleNodeShards = throttleNodeShards;
            this.noNodeShards = noNodeShards;
        }
    }

    /**
     * This class encapsulates the shard state retrieved from a node and the decision that was made
     * by the allocator for allocating to the node that holds the shard copy.
     */
    private static class DecidedNode {
        final NodeGatewayStartedShards nodeShardState;
        final Decision decision;
        final DiscoveryNode node;

        private DecidedNode(NodeGatewayStartedShards nodeShardState, Decision decision, DiscoveryNode node) {
            this.nodeShardState = nodeShardState;
            this.decision = decision;
            this.node = node;
        }

        public DiscoveryNode getNode() {
            return node;
        }
    }
}
