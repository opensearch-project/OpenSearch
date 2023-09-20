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
import org.opensearch.gateway.AsyncShardFetch.FetchResult;
import org.opensearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The primary shard allocator allocates unassigned primary shards to nodes that hold
 * valid copies of the unassigned primaries.  It does this by iterating over all unassigned
 * primary shards in the routing table and fetching shard metadata from each node in the cluster
 * that holds a copy of the shard.  The shard metadata from each node is compared against the
 * set of valid allocation IDs and for all valid shard copies (if any), the primary shard allocator
 * executes the allocation deciders to chose a copy to assign the primary shard to.
 *
 * Note that the PrimaryShardAllocator does *not* allocate primaries on index creation
 * (see {@link org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator}),
 * nor does it allocate primaries when a primary shard failed and there is a valid replica
 * copy that can immediately be promoted to primary, as this takes place in {@link RoutingNodes#failShard}.
 *
 * @opensearch.internal
 */
public abstract class PrimaryShardAllocator extends BaseGatewayShardAllocator {
    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     */
    protected static boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() // must be primary
            && shard.unassigned() // must be unassigned
            // only handle either an existing store or a snapshot recovery
            && (shard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
                || shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT);
    }

    /**
     * Skip doing fetchData call for a shard if recovery mode is snapshot. Also do not take decision if allocator is
     * not responsible for this particular shard.
     *
     * @param unassignedShard unassigned shard routing
     * @param allocation      routing allocation object
     * @return allocation decision taken for this shard
     */
    protected AllocateUnassignedDecision getInEligibleShardDecision(ShardRouting unassignedShard, RoutingAllocation allocation) {
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

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(
        final ShardRouting unassignedShard,
        final RoutingAllocation allocation,
        final Logger logger
    ) {
        AllocateUnassignedDecision decision = getInEligibleShardDecision(unassignedShard, allocation);
        if (decision != null) {
            return decision;
        }
        final FetchResult<NodeGatewayStartedShards> shardState = fetchData(unassignedShard, allocation);
        if (shardState.hasData() == false) {
            allocation.setHasPendingAsyncFetch();
            List<NodeAllocationResult> nodeDecisions = null;
            if (allocation.debugDecision()) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }
        NodeShardStates nodeShardStates = adaptToNodeShardStates(shardState);
        return getAllocationDecision(unassignedShard, allocation, nodeShardStates, logger);
    }

    private static NodeShardStates adaptToNodeShardStates(FetchResult<NodeGatewayStartedShards> shardsState) {
        NodeShardStates nodeShardStates = new NodeShardStates();
        shardsState.getData().forEach((node, nodeGatewayStartedShard) -> {
            nodeShardStates.getNodeShardStates().add(
                new NodeShardState(
                    node,
                    nodeGatewayStartedShard.allocationId(),
                    nodeGatewayStartedShard.primary(),
                    nodeGatewayStartedShard.replicationCheckpoint(),
                    nodeGatewayStartedShard.storeException()
                )
            );
        });
        return nodeShardStates;
    }

    protected AllocateUnassignedDecision getAllocationDecision(
        ShardRouting unassignedShard,
        RoutingAllocation allocation,
        NodeShardStates shardState,
        Logger logger
    ) {
        final boolean explain = allocation.debugDecision();
        // don't create a new IndexSetting object for every shard as this could cause a lot of garbage
        // on cluster restart if we allocate a boat load of shards
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(unassignedShard.index());
        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
        final boolean snapshotRestore = unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;

        assert inSyncAllocationIds.isEmpty() == false;
        // use in-sync allocation ids to select nodes
        final NodeShardsResult nodeShardsResult = buildNodeShardsResult(
            unassignedShard,
            snapshotRestore,
            allocation.getIgnoreNodes(unassignedShard.shardId()),
            inSyncAllocationIds,
            shardState,
            logger
        );
        final boolean enoughAllocationsFound = !nodeShardsResult.orderedAllocationCandidates.getNodeShardStates().isEmpty();
        logger.debug(
            "[{}][{}]: found {} allocation candidates of {} based on allocation ids: [{}]",
            unassignedShard.index(),
            unassignedShard.id(),
            nodeShardsResult.orderedAllocationCandidates.getNodeShardStates().size(),
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
            NodeShardState nodeShardState = decidedNode.nodeShardState;
            logger.debug(
                "[{}][{}]: allocating [{}] to [{}] on primary allocation",
                unassignedShard.index(),
                unassignedShard.id(),
                unassignedShard,
                nodeShardState.getNode()
            );
            node = nodeShardState.getNode();
            allocationId = nodeShardState.allocationId();
        } else if (nodesToAllocate.throttleNodeShards.isEmpty() && !nodesToAllocate.noNodeShards.isEmpty()) {
            // The deciders returned a NO decision for all nodes with shard copies, so we check if primary shard
            // can be force-allocated to one of the nodes.
            nodesToAllocate = buildNodesToAllocate(allocation, nodeShardsResult.orderedAllocationCandidates, unassignedShard, true);
            if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
                final DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
                final NodeShardState nodeShardState = decidedNode.nodeShardState;
                logger.debug(
                    "[{}][{}]: allocating [{}] to [{}] on forced primary allocation",
                    unassignedShard.index(),
                    unassignedShard.id(),
                    unassignedShard,
                    nodeShardState.getNode()
                );
                node = nodeShardState.getNode();
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
     * Builds a map of nodes to the corresponding allocation decisions for those nodes.
     */
    private static List<NodeAllocationResult> buildNodeDecisions(
        NodesToAllocate nodesToAllocate,
        NodeShardStates fetchedShardData,
        Set<String> inSyncAllocationIds
    ) {
        List<NodeAllocationResult> nodeResults = new ArrayList<>();
        Collection<NodeShardState> ineligibleShards = new ArrayList<>();
        if (nodesToAllocate != null) {
            final Set<DiscoveryNode> discoNodes = new HashSet<>();
            nodeResults.addAll(
                Stream.of(nodesToAllocate.yesNodeShards, nodesToAllocate.throttleNodeShards, nodesToAllocate.noNodeShards)
                    .flatMap(Collection::stream)
                    .map(dnode -> {
                        discoNodes.add(dnode.nodeShardState.getNode());
                        return new NodeAllocationResult(
                            dnode.nodeShardState.getNode(),
                            shardStoreInfo(dnode.nodeShardState, inSyncAllocationIds),
                            dnode.decision
                        );
                    })
                    .collect(Collectors.toList())
            );
            ineligibleShards = fetchedShardData
                .getNodeShardStates()
                .stream()
                .filter(shardData -> discoNodes.contains(shardData.getNode()) == false)
                .collect(Collectors.toList());
        } else {
            // there were no shard copies that were eligible for being assigned the allocation,
            // so all fetched shard data are ineligible shards
            ineligibleShards = fetchedShardData.getNodeShardStates();
        }

        nodeResults.addAll(
            ineligibleShards.stream()
                .map(shardData -> new NodeAllocationResult(shardData.getNode(), shardStoreInfo(shardData, inSyncAllocationIds), null))
                .collect(Collectors.toList())
        );

        return nodeResults;
    }

    protected static ShardStoreInfo shardStoreInfo(NodeShardState nodeShardState, Set<String> inSyncAllocationIds) {
        final Exception storeErr = nodeShardState.storeException();
        final boolean inSync = nodeShardState.allocationId() != null && inSyncAllocationIds.contains(nodeShardState.allocationId());
        return new ShardStoreInfo(nodeShardState.allocationId(), inSync, storeErr);
    }

    private static final Comparator<NodeShardState> NO_STORE_EXCEPTION_FIRST_COMPARATOR = Comparator.comparing(
        (NodeShardState state) -> state.storeException() == null
    ).reversed();
    private static final Comparator<NodeShardState> PRIMARY_FIRST_COMPARATOR = Comparator.comparing(NodeShardState::primary).reversed();

    private static final Comparator<NodeShardState> HIGHEST_REPLICATION_CHECKPOINT_FIRST_COMPARATOR = Comparator.comparing(
        NodeShardState::replicationCheckpoint,
        Comparator.nullsLast(Comparator.naturalOrder())
    );

    /**
     * Builds a list of nodes. If matchAnyShard is set to false, only nodes that have an allocation id matching
     * inSyncAllocationIds are added to the list. Otherwise, any node that has a shard is added to the list, but
     * entries with matching allocation id are always at the front of the list.
     */
    protected NodeShardsResult buildNodeShardsResult(
        ShardRouting shard,
        boolean matchAnyShard,
        Set<String> ignoreNodes,
        Set<String> inSyncAllocationIds,
        NodeShardStates shardState,
        Logger logger
    ) {
        NodeShardStates nodeShardStates = new NodeShardStates();
        int numberOfAllocationsFound = 0;
        for (NodeShardState nodeShardState : shardState.getNodeShardStates()) {
            DiscoveryNode node = nodeShardState.getNode();
            String allocationId = nodeShardState.allocationId();

            if (ignoreNodes.contains(node.getId())) {
                continue;
            }

            if (nodeShardState.storeException() == null) {
                if (allocationId == null) {
                    logger.trace("[{}] on node [{}] has no shard state information", shard, nodeShardState.getNode());
                } else {
                    logger.trace("[{}] on node [{}] has allocation id [{}]", shard, nodeShardState.getNode(), allocationId);
                }
            } else {
                final String finalAllocationId = allocationId;
                if (nodeShardState.storeException() instanceof ShardLockObtainFailedException) {
                    logger.trace(
                            () -> new ParameterizedMessage(
                                    "[{}] on node [{}] has allocation id [{}] but the store can not be "
                                            + "opened as it's locked, treating as valid shard",
                                    shard,
                                    nodeShardState.getNode(),
                                    finalAllocationId
                            ),
                            nodeShardState.storeException()
                    );
                } else {
                    logger.trace(
                            () -> new ParameterizedMessage(
                                    "[{}] on node [{}] has allocation id [{}] but the store can not be " + "opened, treating as no allocation id",
                                    shard,
                                    nodeShardState.getNode(),
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
                    nodeShardStates.getNodeShardStates().add(nodeShardState);
                }
            }
        }

        nodeShardStates.getNodeShardStates().sort(createActiveShardComparator(matchAnyShard, inSyncAllocationIds));

        if (logger.isTraceEnabled()) {
            logger.trace(
                "{} candidates for allocation: {}",
                shard,
                nodeShardStates.getNodeShardStates().stream().map(s -> s.getNode().getName()).collect(Collectors.joining(", "))
            );
        }
        return new NodeShardsResult(nodeShardStates, numberOfAllocationsFound);
    }

    private static Comparator<NodeShardState> createActiveShardComparator(boolean matchAnyShard, Set<String> inSyncAllocationIds) {
        /**
         * Orders the active shards copies based on below comparators
         * 1. No store exception i.e. shard copy is readable
         * 2. Prefer previous primary shard
         * 3. Prefer shard copy with the highest replication checkpoint. It is NO-OP for doc rep enabled indices.
         */
        final Comparator<NodeShardState> comparator; // allocation preference
        if (matchAnyShard) {
            // prefer shards with matching allocation ids
            Comparator<NodeShardState> matchingAllocationsFirst = Comparator.comparing(
                (NodeShardState state) -> inSyncAllocationIds.contains(state.allocationId())
            ).reversed();
            comparator = matchingAllocationsFirst.thenComparing(NO_STORE_EXCEPTION_FIRST_COMPARATOR)
                .thenComparing(PRIMARY_FIRST_COMPARATOR)
                .thenComparing(HIGHEST_REPLICATION_CHECKPOINT_FIRST_COMPARATOR);
        } else {
            comparator = NO_STORE_EXCEPTION_FIRST_COMPARATOR.thenComparing(PRIMARY_FIRST_COMPARATOR)
                .thenComparing(HIGHEST_REPLICATION_CHECKPOINT_FIRST_COMPARATOR);
        }

        return comparator;
    }

    /**
     * Split the list of node shard states into groups yes/no/throttle based on allocation deciders
     */
    protected static NodesToAllocate buildNodesToAllocate(
        RoutingAllocation allocation,
        NodeShardStates nodeShardStates,
        ShardRouting shardRouting,
        boolean forceAllocate
    ) {
        List<DecidedNode> yesNodeShards = new ArrayList<>();
        List<DecidedNode> throttledNodeShards = new ArrayList<>();
        List<DecidedNode> noNodeShards = new ArrayList<>();
        for (NodeShardState nodeShardState : nodeShardStates.getNodeShardStates()) {
            RoutingNode node = allocation.routingNodes().node(nodeShardState.getNode().getId());
            if (node == null) {
                continue;
            }

            Decision decision = forceAllocate
                    ? allocation.deciders().canForceAllocatePrimary(shardRouting, node, allocation)
                    : allocation.deciders().canAllocate(shardRouting, node, allocation);
            DecidedNode decidedNode = new DecidedNode(nodeShardState, decision);
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

    protected abstract FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation);

    /**
     * This class encapsulates the result of a call to {@link #buildNodeShardsResult}
     */
    protected static class NodeShardsResult {
        final NodeShardStates orderedAllocationCandidates;
        final int allocationsFound;

        NodeShardsResult(NodeShardStates orderedAllocationCandidates, int allocationsFound) {
            this.orderedAllocationCandidates = orderedAllocationCandidates;
            this.allocationsFound = allocationsFound;
        }
    }

    /**
     * This class encapsulates the result of a call to {@link #buildNodesToAllocate(RoutingAllocation, NodeShardStates, ShardRouting, boolean)}
     * */
    protected static class NodesToAllocate {
        final List<? extends DecidedNode> yesNodeShards;
        final List<? extends DecidedNode> throttleNodeShards;
        final List<? extends DecidedNode> noNodeShards;

        NodesToAllocate(
            List<? extends DecidedNode> yesNodeShards,
            List<? extends DecidedNode> throttleNodeShards,
            List<? extends DecidedNode> noNodeShards
        ) {
            this.yesNodeShards = yesNodeShards;
            this.throttleNodeShards = throttleNodeShards;
            this.noNodeShards = noNodeShards;
        }
    }

    /**
     * This class encapsulates the shard state retrieved from a node and the decision that was made
     * by the allocator for allocating to the node that holds the shard copy.
     */
    protected static class DecidedNode {
        final NodeShardState nodeShardState;
        final Decision decision;

        protected DecidedNode(NodeShardState nodeShardState, Decision decision) {
            this.nodeShardState = nodeShardState;
            this.decision = decision;
        }
    }

    /**
     * The NodeShardState class represents the state of a node shard in a distributed system.
     * It includes several key data points about the shard state, such as its allocation ID,
     * whether it's a primary shard, any store exception, the replication checkpoint, and the
     * DiscoveryNode it belongs to.
     * <p>
     * This class is designed to be used in conjunction with the {@link NodeShardStates} class, which
     * manages multiple NodeShardState instances.
     */
    protected static class NodeShardState {
        // Allocation ID of the shard
        private final String allocationId;
        // Whether the shard is primary
        private final boolean primary;
        // Any store exception associated with the shard
        private final Exception storeException;
        // The replication checkpoint of the shard
        private final ReplicationCheckpoint replicationCheckpoint;
        // The DiscoveryNode the shard belongs to
        private final DiscoveryNode node;

        /**
         * Constructs a new NodeShardState with the given parameters.
         * @param node The DiscoveryNode the shard belongs to.
         * @param allocationId The allocation ID of the shard.
         * @param primary Whether the shard is a primary shard.
         * @param replicationCheckpoint The replication checkpoint of the shard.
         * @param storeException Any store exception associated with the shard.
         */
        public NodeShardState(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            Exception storeException
        ) {
            this.node = node;
            this.allocationId = allocationId;
            this.primary = primary;
            this.replicationCheckpoint = replicationCheckpoint;
            this.storeException = storeException;
        }

        /**
         * Returns the allocation ID of the shard.
         * @return The allocation ID of the shard.
         */
        public String allocationId() {
            return this.allocationId;
        }

        /**
         * Returns whether the shard is a primary shard.
         * @return True if the shard is a primary shard, false otherwise.
         */
        public boolean primary() {
            return this.primary;
        }

        /**
         * Returns the replication checkpoint of the shard.
         * @return The replication checkpoint of the shard.
         */
        public ReplicationCheckpoint replicationCheckpoint() {
            return this.replicationCheckpoint;
        }

        /**
         * Returns any store exception associated with the shard.
         * @return The store exception associated with the shard, or null if there isn't one.
         */
        public Exception storeException() {
            return this.storeException;
        }

        /**
         * Returns the DiscoveryNode the shard belongs to.
         * @return The DiscoveryNode the shard belongs to.
         */
        public DiscoveryNode getNode() {
            return this.node;
        }
    }

    /**
     * The NodeShardStates class manages pairs of {@link NodeShardState} and {@link DiscoveryNode}.
     * It uses a TreeMap to ensure that the entries are sorted based on the natural
     * ordering of the {@link NodeShardState} keys, or according to a provided Comparator.
     * <p>
     * The TreeMap is implemented using a Red-Black tree, which provides efficient
     * performance for common operations such as adding, removing, and retrieving
     * elements.
     * @see TreeMap
     */
    protected static class NodeShardStates {
        // List of entries to store NodeShardState
        private final List<NodeShardState> nodeShardStates;

        /**
         * Constructs a new NodeShardStates
         */
        public NodeShardStates() {
            this.nodeShardStates = new ArrayList<>();
        }

        /**
         * Returns the list of {@link NodeShardState}.
         */
        public List<NodeShardState> getNodeShardStates() {
            return this.nodeShardStates;
        }
    }
}
