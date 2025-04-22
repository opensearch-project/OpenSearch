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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.metadata.AutoExpandReplicas;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.gateway.PriorityComparator;
import org.opensearch.gateway.ShardsBatchGatewayAllocator;
import org.opensearch.snapshots.SnapshotsInfoService;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.cluster.routing.allocation.ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE;

/**
 * This service manages the node allocation of a cluster. For this reason the
 * {@link AllocationService} keeps {@link AllocationDeciders} to choose nodes
 * for shard allocation. This class also manages new nodes joining the cluster
 * and rerouting of shards.
 *
 * @opensearch.internal
 */
public class AllocationService {

    private static final Logger logger = LogManager.getLogger(AllocationService.class);

    private final AllocationDeciders allocationDeciders;
    private Settings settings;
    private Map<String, ExistingShardsAllocator> existingShardsAllocators;
    private final ShardsAllocator shardsAllocator;
    private final ClusterInfoService clusterInfoService;
    private SnapshotsInfoService snapshotsInfoService;
    private final ClusterManagerMetrics clusterManagerMetrics;

    // only for tests that use the GatewayAllocator as the unique ExistingShardsAllocator
    public AllocationService(
        AllocationDeciders allocationDeciders,
        GatewayAllocator gatewayAllocator,
        ShardsAllocator shardsAllocator,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService
    ) {
        this(
            allocationDeciders,
            shardsAllocator,
            clusterInfoService,
            snapshotsInfoService,
            new ClusterManagerMetrics(NoopMetricsRegistry.INSTANCE)
        );
        setExistingShardsAllocators(Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator));
    }

    public AllocationService(
        AllocationDeciders allocationDeciders,
        ShardsAllocator shardsAllocator,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        this(allocationDeciders, shardsAllocator, clusterInfoService, snapshotsInfoService, Settings.EMPTY, clusterManagerMetrics);
    }

    public AllocationService(
        AllocationDeciders allocationDeciders,
        ShardsAllocator shardsAllocator,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        Settings settings,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocator = shardsAllocator;
        this.clusterInfoService = clusterInfoService;
        this.snapshotsInfoService = snapshotsInfoService;
        this.settings = settings;
        this.clusterManagerMetrics = clusterManagerMetrics;
    }

    /**
     * Inject the {@link ExistingShardsAllocator}s to use. May only be called once.
     */
    public void setExistingShardsAllocators(Map<String, ExistingShardsAllocator> existingShardsAllocators) {
        assert this.existingShardsAllocators == null : "cannot set allocators " + existingShardsAllocators + " twice";
        assert existingShardsAllocators.isEmpty() == false : "must add at least one ExistingShardsAllocator";
        this.existingShardsAllocators = Collections.unmodifiableMap(existingShardsAllocators);
    }

    /**
     * Applies the started shards. Note, only initializing ShardRouting instances that exist in the routing table should be
     * provided as parameter and no duplicates should be contained.
     * <p>
     * If the same instance of the {@link ClusterState} is returned, then no change has been made.</p>
     */
    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        assert assertInitialized();
        if (startedShards.isEmpty()) {
            return clusterState;
        }
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            routingNodes,
            clusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime()
        );
        // as starting a primary relocation target can reinitialize replica shards, start replicas first
        startedShards = new ArrayList<>(startedShards);
        startedShards.sort(Comparator.comparing(ShardRouting::primary));
        applyStartedShards(allocation, startedShards);
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyStartedShards(startedShards, allocation);
        }
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
        String startedShardsAsString = firstListElementsToCommaDelimitedString(
            startedShards,
            s -> s.shardId().toString(),
            logger.isDebugEnabled()
        );
        return buildResultAndLogHealthChange(clusterState, allocation, "shards started [" + startedShardsAsString + "]");
    }

    protected ClusterState buildResultAndLogHealthChange(ClusterState oldState, RoutingAllocation allocation, String reason) {
        ClusterState newState = buildResult(oldState, allocation);

        logClusterHealthStateChange(
            new ClusterStateHealth(oldState, ClusterHealthRequest.Level.CLUSTER),
            new ClusterStateHealth(newState, ClusterHealthRequest.Level.CLUSTER),
            reason
        );

        return newState;
    }

    private ClusterState buildResult(ClusterState oldState, RoutingAllocation allocation) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata);
        final RestoreInProgress restoreInProgress = allocation.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            RestoreInProgress updatedRestoreInProgress = allocation.updateRestoreInfoWithRoutingChanges(restoreInProgress);
            if (updatedRestoreInProgress != restoreInProgress) {
                final Map<String, ClusterState.Custom> customsBuilder = new HashMap<>(allocation.getCustoms());
                customsBuilder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
                newStateBuilder.customs(customsBuilder);
            }
        }
        return newStateBuilder.build();
    }

    // Used for testing
    public ClusterState applyFailedShard(ClusterState clusterState, ShardRouting failedShard, boolean markAsStale) {
        return applyFailedShards(clusterState, singletonList(new FailedShard(failedShard, null, null, markAsStale)), emptyList());
    }

    // Used for testing
    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards) {
        return applyFailedShards(clusterState, failedShards, emptyList());
    }

    /**
     * Applies the failed shards. Note, only assigned ShardRouting instances that exist in the routing table should be
     * provided as parameter. Also applies a list of allocation ids to remove from the in-sync set for shard copies for which there
     * are no routing entries in the routing table.
     *
     * <p>
     * If the same instance of ClusterState is returned, then no change has been made.</p>
     */
    public ClusterState applyFailedShards(
        final ClusterState clusterState,
        final List<FailedShard> failedShards,
        final List<StaleShard> staleShards
    ) {
        assert assertInitialized();
        if (staleShards.isEmpty() && failedShards.isEmpty()) {
            return clusterState;
        }
        ClusterState tmpState = IndexMetadataUpdater.removeStaleIdsWithoutRoutings(clusterState, staleShards, logger);

        RoutingNodes routingNodes = getMutableRoutingNodes(tmpState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        long currentNanoTime = currentNanoTime();
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            routingNodes,
            tmpState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime
        );

        for (FailedShard failedShardEntry : failedShards) {
            ShardRouting shardToFail = failedShardEntry.getRoutingEntry();
            IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardToFail.shardId().getIndex());
            allocation.addIgnoreShardForNode(shardToFail.shardId(), shardToFail.currentNodeId());
            // failing a primary also fails initializing replica shards, re-resolve ShardRouting
            ShardRouting failedShard = routingNodes.getByAllocationId(shardToFail.shardId(), shardToFail.allocationId().getId());
            if (failedShard != null) {
                if (failedShard != shardToFail) {
                    logger.trace(
                        "{} shard routing modified in an earlier iteration (previous: {}, current: {})",
                        shardToFail.shardId(),
                        shardToFail,
                        failedShard
                    );
                }
                int failedAllocations = failedShard.unassignedInfo() != null ? failedShard.unassignedInfo().getNumFailedAllocations() : 0;
                final Set<String> failedNodeIds;
                if (failedShard.unassignedInfo() != null) {
                    failedNodeIds = new HashSet<>(failedShard.unassignedInfo().getFailedNodeIds().size() + 1);
                    failedNodeIds.addAll(failedShard.unassignedInfo().getFailedNodeIds());
                    failedNodeIds.add(failedShard.currentNodeId());
                } else {
                    failedNodeIds = Collections.emptySet();
                }
                String message = "failed shard on node [" + shardToFail.currentNodeId() + "]: " + failedShardEntry.getMessage();
                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    UnassignedInfo.Reason.ALLOCATION_FAILED,
                    message,
                    failedShardEntry.getFailure(),
                    failedAllocations + 1,
                    currentNanoTime,
                    System.currentTimeMillis(),
                    false,
                    UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                    failedNodeIds
                );
                if (failedShardEntry.markAsStale()) {
                    allocation.removeAllocationId(failedShard);
                }
                logger.warn(new ParameterizedMessage("failing shard [{}]", failedShardEntry), failedShardEntry.getFailure());
                routingNodes.failShard(logger, failedShard, unassignedInfo, indexMetadata, allocation.changes());
            } else {
                logger.trace("{} shard routing failed in an earlier iteration (routing: {})", shardToFail.shardId(), shardToFail);
            }
        }
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyFailedShards(failedShards, allocation);
        }

        reroute(allocation);
        String failedShardsAsString = firstListElementsToCommaDelimitedString(
            failedShards,
            s -> s.getRoutingEntry().shardId().toString(),
            logger.isDebugEnabled()
        );
        return buildResultAndLogHealthChange(clusterState, allocation, "shards failed [" + failedShardsAsString + "]");
    }

    /**
     * unassigned an shards that are associated with nodes that are no longer part of the cluster, potentially promoting replicas
     * if needed.
     */
    public ClusterState disassociateDeadNodes(ClusterState clusterState, boolean reroute, String reason) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            routingNodes,
            clusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime()
        );

        // first, clear from the shards any node id they used to belong to that is now dead
        disassociateDeadNodes(allocation);

        if (allocation.routingNodesChanged()) {
            clusterState = buildResult(clusterState, allocation);
        }
        if (reroute) {
            return reroute(clusterState, reason);
        } else {
            return clusterState;
        }
    }

    /**
     * Checks if there are replicas with the auto-expand feature that need to be adapted.
     * Returns an updated cluster state if changes were necessary, or the identical cluster if no changes were required.
     */
    public ClusterState adaptAutoExpandReplicas(ClusterState clusterState) {
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            clusterState.getRoutingNodes(),
            clusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime()
        );
        final Map<Integer, List<String>> autoExpandReplicaChanges = AutoExpandReplicas.getAutoExpandReplicaChanges(
            clusterState.metadata(),
            allocation
        );
        if (autoExpandReplicaChanges.isEmpty()) {
            return clusterState;
        } else {
            final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());
            final Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            for (Map.Entry<Integer, List<String>> entry : autoExpandReplicaChanges.entrySet()) {
                final int numberOfReplicas = entry.getKey();
                final String[] indices = entry.getValue().toArray(new String[0]);
                // we do *not* update the in sync allocation ids as they will be removed upon the first index
                // operation which make these copies stale
                routingTableBuilder.updateNumberOfReplicas(numberOfReplicas, indices);
                metadataBuilder.updateNumberOfReplicas(numberOfReplicas, indices);
                // update settings version for each index
                for (final String index : indices) {
                    final IndexMetadata indexMetadata = metadataBuilder.get(index);
                    final IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexMetadata).settingsVersion(
                        1 + indexMetadata.getSettingsVersion()
                    );
                    metadataBuilder.put(indexMetadataBuilder);
                }
                logger.info("updating number_of_replicas to [{}] for indices {}", numberOfReplicas, indices);
            }
            final ClusterState fixedState = ClusterState.builder(clusterState)
                .routingTable(routingTableBuilder.build())
                .metadata(metadataBuilder)
                .build();
            assert AutoExpandReplicas.getAutoExpandReplicaChanges(fixedState.metadata(), allocation).isEmpty();
            return fixedState;
        }
    }

    /**
     * Removes delay markers from unassigned shards based on current time stamp.
     */
    private void removeDelayMarkers(RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
        final Metadata metadata = allocation.metadata();
        while (unassignedIterator.hasNext()) {
            ShardRouting shardRouting = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            if (unassignedInfo.isDelayed()) {
                final long newComputedLeftDelayNanos = unassignedInfo.getRemainingDelay(
                    allocation.getCurrentNanoTime(),
                    metadata.getIndexSafe(shardRouting.index()).getSettings()
                );
                if (newComputedLeftDelayNanos == 0) {
                    unassignedIterator.updateUnassigned(
                        new UnassignedInfo(
                            unassignedInfo.getReason(),
                            unassignedInfo.getMessage(),
                            unassignedInfo.getFailure(),
                            unassignedInfo.getNumFailedAllocations(),
                            unassignedInfo.getUnassignedTimeInNanos(),
                            unassignedInfo.getUnassignedTimeInMillis(),
                            false,
                            unassignedInfo.getLastAllocationStatus(),
                            unassignedInfo.getFailedNodeIds()
                        ),
                        shardRouting.recoverySource(),
                        allocation.changes()
                    );
                }
            }
        }
    }

    /**
     * Reset failed allocation counter for unassigned shards
     */
    private void resetFailedAllocationCounter(RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shardRouting = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            unassignedIterator.updateUnassigned(
                new UnassignedInfo(
                    unassignedInfo.getNumFailedAllocations() > 0 ? UnassignedInfo.Reason.MANUAL_ALLOCATION : unassignedInfo.getReason(),
                    unassignedInfo.getMessage(),
                    unassignedInfo.getFailure(),
                    0,
                    unassignedInfo.getUnassignedTimeInNanos(),
                    unassignedInfo.getUnassignedTimeInMillis(),
                    unassignedInfo.isDelayed(),
                    unassignedInfo.getLastAllocationStatus(),
                    Collections.emptySet()
                ),
                shardRouting.recoverySource(),
                allocation.changes()
            );
        }
    }

    /**
     * Internal helper to cap the number of elements in a potentially long list for logging.
     *
     * @param elements  The elements to log. May be any non-null list. Must not be null.
     * @param formatter A function that can convert list elements to a String. Must not be null.
     * @param <T>       The list element type.
     * @return A comma-separated string of the first few elements.
     */
    public static <T> String firstListElementsToCommaDelimitedString(
        List<T> elements,
        Function<T, String> formatter,
        boolean isDebugEnabled
    ) {
        final int maxNumberOfElements = 10;
        if (isDebugEnabled || elements.size() <= maxNumberOfElements) {
            return elements.stream().map(formatter).collect(Collectors.joining(", "));
        } else {
            return elements.stream().limit(maxNumberOfElements).map(formatter).collect(Collectors.joining(", "))
                + ", ... ["
                + elements.size()
                + " items in total]";
        }
    }

    public CommandsResult reroute(final ClusterState clusterState, AllocationCommands commands, boolean explain, boolean retryFailed) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // we don't shuffle the unassigned shards here, to try and get as close as possible to
        // a consistent result of the effect the commands have on the routing
        // this allows systems to dry run the commands, see the resulting cluster state, and act on it
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            routingNodes,
            clusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime()
        );
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);

        if (retryFailed) {
            resetFailedAllocationCounter(allocation);
        }

        RoutingExplanations explanations = commands.execute(allocation, explain);
        // we revert the ignore disable flag, since when rerouting, we want the original setting to take place
        allocation.ignoreDisable(false);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        reroute(allocation);
        return new CommandsResult(explanations, buildResultAndLogHealthChange(clusterState, allocation, "reroute commands"));
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p>
     * If the same instance of ClusterState is returned, then no change has been made.
     */
    public ClusterState reroute(ClusterState clusterState, String reason) {
        ClusterState fixedClusterState = adaptAutoExpandReplicas(clusterState);

        RoutingNodes routingNodes = getMutableRoutingNodes(fixedClusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            routingNodes,
            fixedClusterState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            currentNanoTime()
        );
        reroute(allocation);
        if (fixedClusterState == clusterState && allocation.routingNodesChanged() == false) {
            return clusterState;
        }
        return buildResultAndLogHealthChange(clusterState, allocation, reason);
    }

    private void logClusterHealthStateChange(ClusterStateHealth previousStateHealth, ClusterStateHealth newStateHealth, String reason) {
        ClusterHealthStatus previousHealth = previousStateHealth.getStatus();
        ClusterHealthStatus currentHealth = newStateHealth.getStatus();
        if (!previousHealth.equals(currentHealth)) {
            logger.info("Cluster health status changed from [{}] to [{}] (reason: [{}]).", previousHealth, currentHealth, reason);
        }
    }

    private boolean hasDeadNodes(RoutingAllocation allocation) {
        for (RoutingNode routingNode : allocation.routingNodes()) {
            if (allocation.nodes().getDataNodes().containsKey(routingNode.nodeId()) == false) {
                return true;
            }
        }
        return false;
    }

    private void reroute(RoutingAllocation allocation) {
        assert hasDeadNodes(allocation) == false : "dead nodes should be explicitly cleaned up. See disassociateDeadNodes";
        assert AutoExpandReplicas.getAutoExpandReplicaChanges(allocation.metadata(), allocation).isEmpty()
            : "auto-expand replicas out of sync with number of nodes in the cluster";
        assert assertInitialized();
        long rerouteStartTimeNS = System.nanoTime();
        removeDelayMarkers(allocation);

        allocateExistingUnassignedShards(allocation);  // try to allocate existing shard copies first
        shardsAllocator.allocate(allocation);
        clusterManagerMetrics.recordLatency(
            clusterManagerMetrics.rerouteHistogram,
            (double) Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - rerouteStartTimeNS))
        );
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
    }

    private void allocateExistingUnassignedShards(RoutingAllocation allocation) {
        allocation.routingNodes().unassigned().sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.beforeAllocation(allocation);
        }

        /*
         Use batch mode if enabled and there is no custom allocator set for Allocation service
         */
        if (isBatchModeEnabled(allocation)) {
            /*
             If we do not have any custom allocator set then we will be using ShardsBatchGatewayAllocator
             Currently AllocationService will not run any custom Allocator that implements allocateAllUnassignedShards
             */
            allocateAllUnassignedShards(allocation);
            return;
        }
        logger.warn("Falling back to single shard assignment since batch mode disable or multiple custom allocators set");

        final RoutingNodes.UnassignedShards.UnassignedIterator primaryIterator = allocation.routingNodes().unassigned().iterator();
        while (primaryIterator.hasNext()) {
            final ShardRouting shardRouting = primaryIterator.next();
            if (shardRouting.primary()) {
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, primaryIterator);
            }
        }

        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.afterPrimariesBeforeReplicas(allocation);
        }

        final RoutingNodes.UnassignedShards.UnassignedIterator replicaIterator = allocation.routingNodes().unassigned().iterator();
        while (replicaIterator.hasNext()) {
            final ShardRouting shardRouting = replicaIterator.next();
            if (shardRouting.primary() == false) {
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, replicaIterator);
            }
        }
    }

    private void allocateAllUnassignedShards(RoutingAllocation allocation) {
        ExistingShardsAllocator allocator = existingShardsAllocators.get(ShardsBatchGatewayAllocator.ALLOCATOR_NAME);
        Optional.ofNullable(allocator.allocateAllUnassignedShards(allocation, true)).ifPresent(Runnable::run);
        allocator.afterPrimariesBeforeReplicas(allocation);
        // Replicas Assignment
        Optional.ofNullable(allocator.allocateAllUnassignedShards(allocation, false)).ifPresent(Runnable::run);
    }

    private void disassociateDeadNodes(RoutingAllocation allocation) {
        for (Iterator<RoutingNode> it = allocation.routingNodes().mutableIterator(); it.hasNext();) {
            RoutingNode node = it.next();
            if (allocation.nodes().getDataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }
            // now, go over all the shards routing on the node, and fail them
            for (ShardRouting shardRouting : node.copyShards()) {
                final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
                boolean delayed = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetadata.getSettings()).nanos() > 0;
                UnassignedInfo unassignedInfo = new UnassignedInfo(
                    UnassignedInfo.Reason.NODE_LEFT,
                    "node_left [" + node.nodeId() + "]",
                    null,
                    0,
                    allocation.getCurrentNanoTime(),
                    System.currentTimeMillis(),
                    delayed,
                    AllocationStatus.NO_ATTEMPT,
                    Collections.emptySet()
                );
                allocation.routingNodes().failShard(logger, shardRouting, unassignedInfo, indexMetadata, allocation.changes());
            }
            // its a dead node, remove it, note, its important to remove it *after* we apply failed shard
            // since it relies on the fact that the RoutingNode exists in the list of nodes
            it.remove();
        }
    }

    private void applyStartedShards(RoutingAllocation routingAllocation, List<ShardRouting> startedShardEntries) {
        assert startedShardEntries.isEmpty() == false : "non-empty list of started shard entries expected";
        RoutingNodes routingNodes = routingAllocation.routingNodes();
        for (ShardRouting startedShard : startedShardEntries) {
            assert startedShard.initializing() : "only initializing shards can be started";
            assert routingAllocation.metadata().index(startedShard.shardId().getIndex()) != null
                : "shard started for unknown index (shard entry: " + startedShard + ")";
            assert startedShard == routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId())
                : "shard routing to start does not exist in routing table, expected: "
                    + startedShard
                    + " but was: "
                    + routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId());

            routingNodes.startShard(logger, startedShard, routingAllocation.changes());
        }
    }

    /**
     * Create a mutable {@link RoutingNodes}. This is a costly operation so this must only be called once!
     */
    private RoutingNodes getMutableRoutingNodes(ClusterState clusterState) {
        return new RoutingNodes(clusterState, false);
    }

    /** override this to control time based decisions during allocation */
    protected long currentNanoTime() {
        return System.nanoTime();
    }

    public void cleanCaches() {
        assert assertInitialized();
        existingShardsAllocators.values().forEach(ExistingShardsAllocator::cleanCaches);
    }

    public int getNumberOfInFlightFetches() {
        assert assertInitialized();
        return existingShardsAllocators.values().stream().mapToInt(ExistingShardsAllocator::getNumberOfInFlightFetches).sum();
    }

    public ShardAllocationDecision explainShardAllocation(ShardRouting shardRouting, RoutingAllocation allocation) {
        assert allocation.debugDecision();
        AllocateUnassignedDecision allocateDecision = shardRouting.unassigned()
            ? explainUnassignedShardAllocation(shardRouting, allocation)
            : AllocateUnassignedDecision.NOT_TAKEN;
        if (allocateDecision.isDecisionTaken()) {
            return new ShardAllocationDecision(allocateDecision, MoveDecision.NOT_TAKEN);
        } else {
            return shardsAllocator.decideShardAllocation(shardRouting, allocation);
        }
    }

    private AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        assert assertInitialized();
        final ExistingShardsAllocator existingShardsAllocator = getAllocatorForShard(shardRouting, routingAllocation);
        final AllocateUnassignedDecision decision = existingShardsAllocator.explainUnassignedShardAllocation(
            shardRouting,
            routingAllocation
        );
        if (decision.isDecisionTaken()) {
            return decision;
        }
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    private ExistingShardsAllocator getAllocatorForShard(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert assertInitialized();
        String allocatorName;
        if (isBatchModeEnabled(routingAllocation)) {
            allocatorName = ShardsBatchGatewayAllocator.ALLOCATOR_NAME;
        } else {
            allocatorName = ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
                routingAllocation.metadata().getIndexSafe(shardRouting.index()).getSettings()
            );
        }
        final ExistingShardsAllocator existingShardsAllocator = existingShardsAllocators.get(allocatorName);
        return existingShardsAllocator != null ? existingShardsAllocator : new NotFoundAllocator(allocatorName);
    }

    private boolean isBatchModeEnabled(RoutingAllocation routingAllocation) {
        return EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(settings)
            && routingAllocation.nodes().getMinNodeVersion().onOrAfter(Version.V_2_14_0)
            && existingShardsAllocators.size() == 2;
    }

    private boolean assertInitialized() {
        assert existingShardsAllocators != null : "must have set allocators first";
        return true;
    }

    private static class NotFoundAllocator implements ExistingShardsAllocator {
        private final String allocatorName;

        private NotFoundAllocator(String allocatorName) {
            this.allocatorName = allocatorName;
        }

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {}

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            unassignedAllocationHandler.removeAndIgnore(AllocationStatus.NO_VALID_SHARD_COPY, allocation.changes());
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation allocation) {
            assert unassignedShard.unassigned();
            assert allocation.debugDecision();
            final List<NodeAllocationResult> nodeAllocationResults = new ArrayList<>(allocation.nodes().getSize());
            for (DiscoveryNode discoveryNode : allocation.nodes()) {
                nodeAllocationResults.add(
                    new NodeAllocationResult(
                        discoveryNode,
                        null,
                        allocation.decision(
                            Decision.NO,
                            "allocator_plugin",
                            "finding the previous copies of this shard requires an allocator called [%s] but "
                                + "that allocator was not found; perhaps the corresponding plugin is not installed",
                            allocatorName
                        )
                    )
                );
            }
            return AllocateUnassignedDecision.no(AllocationStatus.NO_VALID_SHARD_COPY, nodeAllocationResults);
        }

        @Override
        public void cleanCaches() {}

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }

    /**
     * this class is used to describe results of applying a set of
     * {@link org.opensearch.cluster.routing.allocation.command.AllocationCommand}
     *
     * @opensearch.internal
     */
    public static class CommandsResult {

        private final RoutingExplanations explanations;

        private final ClusterState clusterState;

        /**
         * Creates a new {@link CommandsResult}
         * @param explanations Explanation for the reroute actions
         * @param clusterState Resulting cluster state
         */
        private CommandsResult(RoutingExplanations explanations, ClusterState clusterState) {
            this.clusterState = clusterState;
            this.explanations = explanations;
        }

        /**
         * Get the explanation of this result
         */
        public RoutingExplanations explanations() {
            return explanations;
        }

        /**
         * the resulting cluster state, after the commands were applied
         */
        public ClusterState getClusterState() {
            return clusterState;
        }
    }
}
