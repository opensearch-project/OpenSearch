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

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.snapshots.SnapshotShardSizeInfo;
import org.opensearch.snapshots.SnapshotsInfoService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Collections.emptyMap;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;

public abstract class OpenSearchAllocationTestCase extends OpenSearchTestCase {
    private static final ClusterSettings EMPTY_CLUSTER_SETTINGS = new ClusterSettings(
        Settings.EMPTY,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
    );

    public static final SnapshotsInfoService SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES = () -> new SnapshotShardSizeInfo(Map.of()) {
        @Override
        public Long getShardSize(ShardRouting shardRouting) {
            assert shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
                : "Expecting a recovery source of type [SNAPSHOT] but got [" + shardRouting.recoverySource().getType() + ']';
            throw new UnsupportedOperationException();
        }
    };

    public static MockAllocationService createAllocationService() {
        return createAllocationService(Settings.Builder.EMPTY_SETTINGS);
    }

    public static MockAllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, random());
    }

    public static MockAllocationService createAllocationService(Settings settings, Random random) {
        return createAllocationService(settings, EMPTY_CLUSTER_SETTINGS, random);
    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings, Random random) {
        return new MockAllocationService(
            randomAllocationDeciders(settings, clusterSettings, random),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterInfoService clusterInfoService) {
        return new MockAllocationService(
            randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            clusterInfoService,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }

    public static MockAllocationService createAllocationService(Settings settings, GatewayAllocator gatewayAllocator) {
        return createAllocationService(settings, gatewayAllocator, SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES);
    }

    public static MockAllocationService createAllocationService(Settings settings, SnapshotsInfoService snapshotsInfoService) {
        return createAllocationService(settings, new TestGatewayAllocator(), snapshotsInfoService);
    }

    public static MockAllocationService createAllocationService(
        Settings settings,
        GatewayAllocator gatewayAllocator,
        SnapshotsInfoService snapshotsInfoService
    ) {
        return new MockAllocationService(
            randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
            gatewayAllocator,
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            snapshotsInfoService
        );
    }

    public static AllocationDeciders randomAllocationDeciders(Settings settings, ClusterSettings clusterSettings, Random random) {
        List<AllocationDecider> deciders = new ArrayList<>(
            ClusterModule.createAllocationDeciders(settings, clusterSettings, Collections.emptyList())
        );
        Collections.shuffle(deciders, random);
        return new AllocationDeciders(deciders);
    }

    protected static Set<DiscoveryNodeRole> CLUSTER_MANAGER_DATA_ROLES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );

    protected static DiscoveryNode newNode(String nodeId) {
        return newNode(nodeId, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeName, String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeName, nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_DATA_ROLES, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_DATA_ROLES, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeName, String nodeId, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(nodeName, nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Version version) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), CLUSTER_MANAGER_DATA_ROLES, version);
    }

    protected static ClusterState startRandomInitializingShard(ClusterState clusterState, AllocationService strategy) {
        List<ShardRouting> initializingShards = clusterState.getRoutingNodes().shardsWithState(INITIALIZING);
        return startInitialisingShardsAndReroute(strategy, clusterState, initializingShards);
    }

    protected static ClusterState startRandomInitializingShard(ClusterState clusterState, AllocationService strategy, String index) {
        List<ShardRouting> initializingShards = clusterState.getRoutingNodes().shardsWithState(index, INITIALIZING);
        return startInitialisingShardsAndReroute(strategy, clusterState, initializingShards);
    }

    private static ClusterState startInitialisingShardsAndReroute(
        AllocationService strategy,
        ClusterState clusterState,
        List<ShardRouting> initializingShards
    ) {
        if (initializingShards.isEmpty()) {
            return clusterState;
        }
        return startShardsAndReroute(strategy, clusterState, randomFrom(initializingShards));
    }

    protected static AllocationDeciders yesAllocationDeciders() {
        return new AllocationDeciders(
            Arrays.asList(
                new TestAllocateDecision(Decision.YES),
                new SameShardAllocationDecider(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                )
            )
        );
    }

    protected static AllocationDeciders noAllocationDeciders() {
        return new AllocationDeciders(Collections.singleton(new TestAllocateDecision(Decision.NO)));
    }

    protected static AllocationDeciders throttleAllocationDeciders() {
        return new AllocationDeciders(
            Arrays.asList(
                new TestAllocateDecision(Decision.THROTTLE),
                new SameShardAllocationDecider(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                )
            )
        );
    }

    protected static AllocationDeciders allocationDecidersForExcludeAPI(Settings settings) {
        return new AllocationDeciders(
            Arrays.asList(
                new TestAllocateDecision(Decision.YES),
                new SameShardAllocationDecider(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                new FilterAllocationDecider(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
            )
        );
    }

    protected ClusterState applyStartedShardsUntilNoChange(ClusterState clusterState, AllocationService service) {
        ClusterState lastClusterState;
        do {
            lastClusterState = clusterState;
            logger.debug("ClusterState: {}", clusterState.getRoutingNodes());
            clusterState = startInitializingShardsAndReroute(service, clusterState);
        } while (lastClusterState.equals(clusterState) == false);
        return clusterState;
    }

    /**
     * Mark all initializing shards as started, then perform a reroute (which may start some other shards initializing).
     *
     * @return the cluster state after completing the reroute.
     */
    public static ClusterState startInitializingShardsAndReroute(AllocationService allocationService, ClusterState clusterState) {
        return startShardsAndReroute(allocationService, clusterState, clusterState.routingTable().shardsWithState(INITIALIZING));
    }

    /**
     * Mark all initializing shards on the given node as started, then perform a reroute (which may start some other shards initializing).
     *
     * @return the cluster state after completing the reroute.
     */
    public static ClusterState startInitializingShardsAndReroute(
        AllocationService allocationService,
        ClusterState clusterState,
        RoutingNode routingNode
    ) {
        return startShardsAndReroute(allocationService, clusterState, routingNode.shardsWithState(INITIALIZING));
    }

    /**
     * Mark all initializing shards for the given index as started, then perform a reroute (which may start some other shards initializing).
     *
     * @return the cluster state after completing the reroute.
     */
    public static ClusterState startInitializingShardsAndReroute(
        AllocationService allocationService,
        ClusterState clusterState,
        String index
    ) {
        return startShardsAndReroute(
            allocationService,
            clusterState,
            clusterState.routingTable().index(index).shardsWithState(INITIALIZING)
        );
    }

    /**
     * Mark the given shards as started, then perform a reroute (which may start some other shards initializing).
     *
     * @return the cluster state after completing the reroute.
     */
    public static ClusterState startShardsAndReroute(
        AllocationService allocationService,
        ClusterState clusterState,
        ShardRouting... initializingShards
    ) {
        return startShardsAndReroute(allocationService, clusterState, Arrays.asList(initializingShards));
    }

    /**
     * Mark the given shards as started, then perform a reroute (which may start some other shards initializing).
     *
     * @return the cluster state after completing the reroute.
     */
    public static ClusterState startShardsAndReroute(
        AllocationService allocationService,
        ClusterState clusterState,
        List<ShardRouting> initializingShards
    ) {
        return allocationService.reroute(allocationService.applyStartedShards(clusterState, initializingShards), "reroute after starting");
    }

    protected RoutingAllocation newRoutingAllocation(AllocationDeciders deciders, ClusterState state) {
        RoutingAllocation allocation = new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        return allocation;
    }

    public static class TestAllocateDecision extends AllocationDecider {

        private final Decision decision;

        public TestAllocateDecision(Decision decision) {
            this.decision = decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
            return decision;
        }
    }

    /**
     * Utility class to show shards distribution across nodes.
     */
    public static class ShardAllocations {

        private static final String separator = "===================================================";
        private static final String ONE_LINE_RETURN = "\n";
        private static final String TWO_LINE_RETURN = "\n\n";

        /**
         Store shard primary/replica shard count against a node.
         String: NodeId
         int[]: tuple storing primary shard count in 0th index and replica's in 1st
         */
        static TreeMap<String, int[]> nodeToShardCountMap = new TreeMap<>();

        /**
         * Helper map containing NodeName to NodeId
         */
        static TreeMap<String, String> nameToNodeId = new TreeMap<>();

        /*
        Unassigned array containing primary at 0, replica at 1
         */
        static int[] unassigned = new int[2];

        static int[] totalShards = new int[2];

        private final static String printShardAllocationWithHeader(int[] shardCount) {
            StringBuffer sb = new StringBuffer();
            Formatter formatter = new Formatter(sb, Locale.getDefault());
            formatter.format("%-20s %-20s\n", "P", shardCount[0]);
            formatter.format("%-20s %-20s\n", "R", shardCount[1]);
            return sb.toString();
        }

        private static void reset() {
            nodeToShardCountMap.clear();
            nameToNodeId.clear();
            totalShards[0] = totalShards[1] = 0;
            unassigned[0] = unassigned[1] = 0;
        }

        private static void buildMap(ClusterState inputState) {
            reset();
            for (RoutingNode node : inputState.getRoutingNodes()) {
                if (node.node().getName() != null && node.node().getName().isEmpty() == false) {
                    nameToNodeId.putIfAbsent(node.node().getName(), node.nodeId());
                } else {
                    nameToNodeId.putIfAbsent(node.nodeId(), node.nodeId());
                }
                nodeToShardCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
            }
            for (ShardRouting shardRouting : inputState.routingTable().allShards()) {
                // Fetch shard to update. Initialize local array
                updateMap(nodeToShardCountMap, shardRouting);
            }
        }

        private static void updateMap(TreeMap<String, int[]> mapToUpdate, ShardRouting shardRouting) {
            int[] shard;
            shard = shardRouting.assignedToNode() ? mapToUpdate.get(shardRouting.currentNodeId()) : unassigned;
            // Update shard type count
            if (shardRouting.primary()) {
                shard[0]++;
                totalShards[0]++;
            } else {
                shard[1]++;
                totalShards[1]++;
            }
            // For assigned shards, put back counter
            if (shardRouting.assignedToNode()) mapToUpdate.put(shardRouting.currentNodeId(), shard);
        }

        private static String allocation() {
            StringBuffer sb = new StringBuffer();
            sb.append(TWO_LINE_RETURN + separator + ONE_LINE_RETURN);
            Formatter formatter = new Formatter(sb, Locale.getDefault());
            for (Map.Entry<String, String> entry : nameToNodeId.entrySet()) {
                String nodeId = nameToNodeId.get(entry.getKey());
                formatter.format("%-20s\n", entry.getKey().toUpperCase(Locale.getDefault()));
                sb.append(printShardAllocationWithHeader(nodeToShardCountMap.get(nodeId)));
            }
            sb.append(ONE_LINE_RETURN);
            formatter.format("%-20s (P)%-5s (R)%-5s\n\n", "Unassigned ", unassigned[0], unassigned[1]);
            formatter.format("%-20s (P)%-5s (R)%-5s\n\n", "Total Shards", totalShards[0], totalShards[1]);
            return sb.toString();
        }

        public static String printShardDistribution(ClusterState state) {
            buildMap(state);
            return allocation();
        }
    }

    /** A lock {@link AllocationService} allowing tests to override time */
    protected static class MockAllocationService extends AllocationService {

        private volatile long nanoTimeOverride = -1L;

        public MockAllocationService(
            AllocationDeciders allocationDeciders,
            GatewayAllocator gatewayAllocator,
            ShardsAllocator shardsAllocator,
            ClusterInfoService clusterInfoService,
            SnapshotsInfoService snapshotsInfoService
        ) {
            super(allocationDeciders, gatewayAllocator, shardsAllocator, clusterInfoService, snapshotsInfoService);
        }

        public void setNanoTimeOverride(long nanoTime) {
            this.nanoTimeOverride = nanoTime;
        }

        @Override
        protected long currentNanoTime() {
            return nanoTimeOverride == -1L ? super.currentNanoTime() : nanoTimeOverride;
        }
    }

    /**
     * Mocks behavior in ReplicaShardAllocator to remove delayed shards from list of unassigned shards so they don't get reassigned yet.
     */
    protected static class DelayedShardsMockGatewayAllocator extends GatewayAllocator {
        public DelayedShardsMockGatewayAllocator() {}

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
            // no-op
        }

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
            // no-op
        }

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {
            // no-op
        }

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
            // no-op
        }

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            if (shardRouting.primary() || shardRouting.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                return;
            }
            if (shardRouting.unassignedInfo().isDelayed()) {
                unassignedAllocationHandler.removeAndIgnore(UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION, allocation.changes());
            }
        }

    }
}
