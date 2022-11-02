/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;

@SuppressForbidden(reason = "feature flag overrides")
public abstract class RemoteShardsBalancerBaseTestCase extends OpenSearchAllocationTestCase {
    protected static final String LOCAL_NODE_PREFIX = "local-only-node";
    protected static final String REMOTE_NODE_PREFIX = "remote-capable-node";
    protected static final String LOCAL_IDX_PREFIX = "local-idx";
    protected static final String REMOTE_IDX_PREFIX = "remote-idx";
    protected static final Set<DiscoveryNodeRole> MANAGER_DATA_ROLES = Set.of(
        DiscoveryNodeRole.CLUSTER_MANAGER_ROLE,
        DiscoveryNodeRole.DATA_ROLE
    );
    protected static final Set<DiscoveryNodeRole> SEARCH_DATA_ROLES = Set.of(
        DiscoveryNodeRole.CLUSTER_MANAGER_ROLE,
        DiscoveryNodeRole.DATA_ROLE,
        DiscoveryNodeRole.SEARCH_ROLE
    );

    protected static final int PRIMARIES = 5;
    protected static final int REPLICAS = 1;
    private static final int MAX_REROUTE_ITERATIONS = 1000;

    protected ClusterSettings EMPTY_CLUSTER_SETTINGS = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    @BeforeClass
    public static void setup() {
        System.setProperty(FeatureFlags.SEARCHABLE_SNAPSHOT, "true");
    }

    @AfterClass
    public static void teardown() {
        System.setProperty(FeatureFlags.SEARCHABLE_SNAPSHOT, "false");
    }

    public String getNodeId(int id, boolean isRemote, String prefix) {
        if (isRemote) {
            return REMOTE_NODE_PREFIX + "-" + prefix + "-" + id;
        }
        return LOCAL_NODE_PREFIX + "-" + prefix + "-" + id;
    }

    public String getNodeId(int id, boolean isRemote) {
        return getNodeId(id, isRemote, "");
    }

    public String getIndexName(int id, boolean isRemote, String prefix) {
        if (isRemote) {
            return REMOTE_IDX_PREFIX + "-" + prefix + "-" + id;
        }
        return LOCAL_IDX_PREFIX + "-" + prefix + "-" + id;
    }

    public String getIndexName(int id, boolean isRemote) {
        return getIndexName(id, isRemote, "");
    }

    public RoutingAllocation getRoutingAllocation(ClusterState clusterState, RoutingNodes routingNodes) {
        return new RoutingAllocation(
            randomAllocationDeciders(Settings.Builder.EMPTY_SETTINGS, EMPTY_CLUSTER_SETTINGS, random()),
            routingNodes,
            clusterState,
            EmptyClusterInfoService.INSTANCE.getClusterInfo(),
            null,
            System.nanoTime()
        );
    }

    private Map<String, String> createNodeAttributes(String nodeId) {
        Map<String, String> attr = new HashMap<>();
        attr.put("name", nodeId);
        attr.put("node_id", nodeId);
        return attr;
    }

    public ClusterState addNodes(ClusterState clusterState, int nodeCount, boolean isRemote) {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder(clusterState.nodes());
        for (int i = 0; i < nodeCount; i++) {
            String id = getNodeId(i, isRemote, "new");
            nb.add(newNode(id, id, isRemote ? SEARCH_DATA_ROLES : MANAGER_DATA_ROLES));
        }
        return ClusterState.builder(clusterState).nodes(nb.build()).build();
    }

    public ClusterState addNodeWithIP(ClusterState clusterState, int nodeId, boolean isRemote, String IP) throws UnknownHostException {
        TransportAddress ipAddress = new TransportAddress(Inet4Address.getByName(IP), 9200);
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder(clusterState.nodes());
        String id = getNodeId(nodeId, isRemote, "new");
        nb.add(
            new DiscoveryNode(
                id,
                id,
                ipAddress,
                createNodeAttributes(id),
                isRemote ? SEARCH_DATA_ROLES : MANAGER_DATA_ROLES,
                Version.CURRENT
            )
        );
        return ClusterState.builder(clusterState).nodes(nb.build()).build();
    }

    public ClusterState terminateNodes(ClusterState clusterState, AllocationService service, List<String> nodesToTerminate) {
        if (nodesToTerminate.isEmpty()) {
            return clusterState;
        }
        logger.info("Terminating following nodes from cluster: [{}]", nodesToTerminate);
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder(clusterState.nodes());
        nodesToTerminate.forEach(nb::remove);
        clusterState = ClusterState.builder(clusterState).nodes(nb.build()).build();
        clusterState = service.disassociateDeadNodes(clusterState, false, "nodes-terminated");
        return clusterState;
    }

    public ClusterState createInitialCluster(int localOnlyNodes, int remoteCapableNodes, int localIndices, int remoteIndices) {
        Metadata.Builder mb = Metadata.builder();
        for (int i = 0; i < localIndices; i++) {
            mb.put(
                IndexMetadata.builder(getIndexName(i, false))
                    .settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0"))
                    .numberOfShards(PRIMARIES)
                    .numberOfReplicas(REPLICAS)
            );
        }

        for (int i = 0; i < remoteIndices; i++) {
            mb.put(
                IndexMetadata.builder(getIndexName(i, true))
                    .settings(
                        settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0")
                            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                    )
                    .numberOfShards(PRIMARIES)
                    .numberOfReplicas(REPLICAS)
            );
        }
        Metadata metadata = mb.build();

        RoutingTable.Builder rb = RoutingTable.builder();
        for (int i = 0; i < localIndices; i++) {
            rb.addAsNew(metadata.index(getIndexName(i, false)));
        }
        for (int i = 0; i < remoteIndices; i++) {
            rb.addAsNew(metadata.index(getIndexName(i, true)));
        }
        RoutingTable routingTable = rb.build();

        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 0; i < localOnlyNodes; i++) {
            String name = getNodeId(i, false);
            nb.add(newNode(name, name, MANAGER_DATA_ROLES));
        }
        for (int i = 0; i < remoteCapableNodes; i++) {
            String name = getNodeId(i, true);
            nb.add(newNode(name, name, SEARCH_DATA_ROLES));
        }
        DiscoveryNodes nodes = nb.build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(nodes).build();
    }

    protected ClusterState createRemoteIndex(ClusterState state, String indexName) {
        Metadata metadata = Metadata.builder(state.metadata())
            .put(
                IndexMetadata.builder(indexName)
                    .settings(
                        settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "5m")
                            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                    )
                    .numberOfShards(PRIMARIES)
                    .numberOfReplicas(REPLICAS)
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder(state.routingTable()).addAsNew(metadata.index(indexName)).build();
        return ClusterState.builder(state).metadata(metadata).routingTable(routingTable).build();
    }

    private AllocationDeciders remoteAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        List<AllocationDecider> deciders = new ArrayList<>(
            ClusterModule.createAllocationDeciders(settings, clusterSettings, Collections.emptyList())
        );
        Collections.shuffle(deciders, random());
        return new AllocationDeciders(deciders);
    }

    public AllocationService createRemoteCapableAllocationService() {
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        return new OpenSearchAllocationTestCase.MockAllocationService(
            randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
            new TestGatewayAllocator(),
            createShardAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }

    public AllocationService createRemoteCapableAllocationService(String excludeNodes) {
        Settings settings = Settings.builder().put("cluster.routing.allocation.exclude.node_id", excludeNodes).build();
        return new MockAllocationService(
            randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
            new TestGatewayAllocator(),
            createShardAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }

    public AllocationDeciders createAllocationDeciders() {
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        return randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random());

    }

    public ClusterState allocateShardsAndBalance(ClusterState clusterState, AllocationService service) {
        int iterations = 0;
        do {
            clusterState = service.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
            clusterState = service.reroute(clusterState, "reroute");
            iterations++;
        } while (!clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() && iterations < MAX_REROUTE_ITERATIONS);
        return clusterState;
    }

    public int totalShards(int indices) {
        return indices * PRIMARIES * (REPLICAS + 1);
    }

    public int totalPrimaries(int indices) {
        return indices * PRIMARIES;
    }

    public ShardsAllocator createShardAllocator(Settings settings) {
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new BalancedShardsAllocator(settings, clusterSettings);
    }

    /**
     * ClusterInfo that always reports /dev/null for the shards' data paths.
     */
    public static class DevNullClusterInfo extends ClusterInfo {
        public DevNullClusterInfo(
            ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage,
            ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage,
            ImmutableOpenMap<String, Long> shardSizes
        ) {
            super(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, null, ImmutableOpenMap.of());
        }

        @Override
        public String getDataPath(ShardRouting shardRouting) {
            return "/dev/null";
        }
    }
}
