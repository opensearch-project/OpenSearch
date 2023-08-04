/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.util.CollectionUtils;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;

public abstract class OpenSearchAllocationWithConstraintsTestCase extends OpenSearchAllocationTestCase {

    protected MockAllocationService allocation;
    private ClusterState initialClusterState;
    protected ClusterState clusterState;
    private HashMap<String, Integer> indexShardCount;
    private HashMap<String, HashMap<String, Integer>> nodeShardsByIndex;
    private static final int MAX_REROUTE_STEPS_ALLOWED = 1500;

    @Before
    public void clearState() {
        allocation = null;
        initialClusterState = null;
        clusterState = null;
        indexShardCount = null;
        nodeShardsByIndex = null;
    }

    public static String shardIdentifierFromRouting(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }

    public void buildAllocationService() {
        Settings.Builder sb = Settings.builder();
        buildAllocationService(sb);
    }

    public void buildAllocationService(String excludeNodes) {
        logger.info("Excluding nodes: [{}]", excludeNodes);
        Settings.Builder sb = Settings.builder().put("cluster.routing.allocation.exclude._node_id", excludeNodes);

        buildAllocationService(sb);
    }

    public void buildZoneAwareAllocationService() {
        logger.info("Creating zone aware cluster");
        Settings.Builder sb = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone");

        buildAllocationService(sb);
    }

    public void buildAllocationService(Settings.Builder sb) {
        sb.put("cluster.routing.allocation.node_concurrent_recoveries", 1);
        sb.put("cluster.routing.allocation.cluster_concurrent_rebalance", 1);
        allocation = createAllocationService(sb.build());
    }

    public Metadata buildMetadata(Metadata.Builder mb, String indexPrefix, int numberOfIndices, int numberOfShards, int numberOfReplicas) {
        for (int i = 0; i < numberOfIndices; i++) {
            mb.put(
                IndexMetadata.builder(indexPrefix + i)
                    .settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0"))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            );
        }
        return mb.build();
    }

    public RoutingTable buildRoutingTable(RoutingTable.Builder rb, Metadata metadata, String indexPrefix, int numberOfIndices) {
        for (int i = 0; i < numberOfIndices; i++) {
            rb.addAsNew(metadata.index(indexPrefix + i));
        }
        return rb.build();
    }

    public DiscoveryNodes addNodes(DiscoveryNodes.Builder nb, String nodePrefix, int nodesAdded) {
        for (int i = 0; i < nodesAdded; i++) {
            nb.add(newNode(nodePrefix + i, singletonMap("_node_id", nodePrefix + i)));
        }
        return nb.build();
    }

    public void resetCluster() {
        clusterState = initialClusterState;
    }

    public void updateInitialCluster() {
        initialClusterState = clusterState;
    }

    public void createEmptyZoneAwareCluster(Map<String, Integer> nodesPerZone) {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        int nodeId = 0;
        for (String zone : nodesPerZone.keySet()) {
            for (int i = 0; i < nodesPerZone.get(zone); i++) {
                nb.add(newNode("node_" + nodeId, singletonMap("zone", zone)));
                nodeId++;
            }
        }
        initialClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(nb.build()).build();
        clusterState = initialClusterState;
    }

    public void setupInitialCluster(int nodeCount, int indices, int shards, int replicas) {
        final String DEFAULT_INDEX_PREFIX = "index_";
        final String DEFAULT_NODE_PREFIX = "node_";

        Metadata metadata = buildMetadata(Metadata.builder(), DEFAULT_INDEX_PREFIX, indices, shards, replicas);
        RoutingTable routingTable = buildRoutingTable(RoutingTable.builder(), metadata, DEFAULT_INDEX_PREFIX, indices);
        DiscoveryNodes nodes = addNodes(DiscoveryNodes.builder(), DEFAULT_NODE_PREFIX, nodeCount);
        initialClusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).nodes(nodes).build();

        buildAllocationService();
        initialClusterState = allocateShardsAndBalance(initialClusterState);
        clusterState = initialClusterState;

        indexShardCount = new HashMap<>();
        for (int i = 0; i < indices; i++) {
            indexShardCount.put(DEFAULT_INDEX_PREFIX + i, shards * (replicas + 1));
        }

        assertForIndexShardHotSpots(false, nodeCount); // Initial cluster should be balanced
        logger.info("Initial cluster created...");
    }

    public void buildNodeShardsByIndex() {
        nodeShardsByIndex = new HashMap<>();
        for (RoutingNode rn : clusterState.getRoutingNodes()) {
            String node = rn.nodeId();
            nodeShardsByIndex.put(node, new HashMap<>());
            for (ShardRouting shard : rn) {
                assert shard.currentNodeId().equals(node);
                if (shard.state() == INITIALIZING || shard.state() == STARTED) {
                    nodeShardsByIndex.get(node).merge(shard.getIndexName(), 1, Integer::sum);
                }
            }
        }
    }

    public boolean isIndexHotSpotPresent(int nodes, List<String> nodeList) {
        for (String node : nodeList) {
            for (String index : nodeShardsByIndex.get(node).keySet()) {
                int count = nodeShardsByIndex.get(node).get(index);
                int limit = (int) Math.ceil(indexShardCount.get(index) / (float) nodes);
                if (count > limit) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<String> getNodeList(String... nodesToValidate) {
        if (CollectionUtils.isEmpty(nodesToValidate)) {
            return Arrays.asList(clusterState.getNodes().resolveNodes());
        }
        return Arrays.asList(nodesToValidate);
    }

    public void assertForIndexShardHotSpots(boolean expected, int nodes, String... nodesToValidate) {
        List<String> nodeList = getNodeList(nodesToValidate);
        buildNodeShardsByIndex();
        assertEquals(expected, isIndexHotSpotPresent(nodes, nodeList));
    }

    public int allocateAndCheckIndexShardHotSpots(boolean expected, int nodes, String... nodesToValidate) {
        List<String> nodeList = getNodeList(nodesToValidate);
        boolean hasHotSpot = false;
        buildNodeShardsByIndex();
        List<ShardRouting> initShards;
        int movesToBalance = 0;
        do {
            assert movesToBalance <= MAX_REROUTE_STEPS_ALLOWED : "Could not balance cluster in max allowed moves";
            clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
            clusterState = allocation.reroute(clusterState, "reroute");

            initShards = clusterState.getRoutingNodes().shardsWithState(INITIALIZING);
            for (ShardRouting shard : initShards) {
                String node = shard.currentNodeId();
                String index = shard.getIndexName();
                nodeShardsByIndex.get(node).merge(index, 1, Integer::sum);

                if (!nodeList.contains(node)) {
                    continue;
                }

                int count = nodeShardsByIndex.get(node).get(index);
                int limit = (int) Math.ceil(indexShardCount.get(index) / (float) nodes);
                if (count <= limit) {
                    continue;
                }

                /**
                 * Hot spots can occur due to the order in which shards get allocated to nodes.
                 * A node with fewer shards may not be able to accept current shard due to
                 * SameShardAllocationDecider, causing it to breach allocation constraint on
                 * another node. We need to differentiate between such hot spots v/s actual hot
                 * spots.
                 *
                 * A simple check could be to ensure there is no node with shards less than
                 * allocation limit, that can accept current shard. However, in current
                 * allocation algorithm, when nodes get throttled, shards are added to
                 * ModelNodes without adding them to actual cluster (RoutingNodes). As a result,
                 * the shards per node we see here, are different from the ones observed by
                 * weight function in balancer. RoutingNodes with {@link count} < {@link limit}
                 * may not have had the same count in the corresponding ModelNode seen by weight
                 * function. We hence use the following alternate check --
                 *
                 * Given the way {@link limit} is defined, we should not have hot spots if *all*
                 * nodes are eligible to accept the shard. A hot spot is acceptable, if either
                 * all peer nodes have {@link count} > {@link limit}, or if even one node is
                 * ineligible to accept the shard due to SameShardAllocationDecider, as this
                 * leads to a chain of events that breach IndexShardsPerNode constraint on all
                 * other nodes.
                 */

                // If all peer nodes have count >= limit, hotspot is acceptable
                boolean limitBreachedOnAllNodes = true;
                for (RoutingNode peerNode : clusterState.getRoutingNodes()) {
                    if (peerNode.nodeId().equals(node)) {
                        continue;
                    }
                    int peerCount = nodeShardsByIndex.get(peerNode.nodeId()).getOrDefault(index, 0);
                    if (peerCount < limit) {
                        limitBreachedOnAllNodes = false;
                    }
                }
                if (limitBreachedOnAllNodes) {
                    continue;
                }

                // If any node is ineligible to accept the shard, this hot spot is acceptable
                boolean peerHasSameShard = false;
                for (RoutingNode peerNode : clusterState.getRoutingNodes()) {
                    if (peerNode.nodeId().equals(node)) {
                        continue;
                    }
                    ShardRouting sameIdShardOnPeer = peerNode.getByShardId(shard.shardId());
                    if (sameIdShardOnPeer != null && sameIdShardOnPeer.getIndexName().equals(index)) {
                        peerHasSameShard = true;
                    }
                }
                if (peerHasSameShard) {
                    continue;
                }

                hasHotSpot = true;
            }
            movesToBalance++;
        } while (!initShards.isEmpty());

        logger.info("HotSpot: [{}], Moves to balance: [{}]", hasHotSpot, movesToBalance);
        assertEquals(expected, hasHotSpot);
        assertForIndexShardHotSpots(false, nodes); // Post re-balancing, cluster should always be hot spot free.
        return movesToBalance;
    }

    public ClusterState allocateShardsAndBalance(ClusterState clusterState) {
        int iterations = 0;
        do {
            clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
            clusterState = allocation.reroute(clusterState, "reroute");
            iterations++;
        } while (!clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() && iterations < MAX_REROUTE_STEPS_ALLOWED);
        return clusterState;
    }

    public void addNodesWithoutIndexing(int nodeCount, String node_prefix) {
        DiscoveryNodes nodes = addNodes(DiscoveryNodes.builder(clusterState.nodes()), node_prefix, nodeCount);
        clusterState = ClusterState.builder(clusterState).nodes(nodes).build();
    }

    public void terminateNodes(String... nodesToTerminate) {
        if (CollectionUtils.isEmpty(nodesToTerminate)) {
            return;
        }
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder(clusterState.nodes());
        Arrays.asList(nodesToTerminate).forEach(node -> nb.remove(node));
        clusterState = ClusterState.builder(clusterState).nodes(nb.build()).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "node-terminated");
        clusterState = allocateShardsAndBalance(clusterState);
    }

    public void addNodesWithIndexing(int nodeCount, String node_prefix, int indices, int shards, int replicas) {
        final String NEW_INDEX_PREFIX = "new_index_";
        Metadata md = buildMetadata(Metadata.builder(clusterState.getMetadata()), NEW_INDEX_PREFIX, indices, shards, replicas);
        RoutingTable rb = buildRoutingTable(RoutingTable.builder(clusterState.getRoutingTable()), md, NEW_INDEX_PREFIX, indices);
        DiscoveryNodes nodes = addNodes(DiscoveryNodes.builder(clusterState.nodes()), node_prefix, nodeCount);
        clusterState = ClusterState.builder(clusterState).metadata(md).routingTable(rb).nodes(nodes).build();
        for (int i = 0; i < indices; i++) {
            indexShardCount.put(NEW_INDEX_PREFIX + i, shards * (replicas + 1));
        }
    }

    public void addIndices(String index_prefix, int indices, int shards, int replicas) {
        Metadata md = buildMetadata(Metadata.builder(clusterState.getMetadata()), index_prefix, indices, shards, replicas);
        RoutingTable rb = buildRoutingTable(RoutingTable.builder(clusterState.getRoutingTable()), md, index_prefix, indices);
        clusterState = ClusterState.builder(clusterState).metadata(md).routingTable(rb).build();

        if (indexShardCount == null) {
            indexShardCount = new HashMap<>();
        }

        for (int i = 0; i < indices; i++) {
            indexShardCount.put(index_prefix + i, shards * (replicas + 1));
        }
    }

}
