/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationAllocationIT extends SegmentReplicationBaseIT {

    private void createIndex(String idxName, int shardCount, int replicaCount, boolean isSegRep) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount);
        if (isSegRep) {
            builder = builder.put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        } else {
            builder = builder.put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT);
        }
        prepareCreate(idxName, builder).get();
    }

    public void enablePreferPrimaryBalance() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder().put(BalancedShardsAllocator.PREFER_PER_INDEX_PRIMARY_SHARD_BALANCE.getKey(), "true")
                )
        );
    }

    /**
     * This test verifies the happy path where primary shard allocation is balanced when multiple indices are created.
     *
     * This test in general passes without primary shard balance as well due to nature of allocation algorithm which
     * assigns all primary shards first followed by replica copies.
     */
    public void testBalancedPrimaryAllocation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxReplicaCount = 2;
        final int maxShardCount = 5;
        final int nodeCount = randomIntBetween(maxReplicaCount + 1, 10);
        final int numberOfIndices = randomIntBetween(5, 10);

        final List<String> nodeNames = new ArrayList<>();
        logger.info("--> Creating {} nodes", nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }
        enablePreferPrimaryBalance();
        int shardCount, replicaCount, totalShardCount = 0, totalReplicaCount = 0;
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        for (int i = 0; i < numberOfIndices; i++) {
            shardCount = randomIntBetween(1, maxShardCount);
            totalShardCount += shardCount;
            replicaCount = randomIntBetween(0, maxReplicaCount);
            totalReplicaCount += replicaCount;
            createIndex("test" + i, shardCount, replicaCount, i % 2 == 0);
            logger.info("--> Creating index {} with shard count {} and replica count {}", "test" + i, shardCount, replicaCount);
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
            state = client().admin().cluster().prepareState().execute().actionGet().getState();
            shardAllocations.printShardDistribution(state);
        }
        verifyPerIndexPrimaryBalance();
    }

    /**
     * This test verifies balanced primary shard allocation for a single index with large shard count in event of node
     * going down and a new node joining the cluster. The results in shard distribution skewness and re-balancing logic
     * ensures the primary shard distribution is balanced.
     *
     */
    public void testSingleIndexShardAllocation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxReplicaCount = 1;
        final int maxShardCount = 50;
        final int nodeCount = 5;

        final List<String> nodeNames = new ArrayList<>();
        logger.info("--> Creating {} nodes", nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }
        enablePreferPrimaryBalance();

        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        createIndex("test", maxShardCount, maxReplicaCount, true);
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();

        // Remove a node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(0)));
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();

        // Add a new node
        internalCluster().startDataOnlyNode();
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();
    }

    /**
     * Similar to testSingleIndexShardAllocation test but creates multiple indices, multiple node adding in and getting
     * removed. The test asserts post each such event that primary shard distribution is balanced across single index.
     */
    public void testAllocationWithDisruption() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxReplicaCount = 2;
        final int maxShardCount = 5;
        final int nodeCount = randomIntBetween(maxReplicaCount + 1, 10);
        final int numberOfIndices = randomIntBetween(1, 10);

        logger.info("--> Creating {} nodes", nodeCount);
        final List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }
        enablePreferPrimaryBalance();

        int shardCount, replicaCount, totalShardCount = 0, totalReplicaCount = 0;
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        for (int i = 0; i < numberOfIndices; i++) {
            shardCount = randomIntBetween(1, maxShardCount);
            totalShardCount += shardCount;
            replicaCount = randomIntBetween(1, maxReplicaCount);
            totalReplicaCount += replicaCount;
            logger.info("--> Creating index test{} with primary {} and replica {}", i, shardCount, replicaCount);
            createIndex("test" + i, shardCount, replicaCount, i % 2 == 0);
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
            if (logger.isTraceEnabled()) {
                state = client().admin().cluster().prepareState().execute().actionGet().getState();
                shardAllocations.printShardDistribution(state);
            }
        }
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();

        final int additionalNodeCount = randomIntBetween(1, 5);
        logger.info("--> Adding {} nodes", additionalNodeCount);

        internalCluster().startNodes(additionalNodeCount);
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();

        logger.info("--> Stop one third nodes");
        for (int i = 0; i < nodeCount; i += 3) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(i)));
            // give replica a chance to promote as primary before terminating node containing the replica
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        }
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();
    }

    public void testShardAllocationWithAutoExpandReplicas() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxShardCount = 5;
        final int nodeCount = randomIntBetween(1, 10);

        final List<String> nodeNames = new ArrayList<>();
        logger.info("--> Creating {} nodes", nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }
        enablePreferPrimaryBalance();
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        int shardCount = randomIntBetween(1, maxShardCount);
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        prepareCreate("test0", builder).get();
        logger.info("--> Creating index {} with shard count {}", "test0", shardCount);
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(0)));
        // give replica a chance to promote as primary before terminating node containing the replica
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        shardAllocations.printShardDistribution(state);
        verifyPerIndexPrimaryBalance();
    }

    /**
     * Utility method which ensures cluster has balanced primary shard distribution across a single index.
     * @throws Exception
     */
    private void verifyPerIndexPrimaryBalance() throws Exception {
        assertBusy(() -> {
            final ClusterState currentState = client().admin().cluster().prepareState().execute().actionGet().getState();
            RoutingNodes nodes = currentState.getRoutingNodes();
            for (ObjectObjectCursor<String, IndexRoutingTable> index : currentState.getRoutingTable().indicesRouting()) {
                final int totalPrimaryShards = index.value.primaryShardsActive();
                final int avgPrimaryShardsPerNode = (int) Math.ceil(totalPrimaryShards * 1f / currentState.getRoutingNodes().size());
                for (RoutingNode node : nodes) {
                    final int primaryCount = node.shardsWithState(index.key, STARTED)
                        .stream()
                        .filter(ShardRouting::primary)
                        .collect(Collectors.toList())
                        .size();
                    assertTrue(primaryCount <= avgPrimaryShardsPerNode);
                }
            }
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * This class is created for debugging purpose to show shard allocation across nodes. It keeps cluster state which
     * is used to build the node's shard allocation
     */
    private class ShardAllocations {
        ClusterState state;

        public static final String separator = "===================================================";
        public static final String ONE_LINE_RETURN = "\n";
        public static final String TWO_LINE_RETURN = "\n\n";

        /**
         Store shard primary/replica shard count against a node for segrep indices.
         String: NodeId
         int[]: tuple storing primary shard count in 0th index and replica's in 1
         */
        TreeMap<String, int[]> nodeToSegRepCountMap = new TreeMap<>();
        /**
         Store shard primary/replica shard count against a node for docrep indices.
         String: NodeId
         int[]: tuple storing primary shard count in 0th index and replica's in 1
         */
        TreeMap<String, int[]> nodeToDocRepCountMap = new TreeMap<>();

        /**
         * Helper map containing NodeName to NodeId
         */
        TreeMap<String, String> nameToNodeId = new TreeMap<>();

        Map<String, TreeMap<String, int[]>> nodeToIndexCountMap = new HashMap<>();

        /*
        Unassigned array containing primary at 0, replica at 1
         */
        int[] unassigned = new int[2];

        int[] totalShards = new int[2];

        public final String printShardAllocationWithHeader(int[] docrep, int[] segrep) {
            StringBuffer sb = new StringBuffer();
            Formatter formatter = new Formatter(sb, Locale.getDefault());
            formatter.format("%-40s %-20s %-20s %-20s\n", "P", docrep[0] + segrep[0], docrep[0], segrep[0]);
            formatter.format("%-40s %-20s %-20s %-20s\n", "R", docrep[1] + segrep[1], docrep[1], segrep[1]);
            return sb.toString();
        }

        public void reset() {
            nodeToSegRepCountMap.clear();
            nodeToDocRepCountMap.clear();
            nameToNodeId.clear();
            nodeToIndexCountMap.clear();
            totalShards[0] = totalShards[1] = 0;
            unassigned[0] = unassigned[1] = 0;
        }

        public void setState(ClusterState state) {
            this.reset();
            this.state = state;
            buildMap();
        }

        private void buildMap() {
            for (RoutingNode node : state.getRoutingNodes()) {
                nameToNodeId.putIfAbsent(node.node().getName(), node.nodeId());
                nodeToSegRepCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
                nodeToDocRepCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
                nodeToIndexCountMap.put(node.nodeId(), new TreeMap<>());
            }
            for (ShardRouting shardRouting : state.routingTable().allShards()) {
                // Fetch shard to update. Initialize local array
                if (isIndexSegRep(shardRouting.getIndexName())) {
                    updateMap(nodeToSegRepCountMap, shardRouting);
                } else {
                    updateMap(nodeToDocRepCountMap, shardRouting);
                }
            }
        }

        void updateMap(TreeMap<String, int[]> mapToUpdate, ShardRouting shardRouting) {
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
            if (shardRouting.assignedToNode()) {
                mapToUpdate.put(shardRouting.currentNodeId(), shard);
                nodeToIndexCountMap.get(shardRouting.currentNodeId()).putIfAbsent(shardRouting.getIndexName(), new int[2]);
                int[] currentValue = nodeToIndexCountMap.get(shardRouting.currentNodeId()).get(shardRouting.getIndexName());
                int indexToUpdate = shardRouting.primary() ? 0 : 1;
                currentValue[indexToUpdate]++;
            }
        }

        boolean isIndexSegRep(String indexName) {
            return state.metadata()
                .index(indexName)
                .getSettings()
                .get(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey())
                .equals(ReplicationType.SEGMENT.toString());
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(TWO_LINE_RETURN + separator + ONE_LINE_RETURN);
            Formatter formatter = new Formatter(sb, Locale.getDefault());
            for (Map.Entry<String, String> entry : nameToNodeId.entrySet()) {
                String nodeId = nameToNodeId.get(entry.getKey());
                formatter.format(
                    "%-40s %-20s %-20s %-20s\n",
                    entry.getKey().toUpperCase(Locale.getDefault()) + "(" + nodeId + ")",
                    "TOTAL",
                    "DOCREP",
                    "SEGREP"
                );
                sb.append(printShardAllocationWithHeader(nodeToDocRepCountMap.get(nodeId), nodeToSegRepCountMap.get(nodeId)));
                for (Map.Entry<String, int[]> indexCountMap : nodeToIndexCountMap.get(nodeId).entrySet()) {
                    formatter.format("[%s %s,%s], ", indexCountMap.getKey(), indexCountMap.getValue()[0], indexCountMap.getValue()[1]);
                }
                sb.append(ONE_LINE_RETURN);
            }
            sb.append(TWO_LINE_RETURN);
            formatter.format("%-20s (P)%-5s (R)%-5s\n\n", "Unassigned ", unassigned[0], unassigned[1]);
            formatter.format("%-20s (P)%-5s (R)%-5s\n\n", "Total Shards", totalShards[0], totalShards[1]);
            return sb.toString();
        }

        public void printShardDistribution(ClusterState state) {
            this.setState(state);
            logger.info("--> Shard distribution {}", this);
        }
    }

}
