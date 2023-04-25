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
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class BalanceConfigurationTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(BalanceConfigurationTests.class);
    // TODO maybe we can randomize these numbers somehow
    final int numberOfNodes = 25;
    final int numberOfIndices = 12;
    final int numberOfShards = 2;
    final int numberOfReplicas = 2;

    public void testIndexBalance() {
        /* Tests balance over indices only */
        final float indexBalance = 1.0f;
        final float shardBalance = 0.0f;
        final float balanceThreshold = 1.0f;

        Settings.Builder settings = Settings.builder();
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), shardBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);

        AllocationService strategy = createAllocationService(settings.build(), new TestGatewayAllocator());

        ClusterState clusterState = initCluster(strategy);
        assertIndexBalance(
            clusterState.getRoutingTable(),
            clusterState.getRoutingNodes(),
            numberOfNodes,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = addNode(clusterState, strategy);
        assertIndexBalance(
            clusterState.getRoutingTable(),
            clusterState.getRoutingNodes(),
            numberOfNodes + 1,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = removeNodes(clusterState, strategy);
        assertIndexBalance(
            clusterState.getRoutingTable(),
            clusterState.getRoutingNodes(),
            (numberOfNodes + 1) - (numberOfNodes + 1) / 2,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );
    }

    private Settings.Builder getSettingsBuilderForPrimaryBalance() {
        return getSettingsBuilderForPrimaryBalance(true);
    }

    private Settings.Builder getSettingsBuilderForPrimaryBalance(boolean preferPrimaryBalance) {
        final float indexBalance = 0.55f;
        final float shardBalance = 0.45f;
        final float balanceThreshold = 1.0f;

        Settings.Builder settings = Settings.builder();
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.PREFER_PRIMARY_SHARD_BALANCE.getKey(), preferPrimaryBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), shardBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);
        return settings;
    }

    private IndexMetadata getIndexMetadata(String indexName, int shardCount, int replicaCount) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, shardCount)
                    .put(SETTING_NUMBER_OF_REPLICAS, replicaCount)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();
    }

    /**
     * This test verifies that with only primary shard balance, the primary shard distribution per index is balanced.
     */
    public void testPrimaryBalance() {
        AllocationService strategy = createAllocationService(getSettingsBuilderForPrimaryBalance().build(), new TestGatewayAllocator());

        ClusterState clusterState = initCluster(strategy);
        verifyPerIndexPrimaryBalance(clusterState);

        clusterState = addNode(clusterState, strategy);
        verifyPerIndexPrimaryBalance(clusterState);

        clusterState = removeNodes(clusterState, strategy);
        verifyPerIndexPrimaryBalance(clusterState);
    }

    /**
     * This test verifies primary shard balance is not attained without PREFER_PRIMARY_SHARD_BALANCE setting.
     */
    public void testPrimaryBalanceWithoutPreferPrimaryBalanceSetting() {
        final int numberOfNodes = 5;
        final int numberOfIndices = 5;
        final int numberOfShards = 25;
        final int numberOfReplicas = 1;

        final int numberOfRuns = 5;
        int balanceFailed = 0;

        AllocationService strategy = createAllocationService(
            getSettingsBuilderForPrimaryBalance(false).build(),
            new TestGatewayAllocator()
        );
        for (int i = 0; i < numberOfRuns; i++) {
            ClusterState clusterState = initCluster(strategy, numberOfIndices, numberOfNodes, numberOfShards, numberOfReplicas);
            clusterState = removeOneNode(clusterState, strategy);
            logger.info(ShardAllocations.printShardDistribution(clusterState));
            try {
                verifyPerIndexPrimaryBalance(clusterState);
            } catch (AssertionError e) {
                balanceFailed++;
                logger.info("Expected assertion failure");
            }
        }
        assertTrue(balanceFailed >= 4);
    }

    /**
     * This test verifies primary shard balance is attained with PREFER_PRIMARY_SHARD_BALANCE setting.
     */
    public void testPrimaryBalanceWithPreferPrimaryBalanceSetting() {
        final int numberOfNodes = 5;
        final int numberOfIndices = 5;
        final int numberOfShards = 25;
        final int numberOfReplicas = 1;
        final int numberOfRuns = 5;
        int balanceFailed = 0;

        AllocationService strategy = createAllocationService(getSettingsBuilderForPrimaryBalance().build(), new TestGatewayAllocator());
        for (int i = 0; i < numberOfRuns; i++) {
            ClusterState clusterState = initCluster(strategy, numberOfIndices, numberOfNodes, numberOfShards, numberOfReplicas);
            clusterState = removeOneNode(clusterState, strategy);
            logger.info(ShardAllocations.printShardDistribution(clusterState));
            try {
                verifyPerIndexPrimaryBalance(clusterState);
            } catch (AssertionError e) {
                balanceFailed++;
                logger.info("Unexpected assertion failure");
            }
        }
        assertTrue(balanceFailed <= 1);
    }

    /**
     * This test verifies the allocation logic when nodes breach multiple constraints and ensure node breaching min
     * constraints chosen for allocation.
     *
     * This test mimics a cluster state containing four nodes, where one node breaches two constraints while one breaches
     * only one. In order to have nodes breach constraints, test excludes two nodes (node2, node3) from allocation so
     * that other two nodes (node0, node1) have all shards assignments resulting in constraints breach. Test asserts that
     * the new primary shard assignment lands on the node breaching one constraint(node1), while replica land on the other
     * (node0). Final shard allocation state.
     *
     routing_nodes:
     -----node_id[node2][V]
     -----node_id[node3][V]
     -----node_id[node0][V]
     --------[test][1], node[node0], [P], s[STARTED], a[id=7B4dVsrjSoC6imBHO60mrQ]
     --------[test][0], node[node0], [P], s[STARTED], a[id=0HySaPcyRhiKrH6QLA3evw]
     --------[test][2], node[node0], [R], s[STARTED], a[id=pB3iuLKZSC--2yNS0trbgA]
     -----node_id[node1][V]
     --------[test][2], node[node1], [P], s[STARTED], a[id=QWN_T6xpQiWGSD8GJnX-bQ]
     --------[test][1], node[node1], [R], s[STARTED], a[id=ChWdQiOdSdKrTPxwceIu1w]
     --------[test][0], node[node1], [R], s[STARTED], a[id=5Adc5JteQ8-lY2xfsHUg-Q]
     *
     */
    public void testPrimaryBalanceWithContrainstBreaching() {
        // Mark node2, node 3 excluded (FilterAllocationDecider) so that all allocations land on node 0, node 1
        Settings.Builder settingsBuilder = getSettingsBuilderForPrimaryBalance();
        settingsBuilder.put("cluster.routing.allocation.exclude._id", "node2,node3");

        AllocationService strategy = createAllocationService(settingsBuilder.build(), new TestGatewayAllocator());
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<String> nodesList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final DiscoveryNode node = newNode("node" + i);
            discoBuilder = discoBuilder.add(node);
            nodesList.add(node.getId());
        }
        discoBuilder.localNodeId(newNode("node_0").getId());
        discoBuilder.clusterManagerNodeId(newNode("node_0").getId());

        Metadata.Builder metadata = Metadata.builder();
        metadata.persistentSettings(settingsBuilder.build());
        RoutingTable.Builder routingTable = RoutingTable.builder();
        // build index metadata
        IndexMetadata indexMetadata = getIndexMetadata("test", 3, 1);
        // Build index routing table
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        ShardId shardId_0 = new ShardId(indexMetadata.getIndex(), 0);
        ShardId shardId_1 = new ShardId(indexMetadata.getIndex(), 1);
        ShardId shardId_2 = new ShardId(indexMetadata.getIndex(), 2);
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_0 = new IndexShardRoutingTable.Builder(shardId_0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_1 = new IndexShardRoutingTable.Builder(shardId_1);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_2 = new IndexShardRoutingTable.Builder(shardId_2);
        indexShardRoutingBuilder_0.addShard(TestShardRouting.newShardRouting(shardId_0, nodesList.get(0), true, ShardRoutingState.STARTED));
        indexShardRoutingBuilder_1.addShard(TestShardRouting.newShardRouting(shardId_1, nodesList.get(0), true, ShardRoutingState.STARTED));
        indexShardRoutingBuilder_0.addShard(
            TestShardRouting.newShardRouting(shardId_0, nodesList.get(1), false, ShardRoutingState.STARTED)
        );
        indexShardRoutingBuilder_1.addShard(
            TestShardRouting.newShardRouting(shardId_1, nodesList.get(1), false, ShardRoutingState.STARTED)
        );
        indexShardRoutingBuilder_2.addShard(
            TestShardRouting.newShardRouting(
                shardId_2,
                null,
                null,
                true,
                UNASSIGNED,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                unassignedInfo
            )
        );
        indexShardRoutingBuilder_2.addShard(
            TestShardRouting.newShardRouting(
                shardId_2,
                null,
                null,
                false,
                UNASSIGNED,
                RecoverySource.PeerRecoverySource.INSTANCE,
                unassignedInfo
            )
        );

        IndexShardRoutingTable indexShardRoutingTable_0 = indexShardRoutingBuilder_0.build();
        IndexShardRoutingTable indexShardRoutingTable_1 = indexShardRoutingBuilder_1.build();

        indexRoutingTable.addIndexShard(indexShardRoutingBuilder_0.build());
        indexRoutingTable.addIndexShard(indexShardRoutingBuilder_1.build());
        indexRoutingTable.addIndexShard(indexShardRoutingBuilder_2.build());
        routingTable.add(indexRoutingTable);

        IndexMetadata.Builder indexMetaDataBuilder = IndexMetadata.builder(indexMetadata);
        indexMetaDataBuilder.putInSyncAllocationIds(0, indexShardRoutingTable_0.getAllAllocationIds());
        indexMetaDataBuilder.putInSyncAllocationIds(1, indexShardRoutingTable_1.getAllAllocationIds());
        metadata.put(indexMetaDataBuilder.build(), false);

        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("test"));
        stateBuilder.nodes(discoBuilder);
        stateBuilder.metadata(metadata.generateClusterUuidIfNeeded().build());
        stateBuilder.routingTable(routingTable.build());
        ClusterState clusterState = stateBuilder.build();
        clusterState = strategy.reroute(clusterState, "reroute");
        clusterState = applyAllocationUntilNoChange(clusterState, strategy);

        logger.info(ShardAllocations.printShardDistribution(clusterState));
        ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId_2).primaryShard();
        List<ShardRouting> replicaShards = clusterState.routingTable().shardRoutingTable(shardId_2).replicaShards();

        assertTrue(primaryShard.started());
        assertEquals("node1", primaryShard.currentNodeId());

        assertEquals(1, replicaShards.size());
        assertTrue(replicaShards.get(0).started());
        assertEquals("node0", replicaShards.get(0).currentNodeId());
    }

    /**
     * This test verifies global balance by creating indices iteratively and verify primary shards do not pile up on one
     * @throws Exception generic exception
     */
    public void testGlobalPrimaryBalance() throws Exception {
        AllocationService strategy = createAllocationService(getSettingsBuilderForPrimaryBalance().build(), new TestGatewayAllocator());
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .build();
        clusterState = addNode(clusterState, strategy);
        clusterState = addNode(clusterState, strategy);

        clusterState = addIndex(clusterState, strategy, "test-index1", 1, 1);
        clusterState = addIndex(clusterState, strategy, "test-index2", 1, 1);
        clusterState = addIndex(clusterState, strategy, "test-index3", 1, 1);

        logger.info(ShardAllocations.printShardDistribution(clusterState));
        verifyPrimaryBalance(clusterState);
    }

    /**
     * This test mimics a cluster state which can not be rebalanced due to
     * {@link org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider}
     * allocation decider which prevents shard relocation, leaving cluster unbalanced on primaries.
     *
     * There are two nodes (N1, N2) where all primaries land on N1 while replicas on N2.
     * N1        N2
     * ------  --------
     * P1        R1
     * P2        R2
     *
     * -----node_id[node_0][V]
     * --------[test][1], node[node_0], [P], s[STARTED], a[id=xqfZSToVSQaff2xvuxh_yA]
     * --------[test][0], node[node_0], [P], s[STARTED], a[id=VGjOeBGdSmu3pJR6T7v29A]
     * -----node_id[node_1][V]
     * --------[test][1], node[node_1], [R], s[STARTED], a[id=zZI0R8FBQkWMNndEZt9d8w]
     * --------[test][0], node[node_1], [R], s[STARTED], a[id=8IpwEMQ2QEuj5rQOxBagSA]
     */
    public void testPrimaryBalance_NotSolved_1() {
        AllocationService strategy = createAllocationService(getSettingsBuilderForPrimaryBalance().build(), new TestGatewayAllocator());

        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> nodes = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            final DiscoveryNode node = newNode("node_" + i);
            discoBuilder = discoBuilder.add(node);
            nodes.add(node.getId());
        }
        discoBuilder.localNodeId(newNode("node_0").getId());
        discoBuilder.clusterManagerNodeId(newNode("node_0").getId());
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        List<String> nodesList = new ArrayList<>(nodes);
        // build index metadata
        IndexMetadata indexMetadata = getIndexMetadata("test", 2, 1);
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        ShardId shardId_0 = new ShardId(indexMetadata.getIndex(), 0);
        ShardId shardId_1 = new ShardId(indexMetadata.getIndex(), 1);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_0 = new IndexShardRoutingTable.Builder(shardId_0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_1 = new IndexShardRoutingTable.Builder(shardId_1);
        indexShardRoutingBuilder_0.addShard(TestShardRouting.newShardRouting(shardId_0, nodesList.get(0), true, ShardRoutingState.STARTED));
        indexShardRoutingBuilder_1.addShard(TestShardRouting.newShardRouting(shardId_1, nodesList.get(0), true, ShardRoutingState.STARTED));
        indexShardRoutingBuilder_0.addShard(
            TestShardRouting.newShardRouting(shardId_0, nodesList.get(1), false, ShardRoutingState.STARTED)
        );
        indexShardRoutingBuilder_1.addShard(
            TestShardRouting.newShardRouting(shardId_1, nodesList.get(1), false, ShardRoutingState.STARTED)
        );
        indexRoutingTable.addIndexShard(indexShardRoutingBuilder_0.build());
        indexRoutingTable.addIndexShard(indexShardRoutingBuilder_1.build());
        metadata.put(indexMetadata, false);
        routingTable.add(indexRoutingTable);

        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("test"));
        stateBuilder.nodes(discoBuilder);
        stateBuilder.metadata(metadata.generateClusterUuidIfNeeded().build());
        stateBuilder.routingTable(routingTable.build());
        ClusterState clusterState = stateBuilder.build();

        clusterState = strategy.reroute(clusterState, "reroute");
        boolean balanced = true;
        logger.info(ShardAllocations.printShardDistribution(clusterState));
        try {
            verifyPerIndexPrimaryBalance(clusterState);
        } catch (AssertionError e) {
            balanced = false;
        }
        assertFalse(balanced);
    }

    /**
     * This test mimics cluster state where re-balancing is not possible due to existing limitation of re-balancing
     * logic which applies at index level i.e. balance shards single index across all nodes. This will be solved when
     * primary shard count across indices, constraint is added.
     *
     * Please note, P1, P2 belongs to different index
     *
     * N1        N2
     * ------  --------
     * P1       R1
     * P2       R2
     *
     * -----node_id[node_0][V]
     * --------[test1][0], node[node_0], [P], s[STARTED], a[id=u7qtyy5AR42hgEa-JpeArg]
     * --------[test0][0], node[node_0], [P], s[STARTED], a[id=BQrLSo6sQyGlcLdVvGgqLQ]
     * -----node_id[node_1][V]
     * --------[test1][0], node[node_1], [R], s[STARTED], a[id=TDqbfvAfSFK6lnv3aOU9bA]
     * --------[test0][0], node[node_1], [R], s[STARTED], a[id=E85-jhiEQwuB43u5Wq1mAw]
     *
     */
    public void testPrimaryBalance_NotSolved_2() {
        AllocationService strategy = createAllocationService(getSettingsBuilderForPrimaryBalance().build(), new TestGatewayAllocator());

        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> nodes = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            final DiscoveryNode node = newNode("node_" + i);
            discoBuilder = discoBuilder.add(node);
            nodes.add(node.getId());
        }
        discoBuilder.localNodeId(newNode("node_0").getId());
        discoBuilder.clusterManagerNodeId(newNode("node_0").getId());
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        List<String> nodesList = new ArrayList<>(nodes);
        // build index metadata
        IndexMetadata indexMetadata_0 = getIndexMetadata("test0", 1, 1);
        IndexMetadata indexMetadata_1 = getIndexMetadata("test1", 1, 1);
        IndexRoutingTable.Builder indexRoutingTable_0 = IndexRoutingTable.builder(indexMetadata_0.getIndex());
        IndexRoutingTable.Builder indexRoutingTable_1 = IndexRoutingTable.builder(indexMetadata_1.getIndex());
        ShardId shardId_0 = new ShardId(indexMetadata_0.getIndex(), 0);
        ShardId shardId_1 = new ShardId(indexMetadata_1.getIndex(), 0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_0 = new IndexShardRoutingTable.Builder(shardId_0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder_1 = new IndexShardRoutingTable.Builder(shardId_1);
        indexShardRoutingBuilder_0.addShard(TestShardRouting.newShardRouting(shardId_0, nodesList.get(0), true, ShardRoutingState.STARTED));
        indexShardRoutingBuilder_1.addShard(TestShardRouting.newShardRouting(shardId_1, nodesList.get(0), true, ShardRoutingState.STARTED));
        indexShardRoutingBuilder_0.addShard(
            TestShardRouting.newShardRouting(shardId_0, nodesList.get(1), false, ShardRoutingState.STARTED)
        );
        indexShardRoutingBuilder_1.addShard(
            TestShardRouting.newShardRouting(shardId_1, nodesList.get(1), false, ShardRoutingState.STARTED)
        );
        indexRoutingTable_0.addIndexShard(indexShardRoutingBuilder_0.build());
        indexRoutingTable_1.addIndexShard(indexShardRoutingBuilder_1.build());
        metadata.put(indexMetadata_0, false);
        metadata.put(indexMetadata_1, false);
        routingTable.add(indexRoutingTable_0);
        routingTable.add(indexRoutingTable_1);
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("test"));
        stateBuilder.nodes(discoBuilder);
        stateBuilder.metadata(metadata.generateClusterUuidIfNeeded().build());
        stateBuilder.routingTable(routingTable.build());
        ClusterState clusterState = stateBuilder.build();

        clusterState = strategy.reroute(clusterState, "reroute");
        logger.info(ShardAllocations.printShardDistribution(clusterState));
        // The cluster is balanced when considering indices individually not balanced when considering global state
        verifyPerIndexPrimaryBalance(clusterState);
    }

    private void verifyPerIndexPrimaryBalance(ClusterState currentState) {
        RoutingNodes nodes = currentState.getRoutingNodes();
        for (final Map.Entry<String, IndexRoutingTable> index : currentState.getRoutingTable().indicesRouting().entrySet()) {
            final int totalPrimaryShards = index.getValue().primaryShardsActive();
            final int avgPrimaryShardsPerNode = (int) Math.ceil(totalPrimaryShards * 1f / currentState.getRoutingNodes().size());

            for (RoutingNode node : nodes) {
                final int primaryCount = node.shardsWithState(index.getKey(), STARTED)
                    .stream()
                    .filter(ShardRouting::primary)
                    .collect(Collectors.toList())
                    .size();
                assertTrue(primaryCount <= avgPrimaryShardsPerNode);
            }
        }
    }

    private void verifyPrimaryBalance(ClusterState clusterState) throws Exception {
        assertBusy(() -> {
            RoutingNodes nodes = clusterState.getRoutingNodes();
            int totalPrimaryShards = 0;
            for (final IndexRoutingTable index : clusterState.getRoutingTable().indicesRouting().values()) {
                totalPrimaryShards += index.primaryShardsActive();
            }
            final int avgPrimaryShardsPerNode = (int) Math.ceil(totalPrimaryShards * 1f / clusterState.getRoutingNodes().size());
            for (RoutingNode node : nodes) {
                final int primaryCount = node.shardsWithState(STARTED)
                    .stream()
                    .filter(ShardRouting::primary)
                    .collect(Collectors.toList())
                    .size();
                assertTrue(primaryCount <= avgPrimaryShardsPerNode);
            }
        }, 60, TimeUnit.SECONDS);
    }

    public void testShardBalance() {
        /* Tests balance over replicas only */
        final float indexBalance = 0.0f;
        final float shardBalance = 1.0f;
        final float balanceThreshold = 1.0f;

        Settings.Builder settings = Settings.builder();
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), shardBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);

        AllocationService strategy = createAllocationService(settings.build(), new TestGatewayAllocator());

        ClusterState clusterState = initCluster(strategy);
        assertShardBalance(
            clusterState.getRoutingNodes(),
            numberOfNodes,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = addNode(clusterState, strategy);
        assertShardBalance(
            clusterState.getRoutingNodes(),
            numberOfNodes + 1,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = removeNodes(clusterState, strategy);
        assertShardBalance(
            clusterState.getRoutingNodes(),
            numberOfNodes + 1 - (numberOfNodes + 1) / 2,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );
    }

    private ClusterState initCluster(AllocationService strategy) {
        return initCluster(strategy, numberOfIndices, numberOfNodes, numberOfShards, numberOfReplicas);
    }

    private ClusterState addIndex(
        ClusterState clusterState,
        AllocationService strategy,
        String indexName,
        int numberOfShards,
        int numberOfReplicas
    ) {
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.getMetadata());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());

        IndexMetadata.Builder index = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas);

        metadataBuilder = metadataBuilder.put(index);
        routingTableBuilder.addAsNew(index.build());

        clusterState = ClusterState.builder(clusterState)
            .metadata(metadataBuilder.build())
            .routingTable(routingTableBuilder.build())
            .build();
        clusterState = strategy.reroute(clusterState, "index-created");
        return applyAllocationUntilNoChange(clusterState, strategy);
    }

    private ClusterState initCluster(
        AllocationService strategy,
        int numberOfIndices,
        int numberOfNodes,
        int numberOfShards,
        int numberOfReplicas
    ) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetadata.Builder index = IndexMetadata.builder("test" + i)
                .settings(settings(Version.CURRENT).build())
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder = metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();
        for (final IndexMetadata cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor);
        }

        RoutingTable initialRoutingTable = routingTableBuilder.build();

        logger.info("start " + numberOfNodes + " nodes");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        return applyAllocationUntilNoChange(clusterState, strategy);
    }

    private ClusterState addNode(ClusterState clusterState, AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node" + clusterState.getRoutingNodes().size())))
            .build();

        RoutingTable routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        // move initializing to started
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private ClusterState removeOneNode(ClusterState clusterState, AllocationService strategy) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        nodes.remove("node0");
        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        clusterState = strategy.disassociateDeadNodes(clusterState, randomBoolean(), "removed nodes");
        return applyAllocationUntilNoChange(clusterState, strategy);
    }

    private ClusterState removeNodes(ClusterState clusterState, AllocationService strategy) {
        int numberOfNodes = clusterState.getRoutingNodes().size();
        logger.info("Removing half the nodes (" + (numberOfNodes + 1) / 2 + ")");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());

        boolean removed = false;
        for (int i = (numberOfNodes + 1) / 2; i <= numberOfNodes; i++) {
            nodes.remove("node" + i);
            removed = true;
        }

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        if (removed) {
            clusterState = strategy.disassociateDeadNodes(clusterState, randomBoolean(), "removed nodes");
        }
        return applyAllocationUntilNoChange(clusterState, strategy);
    }

    private ClusterState applyAllocationUntilNoChange(ClusterState clusterState, AllocationService strategy) {
        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("rebalancing");
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("complete rebalancing");
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private void assertShardBalance(
        RoutingNodes nodes,
        int numberOfNodes,
        int numberOfIndices,
        int numberOfReplicas,
        int numberOfShards,
        float threshold
    ) {
        final int unassigned = nodes.unassigned().size();

        if (unassigned > 0) {
            // Ensure that if there any unassigned shards, all of their replicas are unassigned as well
            // (i.e. unassigned count is always [replicas] + 1 for each shard unassigned shardId)
            nodes.shardsWithState(UNASSIGNED)
                .stream()
                .collect(Collectors.toMap(ShardRouting::shardId, s -> 1, (a, b) -> a + b))
                .values()
                .forEach(count -> assertEquals(numberOfReplicas + 1, count.longValue()));
        }
        assertEquals(numberOfNodes, nodes.size());

        final int numShards = numberOfIndices * numberOfShards * (numberOfReplicas + 1) - unassigned;
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - threshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + threshold)));

        for (RoutingNode node : nodes) {
            assertThat(node.shardsWithState(STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
            assertThat(node.shardsWithState(STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
        }
    }

    private void assertIndexBalance(
        RoutingTable routingTable,
        RoutingNodes nodes,
        int numberOfNodes,
        int numberOfIndices,
        int numberOfReplicas,
        int numberOfShards,
        float threshold
    ) {

        final int numShards = numberOfShards * (numberOfReplicas + 1);
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - threshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + threshold)));

        for (final String index : routingTable.indicesRouting().keySet()) {
            for (RoutingNode node : nodes) {
                assertThat(node.shardsWithState(index, STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(node.shardsWithState(index, STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }

    public void testPersistedSettings() {
        Settings.Builder settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        ClusterSettings service = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.2f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.3f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(2.0f));

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        service.applySettings(settings.build());
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.2f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.3f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(2.0f));

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.1);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 3.0);
        service.applySettings(settings.build());
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.5f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.1f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(3.0f));
    }

    public void testNoRebalanceOnPrimaryOverload() {
        Settings.Builder settings = Settings.builder();
        AllocationService strategy = new AllocationService(
            randomAllocationDeciders(
                settings.build(),
                new ClusterSettings(Settings.Builder.EMPTY_SETTINGS, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                random()
            ),
            new TestGatewayAllocator(),
            new ShardsAllocator() {
                /*
                 *  // this allocator tries to rebuild this scenario where a rebalance is
                 *  // triggered solely by the primary overload on node [1] where a shard
                 *  // is rebalanced to node 0
                    routing_nodes:
                    -----node_id[0][V]
                    --------[test][0], node[0], [R], s[STARTED]
                    --------[test][4], node[0], [R], s[STARTED]
                    -----node_id[1][V]
                    --------[test][0], node[1], [P], s[STARTED]
                    --------[test][1], node[1], [P], s[STARTED]
                    --------[test][3], node[1], [R], s[STARTED]
                    -----node_id[2][V]
                    --------[test][1], node[2], [R], s[STARTED]
                    --------[test][2], node[2], [R], s[STARTED]
                    --------[test][4], node[2], [P], s[STARTED]
                    -----node_id[3][V]
                    --------[test][2], node[3], [P], s[STARTED]
                    --------[test][3], node[3], [P], s[STARTED]
                    ---- unassigned
                */
                public void allocate(RoutingAllocation allocation) {
                    RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
                    ShardRouting[] drain = unassigned.drain();
                    ArrayUtil.timSort(drain, (a, b) -> { return a.primary() ? -1 : 1; }); // we have to allocate primaries first
                    for (ShardRouting sr : drain) {
                        switch (sr.id()) {
                            case 0:
                                if (sr.primary()) {
                                    allocation.routingNodes().initializeShard(sr, "node1", null, -1, allocation.changes());
                                } else {
                                    allocation.routingNodes().initializeShard(sr, "node0", null, -1, allocation.changes());
                                }
                                break;
                            case 1:
                                if (sr.primary()) {
                                    allocation.routingNodes().initializeShard(sr, "node1", null, -1, allocation.changes());
                                } else {
                                    allocation.routingNodes().initializeShard(sr, "node2", null, -1, allocation.changes());
                                }
                                break;
                            case 2:
                                if (sr.primary()) {
                                    allocation.routingNodes().initializeShard(sr, "node3", null, -1, allocation.changes());
                                } else {
                                    allocation.routingNodes().initializeShard(sr, "node2", null, -1, allocation.changes());
                                }
                                break;
                            case 3:
                                if (sr.primary()) {
                                    allocation.routingNodes().initializeShard(sr, "node3", null, -1, allocation.changes());
                                } else {
                                    allocation.routingNodes().initializeShard(sr, "node1", null, -1, allocation.changes());
                                }
                                break;
                            case 4:
                                if (sr.primary()) {
                                    allocation.routingNodes().initializeShard(sr, "node2", null, -1, allocation.changes());
                                } else {
                                    allocation.routingNodes().initializeShard(sr, "node0", null, -1, allocation.changes());
                                }
                                break;
                        }

                    }
                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    throw new UnsupportedOperationException("explain not supported");
                }
            },
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexMetadata.Builder indexMeta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(5)
            .numberOfReplicas(1);
        metadataBuilder = metadataBuilder.put(indexMeta);
        Metadata metadata = metadataBuilder.build();
        for (final IndexMetadata cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor);
        }
        RoutingTable routingTable = routingTableBuilder.build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < 4; i++) {
            DiscoveryNode node = newNode("node" + i);
            nodes.add(node);
        }

        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.INITIALIZING));
            }
        }
        strategy = createAllocationService(settings.build(), new TestGatewayAllocator());

        logger.info("use the new allocator and check if it moves shards");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();
        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("start the replica shards");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }
    }
}
