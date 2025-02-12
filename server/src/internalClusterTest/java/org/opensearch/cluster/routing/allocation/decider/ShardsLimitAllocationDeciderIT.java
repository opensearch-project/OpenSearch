/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class ShardsLimitAllocationDeciderIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    public void testClusterWideShardsLimit() {
        // Set the cluster-wide shard limit to 2
        updateClusterSetting(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 4);

        // Create the first two indices with 3 shards and 1 replica each
        createIndex("test1", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        createIndex("test2", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1).build());

        // Create the third index with 2 shards and 1 replica
        createIndex("test3", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 1).build());

        // Wait for the shard limit to be applied
        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();

                // Check total number of shards
                assertEquals(16, state.getRoutingTable().allShards().size());

                // Check number of unassigned shards
                int unassignedShards = state.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size();
                assertEquals(4, unassignedShards);

                // Check shard distribution across nodes
                for (RoutingNode routingNode : state.getRoutingNodes()) {
                    assertTrue("Node exceeds shard limit", routingNode.numberOfOwningShards() <= 4);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Additional assertions to verify shard distribution
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        int totalAssignedShards = 0;
        for (RoutingNode routingNode : state.getRoutingNodes()) {
            totalAssignedShards += routingNode.numberOfOwningShards();
        }
        assertEquals("Total assigned shards should be 12", 12, totalAssignedShards);

    }

    public void testIndexSpecificShardLimit() {
        // Set the index-specific shard limit to 2 for the first index only
        Settings indexSettingsWithLimit = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .build();

        Settings indexSettingsWithoutLimit = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 4).put(SETTING_NUMBER_OF_REPLICAS, 1).build();

        // Create the first index with 4 shards, 1 replica, and the index-specific limit
        createIndex("test1", indexSettingsWithLimit);

        // Create the second index with 4 shards and 1 replica, without the index-specific limit
        createIndex("test2", indexSettingsWithoutLimit);

        // Create the third index with 3 shards and 1 replica, without the index-specific limit
        createIndex("test3", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1).build());

        try {
            // Wait for the shard limit to be applied
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();

                // Check total number of shards
                assertEquals(22, state.getRoutingTable().allShards().size());

                // Check total number of assigned and unassigned shards
                int totalAssignedShards = 0;
                int totalUnassignedShards = 0;
                Map<String, Integer> unassignedShardsByIndex = new HashMap<>();

                for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                    String index = indexRoutingTable.getIndex().getName();
                    int indexUnassignedShards = 0;

                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.unassigned()) {
                                totalUnassignedShards++;
                                indexUnassignedShards++;
                            } else {
                                totalAssignedShards++;
                            }
                        }
                    }

                    unassignedShardsByIndex.put(index, indexUnassignedShards);
                }

                assertEquals("Total assigned shards should be 20", 20, totalAssignedShards);
                assertEquals("Total unassigned shards should be 2", 2, totalUnassignedShards);

                // Check unassigned shards for each index
                assertEquals("test1 should have 2 unassigned shards", 2, unassignedShardsByIndex.get("test1").intValue());
                assertEquals("test2 should have 0 unassigned shards", 0, unassignedShardsByIndex.get("test2").intValue());
                assertEquals("test3 should have 0 unassigned shards", 0, unassignedShardsByIndex.get("test3").intValue());
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testCombinedClusterAndIndexSpecificShardLimits() {
        // Set the cluster-wide shard limit to 6
        updateClusterSetting(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 6);

        // Create the first index with 3 shards, 1 replica, and index-specific limit of 1
        Settings indexSettingsWithLimit = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 3)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();
        createIndex("test1", indexSettingsWithLimit);

        // Create the second index with 4 shards and 1 replica
        createIndex("test2", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 4).put(SETTING_NUMBER_OF_REPLICAS, 1).build());

        // Create the third index with 3 shards and 1 replica
        createIndex("test3", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1).build());

        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();

                // Check total number of shards
                assertEquals("Total shards should be 20", 20, state.getRoutingTable().allShards().size());

                int totalAssignedShards = 0;
                int totalUnassignedShards = 0;
                Map<String, Integer> unassignedShardsByIndex = new HashMap<>();
                Map<String, Integer> nodeShardCounts = new HashMap<>();
                Map<String, Set<String>> indexShardsPerNode = new HashMap<>();

                for (RoutingNode routingNode : state.getRoutingNodes()) {
                    String nodeName = routingNode.node().getName();
                    nodeShardCounts.put(nodeName, routingNode.numberOfOwningShards());
                    indexShardsPerNode.put(nodeName, new HashSet<>());

                    for (ShardRouting shardRouting : routingNode) {
                        indexShardsPerNode.get(nodeName).add(shardRouting.getIndexName());
                    }
                }

                for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                    String index = indexRoutingTable.getIndex().getName();
                    int indexUnassignedShards = 0;

                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.unassigned()) {
                                totalUnassignedShards++;
                                indexUnassignedShards++;
                            } else {
                                totalAssignedShards++;
                            }
                        }
                    }

                    unassignedShardsByIndex.put(index, indexUnassignedShards);
                }

                assertEquals("Total assigned shards should be 17", 17, totalAssignedShards);
                assertEquals("Total unassigned shards should be 3", 3, totalUnassignedShards);
                assertEquals("test1 should have 3 unassigned shards", 3, unassignedShardsByIndex.get("test1").intValue());
                assertEquals("test2 should have 0 unassigned shards", 0, unassignedShardsByIndex.getOrDefault("test2", 0).intValue());
                assertEquals("test3 should have 0 unassigned shards", 0, unassignedShardsByIndex.getOrDefault("test3", 0).intValue());

                // Check shard distribution across nodes
                List<Integer> shardCounts = new ArrayList<>(nodeShardCounts.values());
                Collections.sort(shardCounts, Collections.reverseOrder());
                assertEquals("Two nodes should have 6 shards", 6, shardCounts.get(0).intValue());
                assertEquals("Two nodes should have 6 shards", 6, shardCounts.get(1).intValue());
                assertEquals("One node should have 5 shards", 5, shardCounts.get(2).intValue());

                // Check that all nodes have only one shard of the first index
                for (Set<String> indexesOnNode : indexShardsPerNode.values()) {
                    assertTrue("Each node should have a shard from test1", indexesOnNode.contains("test1"));
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testIndexSpecificPrimaryShardLimit() {
        // Set the index-specific primary shard limit to 1 for the first and second index
        Settings indexSettingsWithLimitAndSegmentReplication = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        Settings indexSettingsWithLimit = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        Settings indexSettingsWithoutLimit = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 4).put(SETTING_NUMBER_OF_REPLICAS, 1).build();

        // Create the first index with 4 shards, 1 replica, index-specific primary shard limit, and segment replication
        createIndex("test1", indexSettingsWithLimitAndSegmentReplication);

        // Create the second index with 4 shards, 1 replica, and the index-specific primary shard limit
        createIndex("test2", indexSettingsWithLimit);

        // Create the third index with 4 shards and 1 replica, without the index-specific limit
        createIndex("test3", indexSettingsWithoutLimit);

        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();

                // Check total number of shards
                assertEquals("Total shards should be 24", 24, state.getRoutingTable().allShards().size());

                int totalAssignedShards = 0;
                int totalUnassignedShards = 0;
                Map<String, Integer> unassignedShardsByIndex = new HashMap<>();
                Map<String, Integer> unassignedPrimaryShardsByIndex = new HashMap<>();

                for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                    String index = indexRoutingTable.getIndex().getName();
                    int indexUnassignedShards = 0;
                    int indexUnassignedPrimaryShards = 0;

                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.unassigned()) {
                                totalUnassignedShards++;
                                indexUnassignedShards++;
                                if (shardRouting.primary()) {
                                    indexUnassignedPrimaryShards++;
                                }
                            } else {
                                totalAssignedShards++;
                            }
                        }
                    }

                    unassignedShardsByIndex.put(index, indexUnassignedShards);
                    unassignedPrimaryShardsByIndex.put(index, indexUnassignedPrimaryShards);
                }

                assertEquals("Total assigned shards should be 22", 22, totalAssignedShards);
                assertEquals("Total unassigned shards should be 2", 2, totalUnassignedShards);
                assertEquals("test1 should have 2 unassigned shards", 2, unassignedShardsByIndex.get("test1").intValue());
                assertEquals("test1 should have 1 unassigned primary shard", 1, unassignedPrimaryShardsByIndex.get("test1").intValue());
                assertEquals("test2 should have 0 unassigned shards", 0, unassignedShardsByIndex.getOrDefault("test2", 0).intValue());
                assertEquals("test3 should have 0 unassigned shards", 0, unassignedShardsByIndex.getOrDefault("test3", 0).intValue());

                // Check primary shard distribution for test1 across nodes
                for (RoutingNode routingNode : state.getRoutingNodes()) {
                    int test1PrimaryShardCount = 0;
                    for (ShardRouting shardRouting : routingNode) {
                        if (shardRouting.index().getName().equals("test1") && shardRouting.primary()) {
                            test1PrimaryShardCount++;
                        }
                    }
                    assertTrue("Each node should have at most 1 primary shard for test1", test1PrimaryShardCount <= 1);
                }

                // Verify that test1 uses segment replication
                IndexMetadata test1Metadata = state.metadata().index("test1");
                assertEquals(ReplicationType.SEGMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(test1Metadata.getSettings()));
            });
        } catch (Exception e) {
            logger.error("Failed to assert shard allocation", e);
            fail("Failed to assert shard allocation: " + e.getMessage());
        }
    }

    public void testClusterSpecificPrimaryShardLimit() {
        // Set cluster-level settings
        updateClusterSetting(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 3);
        updateClusterSetting(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2);

        // Settings for test1: 3 primaries, 1 replica
        Settings test1Settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 3)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        // Settings for test2: 2 primaries, 1 replica
        Settings test2Settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        // Settings for test3: 2 primaries, no replicas
        Settings test3Settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        // Create the indices
        createIndex("test1", test1Settings);
        createIndex("test2", test2Settings);
        createIndex("test3", test3Settings);

        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();

                // Track shard statistics
                int totalAssignedShards = 0;
                int totalUnassignedShards = 0;

                // Check shard allocation for each index
                for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.unassigned()) {
                                totalUnassignedShards++;
                            } else {
                                totalAssignedShards++;
                            }
                        }
                    }
                }

                // Assert shard allocation
                assertEquals("Total assigned shards should be 6", 6, totalAssignedShards);
                assertEquals("Total unassigned shards should be 1", 1, totalUnassignedShards);

                // Verify node-level shard distribution
                for (RoutingNode routingNode : state.getRoutingNodes()) {
                    int primaryCount = 0;
                    int totalCount = 0;

                    for (ShardRouting shardRouting : routingNode) {
                        if (shardRouting.primary()) {
                            primaryCount++;
                        }
                        totalCount++;
                    }

                    assertTrue("Each node should have at most 2 primary shards", primaryCount <= 2);
                    assertTrue("Each node should have at most 3 total shards", totalCount <= 3);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testMixedShardsLimitScenario() {
        // Set cluster-wide shard limit to 5
        updateClusterSetting(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 5);

        // First index settings
        Settings firstIndexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        // Second index settings
        Settings secondIndexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .build();

        // Third index settings
        Settings thirdIndexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        // Fourth index settings
        Settings fourthIndexSettings = Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 1).build();

        // Create indices
        createIndex("test1", firstIndexSettings);
        createIndex("test2", secondIndexSettings);
        createIndex("test3", thirdIndexSettings);
        createIndex("test4", fourthIndexSettings);

        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();

                // Check total number of shards
                assertEquals("Total shards should be 16", 16, state.getRoutingTable().allShards().size());

                int totalAssignedShards = 0;
                int totalUnassignedShards = 0;
                Map<String, Integer> unassignedShardsByIndex = new HashMap<>();

                for (IndexRoutingTable indexRoutingTable : state.getRoutingTable()) {
                    String index = indexRoutingTable.getIndex().getName();
                    int indexUnassignedShards = 0;

                    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : shardRoutingTable) {
                            if (shardRouting.unassigned()) {
                                totalUnassignedShards++;
                                indexUnassignedShards++;
                            } else {
                                totalAssignedShards++;
                            }
                        }
                    }

                    unassignedShardsByIndex.put(index, indexUnassignedShards);
                }

                assertEquals("Total assigned shards should be 13", 13, totalAssignedShards);
                assertEquals("Total unassigned shards should be 3", 3, totalUnassignedShards);
                assertEquals("test1 should have 1 unassigned shard", 1, unassignedShardsByIndex.get("test1").intValue());
                assertEquals("test2 should have 1 unassigned shard", 1, unassignedShardsByIndex.get("test2").intValue());
                assertEquals("test3 should have 1 unassigned shard", 1, unassignedShardsByIndex.get("test3").intValue());
                assertEquals("test4 should have 0 unassigned shards", 0, unassignedShardsByIndex.get("test4").intValue());

                // Check shard distribution across nodes
                for (RoutingNode routingNode : state.getRoutingNodes()) {
                    int test1ShardCount = 0;
                    int test2ShardCount = 0;
                    int test3PrimaryShardCount = 0;
                    for (ShardRouting shardRouting : routingNode) {
                        if (shardRouting.index().getName().equals("test1")) {
                            test1ShardCount++;
                        }
                        if (shardRouting.index().getName().equals("test2")) {
                            test2ShardCount++;
                        }
                        if (shardRouting.index().getName().equals("test3") && shardRouting.primary()) {
                            test3PrimaryShardCount++;
                        }
                    }
                    assertTrue("Each node should have at most 1 shard for test1", test1ShardCount <= 1);
                    assertTrue("Each node should have at most 1 shard for test2", test2ShardCount <= 1);
                    assertTrue("Each node should have at most 1 primary shard for test3", test3PrimaryShardCount <= 1);
                }

                // Verify that test1 and test3 use segment replication
                assertEquals(
                    ReplicationType.SEGMENT,
                    IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(state.metadata().index("test1").getSettings())
                );
                assertEquals(
                    ReplicationType.SEGMENT,
                    IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(state.metadata().index("test3").getSettings())
                );
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateClusterSetting(String setting, int value) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(setting, value)).get();
    }
}
