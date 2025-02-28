/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
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

    /**
     * Integration test to verify the behavior of INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
     * in a non-remote store environment.
     *
     * Scenario:
     * An end-user attempts to create an index with INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
     * on a cluster where remote store is not enabled.
     *
     * Expected Outcome:
     * The system should reject the index creation request and throw an appropriate exception,
     * indicating that this setting is only applicable for remote store enabled clusters.
     */
    public void testIndexTotalPrimaryShardsPerNodeSettingWithoutRemoteStore() {
        // Attempt to create an index with INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
        Settings indexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 3)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        // Assert that creating the index throws an exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { createIndex("test_index", indexSettings); }
        );

        // Verify the exception message
        assertTrue(
            "Exception should mention that the setting requires remote store",
            exception.getMessage()
                .contains(
                    "Setting [index.routing.allocation.total_primary_shards_per_node] can only be used with remote store enabled clusters"
                )
        );
    }

    /**
     * Integration test to verify the behavior of CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
     * in a non-remote store environment.
     *
     * Scenario:
     * An end-user attempts to create an index with CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
     * on a cluster where remote store is not enabled.
     *
     * Expected Outcome:
     * The system should reject the index creation request and throw an appropriate exception,
     * indicating that this setting is only applicable for remote store enabled clusters.
     */
    public void testClusterTotalPrimaryShardsPerNodeSettingWithoutRemoteStore() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            updateClusterSetting(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1);
        });

        // Verify the exception message
        assertTrue(
            "Exception should mention that the setting requires remote store",
            exception.getMessage()
                .contains(
                    "Setting [cluster.routing.allocation.total_primary_shards_per_node] can only be used with remote store enabled clusters"
                )
        );

        // Attempt to create an index with INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
        Settings indexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 3)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        createIndex("test_index", indexSettings);
    }

    private void updateClusterSetting(String setting, int value) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(setting, value)).get();
    }
}
