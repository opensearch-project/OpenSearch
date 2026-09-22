/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShardsLimitAllocationDeciderRemoteStoreEnabledIT extends RemoteStoreBaseIntegTestCase {
    @Before
    public void setup() {
        setupCustomCluster();
    }

    private void setupCustomCluster() {
        // Start cluster manager node first
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(Settings.EMPTY);
        // Start data nodes
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        // Wait for green cluster state
        ensureGreen();
    }

    public void testIndexPrimaryShardLimit() throws Exception {
        // Create first index with primary shard limit
        Settings firstIndexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 4))  // 4 shards, 0 replicas
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        // Create first index
        createIndex("test1", firstIndexSettings);

        // Create second index
        createIndex("test2", remoteStoreIndexSettings(0, 4));

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();

            // Check total number of shards (8 total: 4 from each index)
            assertEquals("Total shards should be 8", 8, state.getRoutingTable().allShards().size());

            // Count assigned and unassigned shards for test1
            int test1AssignedShards = 0;
            int test1UnassignedShards = 0;
            Map<String, Integer> nodePrimaryCount = new HashMap<>();

            // Check test1 shard distribution
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test1")) {
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode()) {
                        test1AssignedShards++;
                        // Count primaries per node for test1
                        String nodeId = shard.currentNodeId();
                        nodePrimaryCount.merge(nodeId, 1, Integer::sum);
                    } else {
                        test1UnassignedShards++;
                    }
                }
            }

            // Check test2 shard assignment
            int test2UnassignedShards = 0;
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test2")) {
                for (ShardRouting shard : shardRouting) {
                    if (!shard.assignedToNode()) {
                        test2UnassignedShards++;
                    }
                }
            }

            // Assertions
            assertEquals("test1 should have 3 assigned shards", 3, test1AssignedShards);
            assertEquals("test1 should have 1 unassigned shard", 1, test1UnassignedShards);
            assertEquals("test2 should have no unassigned shards", 0, test2UnassignedShards);

            // Verify no node has more than one primary shard of test1
            for (Integer count : nodePrimaryCount.values()) {
                assertTrue("No node should have more than 1 primary shard of test1", count <= 1);
            }
        });
        cleanUp("test1", "test2");
    }

    public void testUpdatingIndexPrimaryShardLimit() throws Exception {
        // Create first index with primary shard limit
        Settings firstIndexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 4))  // 4 shards, 0 replicas
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        // Create first index
        createIndex("test1", firstIndexSettings);

        // Update the index settings to set INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("test1");
        Settings updatedSettings = Settings.builder().put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1).build();
        updateSettingsRequest.settings(updatedSettings);

        AcknowledgedResponse response = client().admin().indices().updateSettings(updateSettingsRequest).actionGet();

        assertTrue(response.isAcknowledged());

        // Create second index
        createIndex("test2", remoteStoreIndexSettings(0, 4));

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();

            // Check total number of shards (8 total: 4 from each index)
            assertEquals("Total shards should be 8", 8, state.getRoutingTable().allShards().size());

            // Count assigned and unassigned shards for test1
            int test1AssignedShards = 0;
            int test1UnassignedShards = 0;
            Map<String, Integer> nodePrimaryCount = new HashMap<>();

            // Check test1 shard distribution
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test1")) {
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode()) {
                        test1AssignedShards++;
                        // Count primaries per node for test1
                        String nodeId = shard.currentNodeId();
                        nodePrimaryCount.merge(nodeId, 1, Integer::sum);
                    } else {
                        test1UnassignedShards++;
                    }
                }
            }

            // Check test2 shard assignment
            int test2UnassignedShards = 0;
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test2")) {
                for (ShardRouting shard : shardRouting) {
                    if (!shard.assignedToNode()) {
                        test2UnassignedShards++;
                    }
                }
            }

            // Assertions
            assertEquals("test1 should have 3 assigned shards", 3, test1AssignedShards);
            assertEquals("test1 should have 1 unassigned shard", 1, test1UnassignedShards);
            assertEquals("test2 should have no unassigned shards", 0, test2UnassignedShards);

            // Verify no node has more than one primary shard of test1
            for (Integer count : nodePrimaryCount.values()) {
                assertTrue("No node should have more than 1 primary shard of test1", count <= 1);
            }
        });
        cleanUp("test1");
    }

    public void testIndexTemplateWithPrimaryShardLimit() throws Exception {
        // Put an index template that carries index.routing.allocation.total_primary_shards_per_node.
        // On an all-remote-store cluster this must be acknowledged: the template validator should
        // recognise the cluster as remote-store-enabled (via node attributes) rather than looking for
        // the index-local index.remote_store.enabled flag, which a template can never carry.
        AcknowledgedResponse templateResponse = client().admin()
            .indices()
            .preparePutTemplate("primary-shard-limit-template")
            .setPatterns(Collections.singletonList("template-test*"))
            .setSettings(
                Settings.builder()
                    .put(remoteStoreIndexSettings(0, 4))  // 4 shards, 0 replicas
                    .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            )
            .get();

        assertTrue("Template carrying total_primary_shards_per_node should be acknowledged", templateResponse.isAcknowledged());

        // Auto-create the index by indexing a document so the template (not a create-request settings
        // block) fully drives the index settings — including number_of_shards and the primary shard limit.
        client().prepareIndex("template-test1").setId("1").setSource("field", "value").get();

        // The setting (and the injected remote-store flag) must have flowed from the template onto the
        // concrete index — this is the core of the bug: previously the template PUT was rejected with 400.
        ClusterState createdState = client().admin().cluster().prepareState().get().getState();
        Settings indexSettings = createdState.metadata().index("template-test1").getSettings();
        assertEquals(
            "Index created from template should carry 4 primary shards",
            4,
            createdState.metadata().index("template-test1").getNumberOfShards()
        );
        assertEquals(
            "Index created from template should carry the primary shard limit",
            Integer.valueOf(1),
            INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.get(indexSettings)
        );
        assertTrue(
            "Index created from template on a remote-store cluster should be remote-store enabled",
            IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexSettings)
        );

        // And the limit must actually be enforced at allocation time: with 4 primaries, a limit of 1
        // per node and 3 data nodes, at least one primary stays unassigned and no node holds more than one.
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();

            int assignedShards = 0;
            int unassignedShards = 0;
            Map<String, Integer> nodePrimaryCount = new HashMap<>();

            for (IndexShardRoutingTable shardRouting : state.routingTable().index("template-test1")) {
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode()) {
                        assignedShards++;
                        nodePrimaryCount.merge(shard.currentNodeId(), 1, Integer::sum);
                    } else {
                        unassignedShards++;
                    }
                }
            }

            assertEquals("template-test1 should have 3 assigned primaries (one per data node)", 3, assignedShards);
            assertEquals("template-test1 should have 1 unassigned primary (blocked by the per-node limit)", 1, unassignedShards);
            for (Integer count : nodePrimaryCount.values()) {
                assertTrue("No node should have more than 1 primary shard of template-test1", count <= 1);
            }
        });

        client().admin().indices().prepareDeleteTemplate("primary-shard-limit-template").get();
        cleanUp("template-test1");
    }

    public void testClusterPrimaryShardLimitss() throws Exception {
        // Update cluster setting to limit primary shards per node
        updateClusterSetting(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1);

        // Create index with 4 shards and 1 replica
        createIndex("test1", remoteStoreIndexSettings(1, 4));

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();

            // Check total number of shards (8 total: 4 primaries + 4 replicas)
            assertEquals("Total shards should be 8", 8, state.getRoutingTable().allShards().size());

            // Count assigned and unassigned shards for test1
            int assignedShards = 0;
            int unassignedShards = 0;
            int unassignedPrimaries = 0;
            int unassignedReplicas = 0;
            Map<String, Integer> nodePrimaryCount = new HashMap<>();

            // Check shard distribution
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test1")) {
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode()) {
                        assignedShards++;
                        if (shard.primary()) {
                            // Count primaries per node
                            String nodeId = shard.currentNodeId();
                            nodePrimaryCount.merge(nodeId, 1, Integer::sum);
                        }
                    } else {
                        unassignedShards++;
                        if (shard.primary()) {
                            unassignedPrimaries++;
                        } else {
                            unassignedReplicas++;
                        }
                    }
                }
            }

            // Assertions
            assertEquals("Should have 6 assigned shards", 6, assignedShards);
            assertEquals("Should have 2 unassigned shards", 2, unassignedShards);
            assertEquals("Should have 1 unassigned primary", 1, unassignedPrimaries);
            assertEquals("Should have 1 unassigned replica", 1, unassignedReplicas);

            // Verify no node has more than one primary shard
            for (Integer count : nodePrimaryCount.values()) {
                assertTrue("No node should have more than 1 primary shard", count <= 1);
            }
        });
        cleanUp("test1");
    }

    public void testCombinedIndexAndClusterPrimaryShardLimits() throws Exception {
        // Set cluster-wide primary shard limit to 3
        updateClusterSetting(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 3);

        // Create first index with index-level primary shard limit
        Settings firstIndexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(1, 4))  // 4 shards, 1 replica
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 1)
            .build();

        // Create first index
        createIndex("test1", firstIndexSettings);

        // Create second index with no index-level limits
        createIndex("test2", remoteStoreIndexSettings(1, 4));  // 4 shards, 1 replica

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();

            // Check total number of shards (16 total: 8 from each index - 4 primaries + 4 replicas each)
            assertEquals("Total shards should be 16", 16, state.getRoutingTable().allShards().size());

            // Count assigned and unassigned shards for both indices
            int totalAssignedShards = 0;
            int test1UnassignedPrimaries = 0;
            int test1UnassignedReplicas = 0;
            int test2UnassignedShards = 0;
            Map<String, Integer> nodePrimaryCount = new HashMap<>();

            // Check test1 shard distribution
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test1")) {
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode()) {
                        totalAssignedShards++;
                        if (shard.primary()) {
                            String nodeId = shard.currentNodeId();
                            nodePrimaryCount.merge(nodeId, 1, Integer::sum);
                        }
                    } else {
                        if (shard.primary()) {
                            test1UnassignedPrimaries++;
                        } else {
                            test1UnassignedReplicas++;
                        }
                    }
                }
            }

            // Check test2 shard distribution
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test2")) {
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode()) {
                        totalAssignedShards++;
                        if (shard.primary()) {
                            String nodeId = shard.currentNodeId();
                            nodePrimaryCount.merge(nodeId, 1, Integer::sum);
                        }
                    } else {
                        test2UnassignedShards++;
                    }
                }
            }

            // Assertions
            assertEquals("Should have 14 assigned shards", 14, totalAssignedShards);
            assertEquals("Should have 1 unassigned primary in test1", 1, test1UnassignedPrimaries);
            assertEquals("Should have 1 unassigned replica in test1", 1, test1UnassignedReplicas);
            assertEquals("Should have no unassigned shards in test2", 0, test2UnassignedShards);

            // Verify no node has more than one primary shard for test1
            for (IndexShardRoutingTable shardRouting : state.routingTable().index("test1")) {
                Map<String, Integer> test1NodePrimaryCount = new HashMap<>();
                for (ShardRouting shard : shardRouting) {
                    if (shard.assignedToNode() && shard.primary()) {
                        test1NodePrimaryCount.merge(shard.currentNodeId(), 1, Integer::sum);
                    }
                }
                for (Integer count : test1NodePrimaryCount.values()) {
                    assertTrue("No node should have more than 1 primary shard of test1", count <= 1);
                }
            }

            // Verify no node has more than three primary shards total (cluster-wide limit)
            for (Integer count : nodePrimaryCount.values()) {
                assertTrue("No node should have more than 3 primary shards total", count <= 3);
            }
        });
        cleanUp("test1", "test2");
    }

    private void updateClusterSetting(String setting, int value) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(setting, value)).get();
    }

    private void cleanUp(String... indices) throws Exception {
        logger.info(">>> Starting custom tearDown in ShardsLimitAllocationDeciderRemoteStoreEnabledIT");
        // Synchronization: Force flush relevant indices.
        logger.info("Attempting to flush indices {} to help sync remote store before cleanup...", Arrays.toString(indices));
        FlushRequest flushRequest = new FlushRequest(indices);
        flushRequest.force(true); // Force even if no changes detected
        flushRequest.waitIfOngoing(true); // Wait if flush already in progress
        client().admin().indices().flush(flushRequest).actionGet(); // Use actionGet() or get() to wait
        logger.info("Flush request for {} completed.", Arrays.toString(indices));
    }
}
