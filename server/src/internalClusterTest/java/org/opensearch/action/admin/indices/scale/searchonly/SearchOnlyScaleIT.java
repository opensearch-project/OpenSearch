/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchOnlyScaleIT extends RemoteStoreBaseIntegTestCase {

    private static final String TEST_INDEX = "test_scale_index";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.TRUE).build();
    }

    public Settings indexSettings() {
        return Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
    }

    public void testScaleDownToSearchOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);
            int totalPrimaries = 0;
            int totalWriterReplicas = 0;
            int totalSearchReplicas = 0;
            for (IndexShardRoutingTable shardTable : routingTable) {
                if (shardTable.primaryShard() != null) {
                    totalPrimaries++;
                }
                totalWriterReplicas += shardTable.writerReplicas().size();
                totalSearchReplicas += shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            }
            assertEquals("Expected 1 primary", 1, totalPrimaries);
            assertEquals("Expected 0 writer replicas", 1, totalWriterReplicas);
            assertEquals("Expected 1 search replica", 1, totalSearchReplicas);
        });
        ensureGreen(TEST_INDEX);

        // Verify search replicas are active
        IndexShardRoutingTable shardTable = getClusterState().routingTable().index(TEST_INDEX).shard(0);
        assertEquals(1, shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count());

        // Scale down to search-only mode
        assertAcked(client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(true).get());

        // Verify index is in search-only mode
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        // Verify we can still search
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
        assertHitCount(searchResponse, 10);

        // Verify we cannot write to the index
        try {
            client().prepareIndex(TEST_INDEX).setId("new-doc").setSource("field1", "new-value").get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            // Expected exception
        }

        // Verify routing table structure
        assertEquals(0, getClusterState().routingTable().index(TEST_INDEX).shard(0).writerReplicas().size());
        assertEquals(
            1,
            getClusterState().routingTable().index(TEST_INDEX).shard(0).searchOnlyReplicas().stream().filter(ShardRouting::active).count()
        );
        assertNull(shardTable.primaryShard());

    }

    public void testScaleUpFromSearchOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(6);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 5; i++) {
            client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        // Verify initial state has expected replica counts
        IndexShardRoutingTable initialShardTable = getClusterState().routingTable().index(TEST_INDEX).shard(0);
        assertEquals(1, initialShardTable.writerReplicas().size());
        assertEquals(1, initialShardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count());

        // Scale down to search-only mode
        assertAcked(client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(true).get());
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        // Wait for search-only mode to stabilize and verify shard state
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexShardRoutingTable shardTable = state.routingTable().index(TEST_INDEX).shard(0);
            // In search-only mode, there should be no primary shard
            assertNull("Primary should be null in search-only mode", shardTable.primaryShard());
            // Only search replicas should remain
            assertEquals(0, shardTable.writerReplicas().size());
            // All shards should be search-only replicas and STARTED
            for (ShardRouting shard : shardTable) {
                assertTrue("All shards should be search-only replicas", shard.isSearchOnly());
                assertTrue("All search replicas should be started", shard.active());
            }
        });
        // Scale back up to normal mode
        assertAcked(client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(false).get());
        ensureGreen(TEST_INDEX);
        // After scaling up, wait for routing table to stabilize with the expected number of replicas
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);
            int totalPrimaries = 0;
            int totalWriterReplicas = 0;
            int totalSearchReplicas = 0;
            for (IndexShardRoutingTable shardTable : routingTable) {
                if (shardTable.primaryShard() != null) {
                    totalPrimaries++;
                }
                totalWriterReplicas += shardTable.writerReplicas().size();
                totalSearchReplicas += shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            }
            assertEquals("Expected 2 primary", 2, totalPrimaries);
            assertEquals("Expected 2 writer replicas", 2, totalWriterReplicas);
            assertEquals("Expected 2 search replica", 2, totalSearchReplicas);
        });

        // Verify index is no longer in search-only mode
        settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("false"));

        // Verify we can search existing data
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
        assertHitCount(searchResponse, 3);

        // Verify we can write to the index again
        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
            .setId("new-doc")
            .setSource("field1", "new-value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());
    }

    public void testScaleDownValidationWithoutSearchReplicas() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(6);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureYellow(TEST_INDEX);

        // Attempt to scale down should fail due to missing segment replication
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(true).get()
        );

        // Verify exception message mentions missing segment replication requirement
        assertTrue(
            "Expected error about missing search replicas",
            exception.getMessage().contains("Cannot scale to zero without search replicas for index:")
        );
    }

    /**
     * Scenario 1: Tests search-only replicas recovery with persistent data directory
     * and cluster.remote_store.state.enabled=false
     */
    public void testSearchOnlyRecoveryWithPersistentData() throws Exception {
        // Start cluster with persistent data directory
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(6);

        // Create index with search replicas
        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        // Verify initial shard allocation
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            int totalPrimaries = 0;
            int totalReplicas = 0;
            int totalSearchReplicas = 0;

            for (IndexShardRoutingTable shardTable : routingTable) {
                if (shardTable.primaryShard() != null) {
                    totalPrimaries++;
                }
                totalReplicas += shardTable.writerReplicas().size();
                totalSearchReplicas += shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            }

            assertEquals("Expected 2 primaries", 2, totalPrimaries);
            assertEquals("Expected 2 replicas", 2, totalReplicas);
            assertEquals("Expected 2 search replicas", 2, totalSearchReplicas);
        });

        // Enable search-only mode
        assertAcked(client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(true).get());

        // Verify only search replicas are active
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            for (IndexShardRoutingTable shardTable : routingTable) {
                assertNull("Primary should be null", shardTable.primaryShard());
                assertTrue("No writer replicas should exist", shardTable.writerReplicas().isEmpty());
                assertEquals(
                    "One search replica should be active",
                    1,
                    shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count()
                );
            }
        });
    }

    /**
     * Scenario 2: Tests behavior without data directory preservation
     * and cluster.remote_store.state.enabled=false
     */
    public void testRecoveryWithoutDataDirPreservation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(6);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        // Stop all nodes
        internalCluster().stopAllNodes();

        // Start nodes without data directory (simulating data loss)
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(6);

        // Verify index is not found
        expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex().setIndices(TEST_INDEX).get());
    }

    /**
     * Scenario 3: Tests behavior with cluster.remote_store.state.enabled=true
     * but without data directory preservation
     */
    public void testClusterRemoteStoreStateEnabled() throws Exception {
        // Configure cluster remote store state
        Settings remoteStoreSettings = Settings.builder().put(nodeSettings(0)).put("cluster.remote_store.state.enabled", true).build();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(6);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        // Enable search-only mode
        assertAcked(client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(true).get());

        // Stop all nodes
        internalCluster().stopAllNodes();

        // Start nodes without data directory
        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(6);

        // Verify only search replicas recover
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            for (IndexShardRoutingTable shardTable : routingTable) {
                assertTrue(
                    "Only search replicas should be active",
                    shardTable.searchOnlyReplicas().stream().anyMatch(ShardRouting::active)
                );
            }
        });
    }

    /**
     * Scenario 4: Tests recovery with persistent data directory and remote store state
     */
    public void testRecoveryWithPersistentDataAndRemoteStore() throws Exception {
        Settings remoteStoreSettings = Settings.builder().put(nodeSettings(0)).put("cluster.remote_store.state.enabled", true).build();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(6);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        // Stop and restart nodes (simulating restart with persistent data)
        internalCluster().fullRestart();

        // Verify all shards recover
        ensureGreen(TEST_INDEX);
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            int totalPrimaries = 0;
            int totalReplicas = 0;
            int totalSearchReplicas = 0;

            for (IndexShardRoutingTable shardTable : routingTable) {
                if (shardTable.primaryShard() != null) {
                    totalPrimaries++;
                }
                totalReplicas += shardTable.writerReplicas().size();
                totalSearchReplicas += shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            }

            assertEquals("Expected 2 primaries", 2, totalPrimaries);
            assertEquals("Expected 2 replicas", 2, totalReplicas);
            assertEquals("Expected 2 search replicas", 2, totalSearchReplicas);
        });

        // Enable search-only mode
        assertAcked(client().admin().indices().prepareSearchOnly(TEST_INDEX).setScaleDown(true).get());

        // Verify only search replicas remain active
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable routingTable = state.routingTable().index(TEST_INDEX);

            for (IndexShardRoutingTable shardTable : routingTable) {
                assertNull("Primary should be null", shardTable.primaryShard());
                assertTrue("No writer replicas should exist", shardTable.writerReplicas().isEmpty());
                assertEquals(
                    "One search replica should be active",
                    1,
                    shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count()
                );
            }
        });
    }
}
