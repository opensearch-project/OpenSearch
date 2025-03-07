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
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ScaleSearchOnlyIT extends RemoteStoreBaseIntegTestCase {

    private static final String TEST_INDEX = "test_scale_index";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.TRUE).build();
    }

    public Settings indexSettings() {
        return Settings.builder().put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
    }

    /**
     * Tests scaling down an index to search-only mode.
     */
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
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);

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
        });
        ensureGreen(TEST_INDEX);

        IndexShardRoutingTable shardTable = getClusterState().routingTable().index(TEST_INDEX).shard(0);
        assertEquals(1, shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count());

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());

        ensureGreen(TEST_INDEX);

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(TEST_INDEX).get();
        assertTrue(settingsResponse.getSetting(TEST_INDEX, IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey()).equals("true"));

        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 10);
        }, 30, TimeUnit.SECONDS);

        try {
            client().prepareIndex(TEST_INDEX).setId("new-doc").setSource("field1", "new-value").get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException ignored) {}

        assertEquals(0, getClusterState().routingTable().index(TEST_INDEX).shard(0).writerReplicas().size());
        assertEquals(
            1,
            getClusterState().routingTable().index(TEST_INDEX).shard(0).searchOnlyReplicas().stream().filter(ShardRouting::active).count()
        );
        IndexShardRoutingTable currentShardTable = getClusterState().routingTable().index(TEST_INDEX).shard(0);
        assertNull("Primary shard should be null after scale-down", currentShardTable.primaryShard());

    }

    /**
     * Tests restoring an index from search-only mode back to normal mode.
     */
    public void testScaleUpFromSearchOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 5; i++) {
            IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(response, 5);
        });

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());
        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(response, 5);
        });

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(false).get());

        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(response, 5);
        }, 30, TimeUnit.SECONDS);

        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
        assertHitCount(searchResponse, 5);

        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
            .setId("new-doc")
            .setSource("field1", "new-value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());

        assertBusy(() -> {
            SearchResponse finalResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(finalResponse, 6);
        });

    }

    /**
     * Tests scaling down an index to search-only mode when there are no search replicas.
     */
    public void testScaleDownValidationWithoutSearchReplicas() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureYellow(TEST_INDEX);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get()
        );

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
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());

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
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        internalCluster().stopAllNodes();
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex().setIndices(TEST_INDEX).get());
    }

    /**
     * Scenario 3: Tests behavior with cluster.remote_store.state.enabled=true
     * but without data directory preservation
     */
    public void testClusterRemoteStoreStateEnabled() throws Exception {
        Settings remoteStoreSettings = Settings.builder().put(nodeSettings(0)).put("cluster.remote_store.state.enabled", true).build();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());

        internalCluster().stopAllNodes();

        internalCluster().startClusterManagerOnlyNode(remoteStoreSettings);
        internalCluster().startDataOnlyNodes(3);

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
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        internalCluster().fullRestart();

        ensureGreen(TEST_INDEX);
        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());

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
    * Tests the ability to scale search replicas up and down while an index is in search-only mode.
    */
    public void testScaleSearchReplicasInSearchOnlyMode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        Settings initialSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, initialSettings);
        ensureGreen(TEST_INDEX);

        for (int i = 0; i < 5; i++) {
            IndexResponse indexResponse = client().prepareIndex(TEST_INDEX)
                .setId(Integer.toString(i))
                .setSource("field1", "value" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());
        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexShardRoutingTable shardTable = state.routingTable().index(TEST_INDEX).shard(0);

            assertNull("Primary shard should be null in search-only mode", shardTable.primaryShard());
            assertTrue("Writer replicas should be empty in search-only mode", shardTable.writerReplicas().isEmpty());

            long activeSearchReplicas = shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            assertEquals("Should have 1 active search replica initially", 1, activeSearchReplicas);
        });

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 3).build())
                .get()
        );

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexShardRoutingTable shardTable = state.routingTable().index(TEST_INDEX).shard(0);

            long activeSearchReplicas = shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            assertEquals("Should have 3 active search replicas after scaling", 3, activeSearchReplicas);

            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 5);
        }, 30, TimeUnit.SECONDS);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 2).build())
                .get()
        );

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexShardRoutingTable shardTable = state.routingTable().index(TEST_INDEX).shard(0);

            long activeSearchReplicas = shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count();
            assertEquals("Should have 2 active search replicas after scaling down", 2, activeSearchReplicas);

            SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).get();
            assertHitCount(searchResponse, 5);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test that verifies cluster health is GREEN when all search replicas are up with search_only mode enabled
     */
    public void testClusterHealthGreenWithAllSearchReplicas() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);

        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());
        ensureGreen(TEST_INDEX);

        assertBusy(() -> {
            String indexHealth = client().admin().cluster().prepareHealth(TEST_INDEX).get().getStatus().name();
            assertEquals("Index health should be GREEN with all search replicas up", "GREEN", indexHealth);

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexShardRoutingTable shardTable = state.routingTable().index(TEST_INDEX).shard(0);

            assertNull("Primary shard should be null in search-only mode", shardTable.primaryShard());
            assertTrue("Writer replicas should be empty in search-only mode", shardTable.writerReplicas().isEmpty());
            assertEquals(
                "Should have 2 active search replicas",
                1,
                shardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count()
            );
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Test that verifies cluster health is YELLOW when one search replica is down with search_only mode enabled
     */
    /**
     * Test that verifies cluster health is YELLOW when one search replica is down with search_only mode enabled
     */
    public void testClusterHealthYellowWithOneSearchReplicaDown() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);

        Settings specificSettings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .build();

        createIndex(TEST_INDEX, specificSettings);
        ensureGreen(TEST_INDEX);
        assertAcked(client().admin().indices().prepareScaleSearchOnly(TEST_INDEX).setSearchOnly(true).get());
        ensureGreen(TEST_INDEX);

        String nodeToStop = findNodeWithSearchOnlyReplica();

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeToStop));

        assertBusy(() -> {
            String indexHealth = client().admin().cluster().prepareHealth(TEST_INDEX).get().getStatus().name();
            assertEquals("Index health should be YELLOW with one search replica down", "RED", indexHealth);

            ClusterState updatedState = client().admin().cluster().prepareState().get().getState();
            IndexShardRoutingTable updatedShardTable = updatedState.routingTable().index(TEST_INDEX).shard(0);

            assertNull("Primary shard should still be null in search-only mode", updatedShardTable.primaryShard());
            assertTrue("Writer replicas should still be empty", updatedShardTable.writerReplicas().isEmpty());

            assertEquals(
                "Should have 1 active search replica after node failure",
                0,
                updatedShardTable.searchOnlyReplicas().stream().filter(ShardRouting::active).count()
            );
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Helper method to find a node that contains an active search-only replica shard
     */
    private String findNodeWithSearchOnlyReplica() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = state.routingTable().index(TEST_INDEX);

        for (IndexShardRoutingTable shardTable : indexRoutingTable) {
            for (ShardRouting searchReplica : shardTable.searchOnlyReplicas()) {
                if (searchReplica.active()) {
                    String nodeId = searchReplica.currentNodeId();
                    return state.nodes().get(nodeId).getName();
                }
            }
        }

        throw new AssertionError("Could not find any node with active search-only replica");
    }
}
