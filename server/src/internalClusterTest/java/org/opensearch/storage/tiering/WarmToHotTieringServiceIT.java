/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.InternalClusterInfoService;
import org.opensearch.cluster.MockInternalClusterInfoService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.node.Node;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.search.SearchHit;
import org.opensearch.storage.action.tiering.HotToWarmTierAction;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.storage.action.tiering.WarmToHotTierAction;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.opensearch.storage.common.tiering.TieringUtils.W2H_MAX_CONCURRENT_TIERING_REQUESTS_KEY;
import static org.opensearch.storage.tiering.TieringTestUtils.buildDisabledAllocationSettings;
import static org.opensearch.storage.tiering.TieringTestUtils.buildEnabledAllocationSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class WarmToHotTieringServiceIT extends RemoteStoreBaseIntegTestCase {

    protected final String INDEX_NAME = "h2w-test-idx-1";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int MIGRATION_TIMEOUT_SECONDS = 30;
    private static final long TOTAL_SPACE_BYTES = new ByteSizeValue(1000, ByteSizeUnit.KB).getBytes();
    protected static final Map<String, Class<? extends Plugin>> TEST_SPECIFIC_CUSTOM_CLUSTER_INFO = new HashMap<>();

    static {
        TEST_SPECIFIC_CUSTOM_CLUSTER_INFO.put("testHotMigrationWithNoNodesCapacity", MockInternalClusterInfoService.TestPlugin.class);
        TEST_SPECIFIC_CUSTOM_CLUSTER_INFO.put(
            "testHotMigrationWithNoSpaceForLargestShard",
            MockInternalClusterInfoService.TestPlugin.class
        );
        TEST_SPECIFIC_CUSTOM_CLUSTER_INFO.put(
            "testHotMigrationWithNoSpaceForLargestShard_YellowIndex",
            MockInternalClusterInfoService.TestPlugin.class
        );
        TEST_SPECIFIC_CUSTOM_CLUSTER_INFO.put(
            "testHotMigrationWithNoNodesCapacity_YellowIndex",
            MockInternalClusterInfoService.TestPlugin.class
        );
        TEST_SPECIFIC_CUSTOM_CLUSTER_INFO.put(
            "testWarmMigrationWithAllNodesJVMUtilizationBreached",
            MockInternalClusterInfoService.TestPlugin.class
        );
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        String testName = getTestName();
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        Class<? extends Plugin> extra = TEST_SPECIFIC_CUSTOM_CLUSTER_INFO.get(testName);
        if (extra != null) {
            plugins.add(extra);
        }
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.GB))
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    // Helper Methods
    protected void setupCluster(int numberOfReplicas, Settings clusterSettings) {

        internalCluster().startClusterManagerOnlyNode(clusterSettings);
        // Minimum 2 hot nodes needed because warm migration sets replicas to 1,
        // and W2H validator needs second-highest node capacity for replica placement
        internalCluster().startDataOnlyNodes(Math.max(numberOfReplicas + 1, 2), clusterSettings);
        internalCluster().startWarmOnlyNodes(2, clusterSettings);
        interceptCheckpointUpdates();
    }

    protected void setupCluster(int numberOfReplicas) {
        setupCluster(numberOfReplicas, Settings.EMPTY);
    }

    /**
     * Intercepts the UPDATE_VISIBLE_CHECKPOINT transport action on all nodes to gracefully handle
     * the known race condition where a checkpoint update arrives during relocation handoff.
     * See: https://github.com/opensearch-project/OpenSearch/issues/3923
     */
    protected void interceptCheckpointUpdates() {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class,
                nodeName
            );
            mockTransportService.addRequestHandlingBehavior(
                SegmentReplicationSourceService.Actions.UPDATE_VISIBLE_CHECKPOINT,
                (handler, request, channel, task) -> {
                    try {
                        handler.messageReceived(request, channel, task);
                    } catch (AssertionError e) {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    }
                }
            );
        }
    }

    protected void createTestIndex(String indexName, int numberOfShards, int numberOfReplicas) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();

        createIndex(indexName, indexSettings);
        ensureGreen(indexName);
    }

    protected void migrateToWarm(String indexName) throws Exception {
        CountDownLatch h2wLatch = new CountDownLatch(1);
        ClusterStateListener h2wListener = new TieringTestUtils.MockTieringCompletionListener(
            h2wLatch,
            indexName,
            IndexModule.TieringState.HOT_TO_WARM.toString()
        );

        try {
            clusterService().addListener(h2wListener);

            logger.info("starting hot to warm tiering for index: {}", indexName);
            final IndexTieringRequest request = new IndexTieringRequest("warm", indexName);
            AcknowledgedResponse response = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertTrue("Hot to Warm migration timed out", h2wLatch.await(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertBusy(() -> {
                assertIndexShardLocation(indexName, true);
                verifyWarmIndexSettings(indexName);
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw e;
        } finally {
            clusterService().removeListener(h2wListener);
        }
    }

    protected void migrateToHot(String indexName) throws Exception {

        CountDownLatch w2hLatch = new CountDownLatch(1);
        ClusterStateListener w2hListener = new TieringTestUtils.MockTieringCompletionListener(
            w2hLatch,
            indexName,
            IndexModule.TieringState.WARM_TO_HOT.toString()
        );

        try {
            clusterService().addListener(w2hListener);

            final IndexTieringRequest request = new IndexTieringRequest("hot", indexName);
            AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertTrue("Warm to Hot migration timed out", w2hLatch.await(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertBusy(() -> {
                assertIndexShardLocation(indexName, false);
                verifyHotIndexSettings(indexName);
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            clusterService().removeListener(w2hListener);
        }
    }

    // Helper method to verify index shard location
    protected void assertIndexShardLocation(String indexName, boolean onWarmNodes) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);

        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            ShardRouting shardRouting = shardRoutingTable.primaryShard();
            String assignedNodeId = shardRouting.currentNodeId();
            DiscoveryNode assignedNode = state.nodes().get(assignedNodeId);

            if (shardRouting.unassigned() && shardRouting.primary() == false || assignedNode == null) {
                // continue if replica shard is unassigned.
                continue;
            }
            if (onWarmNodes) {
                assertTrue("Shard should be on node warm node ", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            } else {
                assertFalse("Shard should not be on warm node ", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            }
        }
    }

    protected void verifyWarmIndexSettings(String indexName) {
        Settings currentSettings = internalCluster().clusterService().state().metadata().index(indexName).getSettings();

        Map<String, Object> expectedSettings = TieringTestUtils.getExpectedWarmIndexSettings();
        for (Map.Entry<String, Object> entry : expectedSettings.entrySet()) {
            assertEquals(
                "Incorrect value for setting: " + entry.getKey(),
                entry.getValue().toString(),
                currentSettings.get(entry.getKey())
            );
        }
    }

    protected void verifyHotIndexSettings(String indexName) {
        Settings currentSettings = internalCluster().clusterService().state().metadata().index(indexName).getSettings();

        Map<String, Object> expectedSettings = TieringTestUtils.getExpectedHotIndexSettings();
        for (Map.Entry<String, Object> entry : expectedSettings.entrySet()) {
            assertEquals(
                "Incorrect value for setting: " + entry.getKey(),
                entry.getValue().toString(),
                currentSettings.get(entry.getKey())
            );
        }
    }

    protected long getDocCount(String indexName) {
        refresh(indexName);
        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
        return response.getHits().getTotalHits().value();
    }

    public void testHotToWarmAndWarmToHotMigrationWithAndWithoutReplica() throws Exception {
        int numberOfReplicas = randomIntBetween(0, 2);
        int numberOfShards = randomIntBetween(1, 2);
        logger.info("Testing hot to warm migration with {} replicas and {} shards", numberOfReplicas, numberOfShards);

        // Setup and create index
        setupCluster(numberOfReplicas);
        createTestIndex(INDEX_NAME, numberOfShards, numberOfReplicas);

        // Verify initial state
        assertIndexShardLocation(INDEX_NAME, false);

        // Migrate to warm
        migrateToWarm(INDEX_NAME);

        // Migrate back to hot
        migrateToHot(INDEX_NAME);

        // Cleanup
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotToWarmAndWarmToHotMigrationWithAndWithoutReplicaWithData() throws Exception {
        int numberOfReplicas = randomIntBetween(1, 2);
        int numberOfShards = randomIntBetween(1, 2);
        logger.info("Testing migration with replicas: {}, shards: {}", numberOfReplicas, numberOfShards);

        // Setup and create index
        setupCluster(numberOfReplicas);
        createTestIndex(INDEX_NAME, numberOfShards, numberOfReplicas);
        ensureGreen(INDEX_NAME);

        // Index initial documents
        Map<String, Long> initialIndexStats = indexData(randomIntBetween(1, 3), true, INDEX_NAME);
        long initialDocCount = initialIndexStats.get(TOTAL_OPERATIONS);
        refresh(INDEX_NAME);
        flush(INDEX_NAME);

        // Verify initial state and doc count
        assertIndexShardLocation(INDEX_NAME, false);
        assertEquals("Initial document count mismatch", initialDocCount, getDocCount(INDEX_NAME));

        // Migrate to warm
        migrateToWarm(INDEX_NAME);

        // Index more documents while in warm tier
        Map<String, Long> warmIndexStats = indexData(randomIntBetween(1, 3), true, INDEX_NAME);
        long additionalDocs = warmIndexStats.get(TOTAL_OPERATIONS);
        long totalExpectedDocs = initialDocCount + additionalDocs;
        refresh(INDEX_NAME);
        flush(INDEX_NAME);

        // Verify doc count in warm tier
        assertEquals("Document count mismatch in warm tier", totalExpectedDocs, getDocCount(INDEX_NAME));

        // Migrate back to hot
        migrateToHot(INDEX_NAME);

        // Verify final doc count
        assertEquals("Document count changed after W2H migration", totalExpectedDocs, getDocCount(INDEX_NAME));

        // Cleanup
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotToWarmMigrationWithDeletedDocumentSuccess() throws Exception {
        int numberOfReplicas = randomIntBetween(1, 2);
        logger.info("Testing migration with document deletions, replicas: {}", numberOfReplicas);

        // Setup and create index
        setupCluster(numberOfReplicas);
        createTestIndex(INDEX_NAME, 1, numberOfReplicas);
        ensureGreen(INDEX_NAME);

        // Index initial documents
        Map<String, Long> indexStats = indexData(randomIntBetween(1, 3), true, INDEX_NAME);
        long initialDocCount = indexStats.get(TOTAL_OPERATIONS);
        refresh(INDEX_NAME);
        flush(INDEX_NAME);

        // Delete some documents before warm migration
        SearchResponse initialSearchResponse = client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(2).get();

        List<String> initialDeletedIds = new ArrayList<>();
        for (SearchHit hit : initialSearchResponse.getHits()) {
            String docId = hit.getId();
            initialDeletedIds.add(docId);
            assertEquals(DocWriteResponse.Result.DELETED, client().prepareDelete(INDEX_NAME, docId).get().getResult());
        }
        long expectedDocsAfterInitialDelete = initialDocCount - initialDeletedIds.size();
        refresh(INDEX_NAME);
        flush(INDEX_NAME);

        // Verify initial state and doc count
        assertIndexShardLocation(INDEX_NAME, false);
        assertEquals("Document count mismatch after initial deletions", expectedDocsAfterInitialDelete, getDocCount(INDEX_NAME));

        // Migrate to warm
        migrateToWarm(INDEX_NAME);

        // Delete more documents while in warm tier
        SearchResponse warmSearchResponse = client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(2).get();

        List<String> warmDeletedIds = new ArrayList<>();
        for (SearchHit hit : warmSearchResponse.getHits()) {
            String docId = hit.getId();
            warmDeletedIds.add(docId);
            assertEquals(DocWriteResponse.Result.DELETED, client().prepareDelete(INDEX_NAME, docId).get().getResult());
        }
        long expectedDocsAfterWarmDelete = expectedDocsAfterInitialDelete - warmDeletedIds.size();
        refresh(INDEX_NAME);
        flush(INDEX_NAME);

        // Verify doc count before hot migration
        assertEquals("Document count mismatch after warm tier deletions", expectedDocsAfterWarmDelete, getDocCount(INDEX_NAME));

        // Migrate to hot
        migrateToHot(INDEX_NAME);

        // Verify deleted documents remain deleted
        SearchResponse deletedDocsSearch = client().prepareSearch(INDEX_NAME)
            .setQuery(
                QueryBuilders.idsQuery().addIds(Stream.concat(initialDeletedIds.stream(), warmDeletedIds.stream()).toArray(String[]::new))
            )
            .setSize(initialDeletedIds.size() + warmDeletedIds.size())
            .get();
        assertEquals("Deleted documents reappeared after migration", 0, deletedDocsSearch.getHits().getTotalHits().value());

        // Cleanup
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    /*
    If there is a quorum loss, restoration of indices metadata happens at a later event than the master-change.
    This test creates a domain with one master node, creates a quorum loss and checks if the tieringContext
    is reconstructed properly
    */
    public void testWarmToHotMigrationDuringQuorumLoss() throws Exception {
        logger.info("--> Starting test for warm-to-hot migration during quorum loss");

        Settings logsettings = Settings.builder().put("logger.org.opensearch.cluster.routing.allocation", "TRACE").build();

        // Start cluster nodes with 1 master node for quorum
        String masterNodeId = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startWarmOnlyNodes(2);
        interceptCheckpointUpdates();

        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(logsettings).get();

        final String[] indices = new String[] { "test-1", "test-2" };
        Map<String, Long> indexDocCounts = new HashMap<>();

        try {
            // Create indices and add data
            for (String index : indices) {
                createTestIndex(index, 1, 1);
                Map<String, Long> stats = indexData(1, false, index);
                indexDocCounts.put(index, stats.get(TOTAL_OPERATIONS));
                logger.info("--> Created index {} with {} documents", index, stats.get(TOTAL_OPERATIONS));
            }
            ensureGreen(indices);

            // Move all indices to warm tier first
            for (String index : indices) {
                migrateToWarm(index);
            }

            refresh(indices);

            // disable allocation to block migrations.
            logger.info("--> Enabling allocation to allow migrations to complete");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Start migration back to hot for all indices
            logger.info("--> Starting warm to hot migration for all indices");
            for (String index : indices) {
                final IndexTieringRequest request = new IndexTieringRequest("hot", index);
                AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
                assertTrue(response.isAcknowledged());
            }

            // Sleep to ensure migration requests are processed
            TieringTestUtils.safeSleep(5000);

            // Verify migrations are stuck due to disabled allocation
            for (String index : indices) {
                assertBusy(() -> {
                    ClusterState state = client().admin().cluster().prepareState().get().getState();
                    ShardRouting shardRouting = state.routingTable().index(index).shard(0).primaryShard();
                    assertFalse("Shard should not be relocating", shardRouting.relocating());
                    assertIndexShardLocation(index, true);  // Should still be on warm nodes
                }, 30, TimeUnit.SECONDS);
            }

            // Restart current master node
            logger.info("--> Restarting current master node");
            internalCluster().restartNode(masterNodeId);

            // Wait for cluster to stabilize with new master
            ensureStableCluster(5); // Remaining nodes (1 master + 2 data + 2 warm)
            logger.info("--> Cluster stabilized after master node failure");

            // Enable allocation to allow migrations to proceed
            logger.info("--> Enabling allocation to allow migrations to complete");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            for (String index : indices) {
                client().admin()
                    .cluster()
                    .restoreRemoteStore(
                        new RestoreRemoteStoreRequest().indices(index).restoreAllShards(false),
                        PlainActionFuture.newFuture()
                    );
            }

            // Verify migrations complete on remaining master
            assertBusy(() -> {
                for (String index : indices) {
                    assertIndexShardLocation(index, false);  // Should now be on hot nodes
                    verifyHotIndexSettings(index);
                }
            }, 60, TimeUnit.SECONDS);

            // Verify data consistency
            for (String index : indices) {
                refresh(index);
                long expectedCount = indexDocCounts.get(index);
                flush(index);
                long actualCount = client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value();

                logger.info("--> Index {} document count - expected: {}, actual: {}", index, expectedCount, actualCount);
                assertEquals("Document count mismatch for index " + index, expectedCount, actualCount);
            }

        } finally {
            // Cleanup
            for (String index : indices) {
                client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            }
        }

    }

    public void testWarmToHotMigrationWithIndexDeletion() throws Exception {
        int numberOfReplicas = randomIntBetween(0, 2);
        int numberOfShards = randomIntBetween(1, 2);
        logger.info("Testing W2H migration with index deletion, replicas: {}, shards: {}", numberOfReplicas, numberOfShards);

        // Setup and create index
        setupCluster(numberOfReplicas);
        createTestIndex(INDEX_NAME, numberOfShards, numberOfReplicas);
        ensureGreen(INDEX_NAME);

        // First migrate to warm tier
        migrateToWarm(INDEX_NAME);

        // Create latch to track migration start
        CountDownLatch w2hMigrationStarted = new CountDownLatch(1);

        // Add listener to detect when shards start relocating
        ClusterStateListener relocationListener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().routingTable().hasIndex(INDEX_NAME)) {
                    boolean isRelocating = event.state().routingTable().allShards(INDEX_NAME).stream().anyMatch(ShardRouting::relocating);

                    if (isRelocating) {
                        w2hMigrationStarted.countDown();
                    }
                }
            }
        };

        try {
            clusterService().addListener(relocationListener);

            // Start warm to hot migration
            final IndexTieringRequest hotRequest = new IndexTieringRequest("hot", INDEX_NAME);
            client().admin().indices().execute(WarmToHotTierAction.INSTANCE, hotRequest).actionGet();

            // Wait for migration to start
            assertTrue("Warm to Hot migration didn't start", w2hMigrationStarted.await(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Delete index during migration
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();

            // Verify index is deleted
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                assertFalse("Index should be deleted", state.metadata().hasIndex(INDEX_NAME));
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        } finally {
            clusterService().removeListener(relocationListener);
        }
    }

    public void testMultipleIndicesWarmToHotMigration() throws Exception {
        // Setup cluster
        setupCluster(1);  // Using fixed replica count for multiple indices

        // Create multiple indices
        String[] indices = new String[] { "test-1", "test-2", "test-3", "test-4" };
        for (String index : indices) {
            createTestIndex(index, 1, 1);  // Using fixed shard and replica count
        }
        ensureGreen(indices);

        // First migrate all indices to warm
        CountDownLatch h2wLatch = new CountDownLatch(indices.length);
        List<ClusterStateListener> h2wListeners = new ArrayList<>();

        // Start H2W migration for all indices
        for (String index : indices) {
            final IndexTieringRequest request = new IndexTieringRequest("warm", index);
            client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet();

            ClusterStateListener listener = new TieringTestUtils.MockTieringCompletionListener(
                h2wLatch,
                index,
                IndexModule.TieringState.HOT_TO_WARM.toString()
            );
            h2wListeners.add(listener);
            clusterService().addListener(listener);
        }

        // Wait for all H2W migrations to complete
        assertTrue("Hot to Warm migration timed out", h2wLatch.await(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        // Verify all indices moved to warm nodes
        assertBusy(() -> {
            for (String index : indices) {
                assertIndexShardLocation(index, true);
                verifyWarmIndexSettings(index);
            }
        }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Clean up H2W listeners
        h2wListeners.forEach(listener -> clusterService().removeListener(listener));

        // Now migrate all indices back to hot
        CountDownLatch w2hLatch = new CountDownLatch(indices.length);
        List<ClusterStateListener> w2hListeners = new ArrayList<>();

        // Start W2H migration for all indices
        for (String index : indices) {
            final IndexTieringRequest request = new IndexTieringRequest("hot", index);
            client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();

            ClusterStateListener listener = new TieringTestUtils.MockTieringCompletionListener(
                w2hLatch,
                index,
                IndexModule.TieringState.WARM_TO_HOT.toString()
            );
            w2hListeners.add(listener);
            clusterService().addListener(listener);
        }

        // Wait for all W2H migrations to complete
        assertTrue("Warm to Hot migration timed out", w2hLatch.await(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        // Verify all indices moved back to hot nodes
        assertBusy(() -> {
            for (String index : indices) {
                assertIndexShardLocation(index, false);
                verifyHotIndexSettings(index);
            }
        }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Clean up W2H listeners
        w2hListeners.forEach(listener -> clusterService().removeListener(listener));

        // Cleanup indices
        for (String index : indices) {
            client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
        }
    }

    protected void verifyMigrationError(String indexName, String targetTier, String expectedErrorMessage) {
        final IndexTieringRequest request = new IndexTieringRequest(targetTier, indexName);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    public void testHotMigrationForAlreadyMigratingIndex() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);
        ensureGreen(INDEX_NAME);
        migrateToWarm(INDEX_NAME);

        // Start initial hot migration
        final IndexTieringRequest hotRequest = new IndexTieringRequest("hot", INDEX_NAME);
        AcknowledgedResponse hotResponse = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, hotRequest).actionGet();
        assertTrue(hotResponse.isAcknowledged());

        // Verify no error when trying concurrent migration
        migrateToHot(INDEX_NAME);

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotMigrationForAlreadyHotIndex() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);
        migrateToWarm(INDEX_NAME);
        migrateToHot(INDEX_NAME);

        // Verify error when trying to migrate hot index to hot
        verifyMigrationError(INDEX_NAME, "hot", "Cannot migrate index [" + INDEX_NAME + "] to HOT tier");

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testConcurrentWarmToHotMigrationWithHotNodesRestarts() throws Exception {
        logger.info("--> Starting warm to hot migration test with hot node restarts during relocation");

        // Setup cluster with normal allocation settings (don't disable initially)
        setupCluster(1);
        final String TEST_INDEX = "test-index";

        // Create index and migrate it to warm first
        createTestIndex(TEST_INDEX, 1, 1);
        ensureGreen(TEST_INDEX);

        Map<String, Long> indexStats = indexData(1, false, TEST_INDEX);
        long originalDocCount = indexStats.get(TOTAL_OPERATIONS);

        // First migrate to warm
        migrateToWarm(TEST_INDEX);

        try {
            // Now disable allocation to control the timing
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
                        .put("cluster.routing.allocation.node_concurrent_incoming_recoveries", 1)
                        .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 1)
                        .build()
                )
                .get();

            // Trigger warm to hot migration
            logger.info("--> Starting warm to hot migration for index: {}", TEST_INDEX);
            final IndexTieringRequest request = new IndexTieringRequest("hot", TEST_INDEX);
            AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            // Wait for relocation to start
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shardRouting = state.routingTable().index(TEST_INDEX).shard(0).primaryShard();
                assertTrue("Shard should be relocating", shardRouting.relocating());

                // Get target node ID
                String targetNodeId = shardRouting.relocatingNodeId();
                DiscoveryNode targetNode = state.nodes().get(targetNodeId);
                assertTrue("Target node should be a hot node", targetNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE));

                logger.info("--> Restarting hot node {} during relocation", targetNode.getName());
                internalCluster().restartNode(targetNode.getName());
            }, 30, TimeUnit.SECONDS);

            // Enable full allocation to allow migration to complete
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put("cluster.routing.allocation.node_concurrent_recoveries", 2)
                        .put("cluster.routing.allocation.node_concurrent_incoming_recoveries", 2)
                        .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 2)
                        .build()
                )
                .get();

            // Verify migration eventually completes
            assertBusy(() -> {
                ensureGreen(TEST_INDEX);
                assertIndexShardLocation(TEST_INDEX, false); // Should be on hot nodes
                verifyHotIndexSettings(TEST_INDEX);
            }, 60, TimeUnit.SECONDS);

            // Verify data consistency
            refresh(TEST_INDEX);
            assertHitCount(client().prepareSearch(TEST_INDEX).setSize(0).get(), originalDocCount);

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(TEST_INDEX)).actionGet();
        }
    }

    public void testConcurrentWarmToHotMigrationWithHotNodesFailure() throws Exception {
        logger.info("--> Starting warm to hot migration test with hot node failed during relocation");

        // Setup cluster with normal allocation settings (don't disable initially)
        setupCluster(1);
        final String TEST_INDEX = "test-index";

        // Create index and migrate it to warm first
        createTestIndex(TEST_INDEX, 1, 1);
        ensureGreen(TEST_INDEX);

        Map<String, Long> indexStats = indexData(1, false, TEST_INDEX);
        long originalDocCount = indexStats.get(TOTAL_OPERATIONS);

        // First migrate to warm
        migrateToWarm(TEST_INDEX);

        try {
            // Now disable allocation to control the timing
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(1)).get();

            // Trigger warm to hot migration
            logger.info("--> Starting warm to hot migration for index: {}", TEST_INDEX);
            final IndexTieringRequest request = new IndexTieringRequest("hot", TEST_INDEX);
            AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            // Wait for relocation to start
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shardRouting = state.routingTable().index(TEST_INDEX).shard(0).primaryShard();
                assertTrue("Shard should be relocating", shardRouting.relocating());

                // Get target node ID
                String targetNodeId = shardRouting.relocatingNodeId();
                DiscoveryNode targetNode = state.nodes().get(targetNodeId);
                assertTrue("Target node should be a hot node", targetNode.getRoles().contains(DiscoveryNodeRole.DATA_ROLE));

                logger.info("--> Stop hot node {} during relocation", targetNode.getName());
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(targetNode.getName()));
            }, 30, TimeUnit.SECONDS);

            // Enable full allocation to allow migration to complete
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            internalCluster().startDataOnlyNode();

            // Verify migration eventually completes
            assertBusy(() -> {
                ensureGreen(TEST_INDEX);
                assertIndexShardLocation(TEST_INDEX, false); // Should be on hot nodes
                verifyHotIndexSettings(TEST_INDEX);
            }, 60, TimeUnit.SECONDS);

            // Verify data consistency
            refresh(TEST_INDEX);
            assertHitCount(client().prepareSearch(TEST_INDEX).setSize(0).get(), originalDocCount);

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(TEST_INDEX)).actionGet();
        }
    }

    public void testConcurrentWarmToHotMigrationWithAllHotNodesRestarts() throws Exception {
        logger.info("--> Starting warm to hot migration test with all hot nodes restart during relocation");

        // Setup cluster with controlled allocation
        setupCluster(2);  // Start with 2 hot nodes
        final String TEST_INDEX = "test-index";

        // Create index and migrate it to warm first
        createTestIndex(TEST_INDEX, 1, 1);
        ensureGreen(TEST_INDEX);

        Map<String, Long> indexStats = indexData(1, false, TEST_INDEX);
        long originalDocCount = indexStats.get(TOTAL_OPERATIONS);

        // First migrate to warm
        migrateToWarm(TEST_INDEX);

        try {
            // Control allocation speed
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(1)).get();

            // Trigger warm to hot migration
            logger.info("--> Starting warm to hot migration for index: {}", TEST_INDEX);
            final IndexTieringRequest request = new IndexTieringRequest("hot", TEST_INDEX);
            AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            // Wait for relocation to start and collect hot nodes
            List<String> hotNodeNames = new ArrayList<>();
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shardRouting = state.routingTable().index(TEST_INDEX).shard(0).primaryShard();
                assertTrue("Shard should be relocating", shardRouting.relocating());

                // Collect all hot nodes
                state.nodes()
                    .getNodes()
                    .values()
                    .stream()
                    .filter(node -> node.getRoles().contains(DiscoveryNodeRole.DATA_ROLE))
                    .forEach(node -> hotNodeNames.add(node.getName()));

                assertTrue("Should have at least one hot node", !hotNodeNames.isEmpty());
            }, 30, TimeUnit.SECONDS);

            // Restart all hot nodes
            logger.info("--> Restarting all hot nodes during relocation: {}", hotNodeNames);
            for (String nodeName : hotNodeNames) {
                logger.info("--> Restarting hot node: {}", nodeName);
                internalCluster().restartNode(nodeName, new InternalTestCluster.RestartCallback());
            }

            // Enable full allocation to allow migration to complete
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            // Verify migration eventually completes
            assertBusy(() -> {
                ensureGreen(TEST_INDEX);
                assertIndexShardLocation(TEST_INDEX, false); // Should be on hot nodes
                verifyHotIndexSettings(TEST_INDEX);
            }, 60, TimeUnit.SECONDS);

            // Verify data consistency
            refresh(TEST_INDEX);
            assertBusy(() -> {
                SearchResponse searchResponse = client().prepareSearch(TEST_INDEX)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize((int) originalDocCount)
                    .get();
                assertEquals("Document count should match original", originalDocCount, searchResponse.getHits().getTotalHits().value());
            }, 30, TimeUnit.SECONDS);

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(TEST_INDEX)).actionGet();
        }
    }

    public void testWarmToHotMigrationWithMasterNodeFailure() throws Exception {
        logger.info("--> Starting test for warm-to-hot migration with master node failed");

        // Start cluster nodes with 3 master nodes for quorum
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().startWarmOnlyNodes(2);
        interceptCheckpointUpdates();

        final String[] indices = new String[] { "test-1", "test-2", "test-3" };
        Map<String, Long> indexDocCounts = new HashMap<>();

        try {
            // Create indices and add data
            for (String index : indices) {
                createTestIndex(index, 1, 1);
                Map<String, Long> stats = indexData(1, false, index);
                indexDocCounts.put(index, stats.get(TOTAL_OPERATIONS));
                logger.info("--> Created index {} with {} documents", index, stats.get(TOTAL_OPERATIONS));
            }
            ensureGreen(indices);

            // Move all indices to warm tier first
            for (String index : indices) {
                migrateToWarm(index);
            }

            refresh(indices);

            // disable allocation to block migrations.
            logger.info("--> Enabling allocation to allow migrations to complete");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Start migration back to hot for all indices
            logger.info("--> Starting warm to hot migration for all indices");
            for (String index : indices) {
                final IndexTieringRequest request = new IndexTieringRequest("hot", index);
                AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
                assertTrue(response.isAcknowledged());
            }

            // Sleep to ensure migration requests are processed
            TieringTestUtils.safeSleep(5000);

            // Verify migrations are stuck due to disabled allocation
            for (String index : indices) {
                assertBusy(() -> {
                    ClusterState state = client().admin().cluster().prepareState().get().getState();
                    ShardRouting shardRouting = state.routingTable().index(index).shard(0).primaryShard();
                    assertFalse("Shard should not be relocating", shardRouting.relocating());
                    assertIndexShardLocation(index, true);  // Should still be on warm nodes
                }, 30, TimeUnit.SECONDS);
            }

            // Stop current master node
            logger.info("--> Stopping current master node");
            internalCluster().stopCurrentClusterManagerNode();

            // Wait for cluster to stabilize with new master
            ensureStableCluster(6); // Remaining nodes (2 master + 2 data + 2 warm)
            logger.info("--> Cluster stabilized after master node failure");

            // Enable allocation to allow migrations to proceed
            logger.info("--> Enabling allocation to allow migrations to complete");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            // Verify migrations complete on remaining master
            assertBusy(() -> {
                for (String index : indices) {
                    assertIndexShardLocation(index, false);  // Should now be on hot nodes
                    verifyHotIndexSettings(index);
                }
            }, 60, TimeUnit.SECONDS);

            // Verify data consistency
            for (String index : indices) {
                refresh(index);
                long expectedCount = indexDocCounts.get(index);
                flush(index);
                long actualCount = client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value();

                logger.info("--> Index {} document count - expected: {}, actual: {}", index, expectedCount, actualCount);
                assertEquals("Document count mismatch for index " + index, expectedCount, actualCount);
            }

        } finally {
            // Cleanup
            for (String index : indices) {
                client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            }
        }
    }

    public void testBothTypeMigrationWithMasterFailure() throws Exception {
        logger.info("--> Starting test for both migration with master node failure");

        // Start cluster nodes
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);  // hot nodes
        internalCluster().startWarmOnlyNodes(2);  // warm nodes
        interceptCheckpointUpdates();

        final String[] hotToWarmIndices = new String[] { "hot-to-warm-1", "hot-to-warm-2" };
        final String[] warmToHotIndices = new String[] { "warm-to-hot-1", "warm-to-hot-2" };
        Map<String, Long> indexDocCounts = new HashMap<>();

        try {
            // Create and populate all indices
            for (String index : hotToWarmIndices) {
                createTestIndex(index, 1, 1);
                Map<String, Long> stats = indexData(1, false, index);
                indexDocCounts.put(index, stats.get(TOTAL_OPERATIONS));
            }

            for (String index : warmToHotIndices) {
                createTestIndex(index, 1, 1);
                Map<String, Long> stats = indexData(1, false, index);
                indexDocCounts.put(index, stats.get(TOTAL_OPERATIONS));
                // Move these indices to warm initially
                migrateToWarm(index);
            }

            ensureGreen(hotToWarmIndices);
            ensureGreen(warmToHotIndices);

            // Disable allocation to block migrations
            logger.info("--> Disabling allocation to block migrations");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Initiate both hot-to-warm and warm-to-hot migrations
            logger.info("--> Starting bidirectional migrations");
            for (String index : hotToWarmIndices) {
                final IndexTieringRequest request = new IndexTieringRequest("warm", index);
                AcknowledgedResponse response = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet();
                assertTrue(response.isAcknowledged());
            }

            for (String index : warmToHotIndices) {
                final IndexTieringRequest request = new IndexTieringRequest("hot", index);
                AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
                assertTrue(response.isAcknowledged());
            }

            // Verify migrations are stuck
            TieringTestUtils.safeSleep(5000);
            for (String index : Stream.concat(Arrays.stream(hotToWarmIndices), Arrays.stream(warmToHotIndices)).toArray(String[]::new)) {
                assertBusy(() -> {
                    ClusterState state = client().admin().cluster().prepareState().get().getState();
                    ShardRouting shardRouting = state.routingTable().index(index).shard(0).primaryShard();
                    assertFalse("Shard should not be relocating", shardRouting.relocating());
                }, 30, TimeUnit.SECONDS);
            }

            // Enable allocation to allow migrations to proceed
            logger.info("--> Enabling allocation to allow migrations to complete");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            // Stop current master node
            logger.info("--> Stopping current master node");
            internalCluster().stopCurrentClusterManagerNode();

            // Wait for cluster to stabilize with new master
            ensureStableCluster(6); // Remaining nodes
            logger.info("--> Cluster stabilized after master node failure");

            // Verify migrations complete correctly
            assertBusy(() -> {
                // Verify hot-to-warm indices are now on warm nodes
                for (String index : hotToWarmIndices) {
                    assertIndexShardLocation(index, true); // Should be on warm nodes
                    verifyWarmIndexSettings(index);
                }

                // Verify warm-to-hot indices are now on hot nodes
                for (String index : warmToHotIndices) {
                    assertIndexShardLocation(index, false); // Should be on hot nodes
                    verifyHotIndexSettings(index);
                }
            }, 60, TimeUnit.SECONDS);

            // Verify data consistency
            for (String index : Stream.concat(Arrays.stream(hotToWarmIndices), Arrays.stream(warmToHotIndices)).toArray(String[]::new)) {
                refresh(index);
                long expectedCount = indexDocCounts.get(index);
                flush(index);
                long actualCount = client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value();

                logger.info("--> Index {} document count - expected: {}, actual: {}", index, expectedCount, actualCount);
                assertEquals("Document count mismatch for index " + index, expectedCount, actualCount);
            }

        } finally {
            // Cleanup
            for (String index : Stream.concat(Arrays.stream(hotToWarmIndices), Arrays.stream(warmToHotIndices)).toArray(String[]::new)) {
                client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            }
        }
    }

    public void testHotMigrationWithNoNodesCapacity() throws Exception {

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);
        // refreshClusterInfo();
        migrateToWarm(INDEX_NAME);

        MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 3000000L);

        // Verify error when trying concurrent migration
        verifyMigrationError(INDEX_NAME, "hot", "since we don't have enough [" + "HOT" + "] capacity. Please add more");

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotMigrationWithNoNodesCapacity_YellowIndex() throws Exception {
        // Start minimal cluster - one master, one hot node, one warm node
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        String warmNode = internalCluster().startWarmOnlyNodes(2).get(0);
        interceptCheckpointUpdates();

        final String INDEX_NAME = "test-index";
        Map<String, Long> indexDocCounts = new HashMap<>();

        try {
            // Create index with 1 primary and 1 replica on warm node
            createTestIndex(INDEX_NAME, 1, 1);
            ensureYellow(INDEX_NAME);
            Map<String, Long> stats = indexData(1, false, INDEX_NAME);
            indexDocCounts.put(INDEX_NAME, stats.get(TOTAL_OPERATIONS));
            logger.info("--> Created index {} with {} documents and 1 replica", INDEX_NAME, stats.get(TOTAL_OPERATIONS));

            // Move to warm tier first
            migrateToWarm(INDEX_NAME);

            ensureGreen(INDEX_NAME);

            // Drop any warm node and verify index is in yellow state now.
            internalCluster().stopRandomWarmNode();
            ensureYellow(INDEX_NAME);

            MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
            clusterInfoService.setDiskUsageFunctionAndRefresh(
                (discoveryNode, fsInfoPath) -> setDiskUsage(
                    fsInfoPath,
                    new ByteSizeValue(200, ByteSizeUnit.BYTES).getBytes(),
                    new ByteSizeValue(500, ByteSizeUnit.BYTES).getBytes()
                )
            );
            clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 500L);

            // Although there is only 1 node to accomodate the largest shard, validation should fail as the Index is Yellow
            verifyMigrationError(INDEX_NAME, "HOT", "");

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testHotMigrationWithNoSpaceForLargestShard() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);
        migrateToWarm(INDEX_NAME);

        MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(
                fsInfoPath,
                new ByteSizeValue(600, ByteSizeUnit.BYTES).getBytes(),
                new ByteSizeValue(1000, ByteSizeUnit.BYTES).getBytes()
            )
        );
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 900L);

        // Verify error when trying concurrent migration
        verifyMigrationError(INDEX_NAME, "hot", "since we don't have [HOT] node with free space to fit it's largest shard");

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotMigrationWithNoSpaceForLargestShard_YellowIndex() throws Exception {
        // Start minimal cluster - one master, one hot node, one warm node
        internalCluster().startClusterManagerOnlyNode();
        String hotNodeId = internalCluster().startDataOnlyNodes(2).get(0);
        String warmNode = internalCluster().startWarmOnlyNodes(2).get(0);
        interceptCheckpointUpdates();

        final String INDEX_NAME = "test-index";
        Map<String, Long> indexDocCounts = new HashMap<>();

        try {
            // Create index with 1 primary and 1 replica on warm node
            createTestIndex(INDEX_NAME, 1, 1);
            Map<String, Long> stats = indexData(1, false, INDEX_NAME);
            indexDocCounts.put(INDEX_NAME, stats.get(TOTAL_OPERATIONS));
            logger.info("--> Created index {} with {} documents and 1 replica", INDEX_NAME, stats.get(TOTAL_OPERATIONS));

            // Move to warm tier first
            migrateToWarm(INDEX_NAME);

            ensureGreen(INDEX_NAME);

            // Drop any warm node and verify index is in yellow state now.
            internalCluster().stopRandomWarmNode();
            ensureYellow(INDEX_NAME);

            MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
            clusterInfoService.setDiskUsageFunctionAndRefresh(
                (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 1000, discoveryNode.getId().equals(hotNodeId) ? 1000 : 200)
            );

            clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 900L);

            // Although there is only 1 node to accomodate the largest shard, validation should fail as the Index has 1 replica
            verifyMigrationError(INDEX_NAME, "HOT", "since we don't have enough [HOT] capacity");

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testHotMigrationWithTieringQueueSizeBreached() throws Exception {
        setupCluster(1);

        createTestIndex(INDEX_NAME, 1, 1);
        createTestIndex("test-index-2", 1, 1);
        createTestIndex("test-index-3", 1, 1);

        migrateToWarm(INDEX_NAME);
        migrateToWarm("test-index-2");
        migrateToWarm("test-index-3");

        // Disable allocation to block migrations
        logger.info("--> Disabling allocation to block migrations");

        Settings.Builder settingsBuilder = Settings.builder()
            .put(buildDisabledAllocationSettings())
            .put(W2H_MAX_CONCURRENT_TIERING_REQUESTS_KEY, 2);

        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder).get();

        List<String> inFlightTieringIndices = List.of(INDEX_NAME, "test-index-2");
        for (String index : inFlightTieringIndices) {
            final IndexTieringRequest stuckRequest = new IndexTieringRequest("hot", index);
            AcknowledgedResponse stuckResponse = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, stuckRequest).actionGet();
            assertTrue(stuckResponse.isAcknowledged());
            TieringTestUtils.safeSleep(4000);

            // Verify index hasn't started relocation
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shardRouting = state.routingTable().index(index).shard(0).primaryShard();
                assertFalse("Shard should not be relocating", shardRouting.relocating());
                assertIndexShardLocation(index, true);
            }, 30, TimeUnit.SECONDS);

        }
        IndexTieringRequest tieringRequest = new IndexTieringRequest("WARM", "test-index-3");
        // Try migrating again and verify error
        assertThrows(
            OpenSearchRejectedExecutionException.class,
            () -> client().admin().indices().execute(WarmToHotTierAction.INSTANCE, tieringRequest).actionGet()
        );

        // Cleanup
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotMigrationWithoutClusterInfoMockForJVMUtilization() throws Exception {

        // ResourceUsageCollector requires atleast below stats to be ready within a window to capture stats
        Settings.Builder settingsBuilder = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
            .put(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(5000))
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500));

        setupCluster(1, settingsBuilder.build());
        createTestIndex(INDEX_NAME, 1, 1);

        // Adding a delay to let the JVM and CPU stats get collected
        TieringTestUtils.safeSleep(6000);
        // Try migrating again and verify error
        migrateToWarm(INDEX_NAME);
        migrateToHot(INDEX_NAME);

        // Cleanup
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testWarmMigrationWithAllNodesJVMUtilizationBreached() throws Exception {
        setupCluster(1);

        createTestIndex(INDEX_NAME, 1, 1);

        MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 230L);
        migrateToWarm(INDEX_NAME);

        clusterInfoService.setNodeResourceUsageFunctionAndRefresh(
            (nodeId -> new NodeResourceUsageStats(nodeId, System.currentTimeMillis(), 99, 20, null))
        );

        // Try migrating again and verify error
        verifyMigrationError(INDEX_NAME, "HOT", "Rejecting tiering request as JVM Utilization is high on all [HOT] nodes");

        // Cleanup
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testWarmToHotMigrationWithUnassignedReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        internalCluster().startWarmOnlyNodes(2);
        interceptCheckpointUpdates();

        try {
            createTestIndex(INDEX_NAME, 1, 2);
            indexBulk(INDEX_NAME, 100);
            ensureYellow(INDEX_NAME);

            migrateToWarm(INDEX_NAME);

            internalCluster().stopRandomWarmNode();
            ensureYellow(INDEX_NAME);

            final IndexTieringRequest hotRequest = new IndexTieringRequest("hot", INDEX_NAME);
            AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, hotRequest).actionGet();
            assertTrue(response.isAcknowledged());

            assertBusy(() -> {
                assertIndexShardLocation(INDEX_NAME, false);
                verifyHotIndexSettings(INDEX_NAME);
                ensureYellow(INDEX_NAME);
            }, 30, TimeUnit.SECONDS);

            refresh(INDEX_NAME);
            assertEquals(100, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
            flush(INDEX_NAME);
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    protected MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
    }

    protected void refreshClusterInfo() {
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalCluster().getInstance(
            ClusterInfoService.class,
            internalCluster().getClusterManagerName()
        );
        infoService.refresh();
    }

    protected static FsInfo.Path setDiskUsage(FsInfo.Path original, long totalBytes, long freeBytes) {
        return new FsInfo.Path(original.getPath(), original.getMount(), totalBytes, freeBytes, freeBytes);
    }

}
