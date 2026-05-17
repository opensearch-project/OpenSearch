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
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
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
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.storage.tiering.TieringTestUtils.buildDisabledAllocationSettings;
import static org.opensearch.storage.tiering.TieringTestUtils.buildEnabledAllocationSettings;
import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class HotToWarmTieringServiceIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "h2w-test-idx-1";
    protected static final String INDEX_NAME_2 = "h2w-test-idx-2";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int MIGRATION_TIMEOUT_SECONDS = 30;
    private static final long TOTAL_SPACE_BYTES = new ByteSizeValue(1000, ByteSizeUnit.KB).getBytes();

    protected static final Map<String, Class<? extends Plugin>> TEST_SPECIFIC_CUSTOM_CLUSTER_INFO = Map.of(
        "testWarmMigrationWithAllNodesJVMUtilizationBreached",
        MockInternalClusterInfoService.TestPlugin.class,
        "testWarmMigrationWithAllNodesWaterMarkBreachedWithExistingTieredIndices",
        MockInternalClusterInfoService.TestPlugin.class,
        "testWarmMigrationWithAllNodesWaterMarkBreached",
        MockInternalClusterInfoService.TestPlugin.class
    );

    protected static final Map<String, ByteSizeValue> TEST_SPECIFIC_CACHE_SIZES = new HashMap<>();
    static {
        TEST_SPECIFIC_CACHE_SIZES.put("testWarmMigrationWithLargeTieringIndicesQueue", new ByteSizeValue(300, ByteSizeUnit.BYTES));
        TEST_SPECIFIC_CACHE_SIZES.put("testWarmMigrationWithAllNodesWaterMarkBreached", new ByteSizeValue(100, ByteSizeUnit.BYTES));
        TEST_SPECIFIC_CACHE_SIZES.put(
            "testWarmMigrationWithAllNodesWaterMarkBreachedWithExistingTieredIndices",
            new ByteSizeValue(1000, ByteSizeUnit.BYTES)
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
        String testName = getTestName();
        ByteSizeValue cacheSize = TEST_SPECIFIC_CACHE_SIZES.getOrDefault(testName, new ByteSizeValue(1, ByteSizeUnit.GB));
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true).build();
    }

    protected void setupCluster(int numberOfReplicas) {
        setupCluster(numberOfReplicas, null);
    }

    protected void setupCluster(int numberOfReplicas, Settings clusterSettings) {
        setupCluster(numberOfReplicas, null, null, clusterSettings);
    }

    protected void setupCluster(int numberOfReplicas, String lowDiskWaterMark, String dataToFileCacheRatio) {
        setupCluster(numberOfReplicas, lowDiskWaterMark, dataToFileCacheRatio, null);
    }

    protected void setupCluster(int numberOfReplicas, String lowDiskWaterMark, String dataToFileCacheRatio, Settings clusterSettings) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), lowDiskWaterMark != null ? lowDiskWaterMark : "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), dataToFileCacheRatio != null ? Long.parseLong(dataToFileCacheRatio) : 5.0)
            .put(clusterSettings == null ? Settings.EMPTY : clusterSettings);

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(numberOfReplicas + 1, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        interceptCheckpointUpdates();
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
        refreshClusterInfo();
    }

    protected void migrateToWarm(String indexName) throws Exception {
        // Ensure segment replication is fully caught up before triggering relocation
        if (indexExists(indexName)) {
            flush(indexName);
            waitForReplication(indexName);
        }

        CountDownLatch latch = new CountDownLatch(1);
        ClusterStateListener listener = new TieringTestUtils.MockTieringCompletionListener(
            latch,
            indexName,
            IndexModule.TieringState.HOT_TO_WARM.toString()
        );

        try {
            clusterService().addListener(listener);
            final IndexTieringRequest request = new IndexTieringRequest("warm", indexName);
            AcknowledgedResponse response = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertTrue("Hot to Warm migration timed out", latch.await(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertBusy(() -> {
                assertIndexShardLocation(indexName, true);
                verifyWarmIndexSettings(indexName);
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            clusterService().removeListener(listener);
        }
    }

    protected static void assertIndexShardLocation(String indexName, boolean onWarmNodes) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);

        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            ShardRouting shardRouting = shardRoutingTable.primaryShard();
            if (shardRouting.unassigned() && shardRouting.primary() == false) {
                continue;
            }
            String assignedNodeId = shardRouting.currentNodeId();
            DiscoveryNode assignedNode = state.nodes().get(assignedNodeId);
            if (onWarmNodes) {
                assertTrue("Shard should be on warm node", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            } else {
                assertFalse("Shard should not be on warm node", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
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

    protected void migrateMultipleIndicesToWarm(String[] indices) throws Exception {
        // Ensure segment replication is fully caught up before triggering relocation
        for (String index : indices) {
            flush(index);
            waitForReplication(index);
        }

        CountDownLatch migrationLatch = new CountDownLatch(indices.length);
        List<ClusterStateListener> listeners = new ArrayList<>();

        for (String index : indices) {
            flush(index);
            waitForReplication(index);
            client().admin().indices().execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", index)).actionGet();
            ClusterStateListener listener = new TieringTestUtils.MockTieringCompletionListener(
                migrationLatch,
                index,
                IndexModule.TieringState.HOT_TO_WARM.toString()
            );
            listeners.add(listener);
            clusterService().addListener(listener);
        }

        try {
            assertTrue("Migration timed out", migrationLatch.await(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertBusy(() -> {
                for (String index : indices) {
                    assertIndexShardLocation(index, true);
                    verifyWarmIndexSettings(index);
                }
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            listeners.forEach(l -> clusterService().removeListener(l));
        }
    }

    protected CountDownLatch waitForMigrationToStart(String indexName) {
        CountDownLatch migrationStarted = new CountDownLatch(1);
        clusterService().addListener(event -> {
            if (event.state().routingTable().hasIndex(indexName)) {
                boolean isRelocating = event.state().routingTable().allShards(indexName).stream().anyMatch(ShardRouting::relocating);
                if (isRelocating) migrationStarted.countDown();
            }
        });
        return migrationStarted;
    }

    protected void verifyMigrationError(String indexName, String expectedErrorMessage) {
        final IndexTieringRequest request = new IndexTieringRequest("warm", indexName);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    protected void refreshClusterInfo() {
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalCluster().getInstance(
            ClusterInfoService.class,
            internalCluster().getClusterManagerName()
        );
        infoService.refresh();
    }

    public void testHotToWarmMigrationBasic() throws Exception {
        int numberOfReplicas = randomIntBetween(0, 1);
        int numberOfShards = randomIntBetween(1, 2);

        setupCluster(numberOfReplicas);
        createTestIndex(INDEX_NAME, numberOfShards, numberOfReplicas);
        assertIndexShardLocation(INDEX_NAME, false);

        migrateToWarm(INDEX_NAME);

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotToWarmMigrationWithData() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        indexBulk(INDEX_NAME, 100);
        refresh(INDEX_NAME);

        assertIndexShardLocation(INDEX_NAME, false);
        migrateToWarm(INDEX_NAME);

        refresh(INDEX_NAME);
        assertEquals(100, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotToWarmMigrationWithDeletedDocumentSuccess() throws Exception {
        int numberOfReplicas = randomIntBetween(1, 2);
        logger.info("Testing migration with deletions, replicas: {}", numberOfReplicas);

        setupCluster(numberOfReplicas, null, null);
        createTestIndex(INDEX_NAME, 1, numberOfReplicas);

        Map<String, Long> indexStats = indexData(randomIntBetween(1, 3), true, INDEX_NAME);
        long initialDocCount = indexStats.get(TOTAL_OPERATIONS);

        List<String> deletedIds = deleteRandomDocuments(INDEX_NAME, 2);
        long expectedDocsAfterDelete = initialDocCount - deletedIds.size();
        refresh(INDEX_NAME);
        flush(INDEX_NAME);

        assertIndexShardLocation(INDEX_NAME, false);

        migrateToWarm(INDEX_NAME);

        refresh(INDEX_NAME);
        flush(INDEX_NAME);
        assertEquals(
            "Document count changed after migration",
            expectedDocsAfterDelete,
            client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value()
        );

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    private List<String> deleteRandomDocuments(String indexName, int count) {
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(count).get();

        List<String> deletedIds = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits()) {
            String docId = hit.getId();
            deletedIds.add(docId);
            assertEquals(DocWriteResponse.Result.DELETED, client().prepareDelete(indexName, docId).get().getResult());
        }
        return deletedIds;
    }

    public void testWarmMigrationForAlreadyMigratedIndex() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        migrateToWarm(INDEX_NAME);

        verifyMigrationError(INDEX_NAME, "Cannot migrate index [" + INDEX_NAME + "] to WARM tier");

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testMultipleIndicesHotToWarmMigration() throws Exception {
        setupCluster(1);

        String[] indices = new String[] { "test-1", "test-2", "test-3" };
        for (String index : indices) {
            createTestIndex(index, 1, 1);
        }
        ensureGreen(indices);

        for (String index : indices) {
            assertIndexShardLocation(index, false);
        }

        // Ensure segment replication is fully caught up before triggering relocation
        for (String index : indices) {
            flush(index);
            waitForReplication(index);
        }

        CountDownLatch migrationLatch = new CountDownLatch(indices.length);
        List<ClusterStateListener> listeners = new ArrayList<>();

        for (String index : indices) {
            flush(index);
            waitForReplication(index);
            client().admin().indices().execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", index)).actionGet();
            ClusterStateListener listener = new TieringTestUtils.MockTieringCompletionListener(
                migrationLatch,
                index,
                IndexModule.TieringState.HOT_TO_WARM.toString()
            );
            listeners.add(listener);
            clusterService().addListener(listener);
        }

        try {
            assertTrue("Migration timed out", migrationLatch.await(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertBusy(() -> {
                for (String index : indices) {
                    assertIndexShardLocation(index, true);
                    verifyWarmIndexSettings(index);
                }
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            listeners.forEach(l -> clusterService().removeListener(l));
            for (String index : indices) {
                client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            }
        }
    }

    public void testHotToWarmMigrationWithIndexDeletion() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);
        assertIndexShardLocation(INDEX_NAME, false);

        CountDownLatch migrationStarted = new CountDownLatch(1);
        ClusterStateListener relocationListener = event -> {
            if (event.state().routingTable().hasIndex(INDEX_NAME)) {
                boolean isRelocating = event.state().routingTable().allShards(INDEX_NAME).stream().anyMatch(ShardRouting::relocating);
                if (isRelocating) {
                    migrationStarted.countDown();
                }
            }
        };
        clusterService().addListener(relocationListener);

        // Ensure segment replication is fully caught up before triggering relocation
        flush(INDEX_NAME);
        waitForReplication(INDEX_NAME);

        client().admin().indices().execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", INDEX_NAME)).actionGet();
        assertTrue("Migration didn't start", migrationStarted.await(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertFalse("Index should be deleted", state.metadata().hasIndex(INDEX_NAME));
        }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        clusterService().removeListener(relocationListener);
    }

    public void testWarmMigrationReplicaCountFrom2To1() throws Exception {
        setupCluster(2);
        createTestIndex("test-replica", 1, 2);

        assertEquals(
            "2",
            client().admin()
                .indices()
                .prepareGetSettings("test-replica")
                .get()
                .getSetting("test-replica", IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );

        migrateToWarm("test-replica");

        assertEquals(
            "1",
            client().admin()
                .indices()
                .prepareGetSettings("test-replica")
                .get()
                .getSetting("test-replica", IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );

        client().admin().indices().delete(new DeleteIndexRequest("test-replica")).actionGet();
    }

    public void testConcurrentMigrationWithStuckIndex() throws Exception {
        Settings disabledSettings = buildDisabledAllocationSettings();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .put(disabledSettings);

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(2, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        interceptCheckpointUpdates();

        createTestIndex("stuck-index", 1, 1);
        createTestIndex("second-index", 1, 1);
        ensureGreen("stuck-index", "second-index");

        try {
            flush("stuck-index");
            waitForReplication("stuck-index");
            AcknowledgedResponse resp = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", "stuck-index"))
                .actionGet();
            assertTrue(resp.isAcknowledged());
            TieringTestUtils.safeSleep(5000);

            assertBusy(() -> { assertIndexShardLocation("stuck-index", false); }, 30, TimeUnit.SECONDS);

            flush("second-index");
            waitForReplication("second-index");
            client().admin().indices().execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", "second-index")).actionGet();

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            assertBusy(() -> {
                assertIndexShardLocation("stuck-index", true);
                assertIndexShardLocation("second-index", true);
                verifyWarmIndexSettings("stuck-index");
                verifyWarmIndexSettings("second-index");
            }, 30, TimeUnit.SECONDS);
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest("stuck-index")).actionGet();
            client().admin().indices().delete(new DeleteIndexRequest("second-index")).actionGet();
        }
    }

    public void testWarmMigrationForExistingMigratingIndex() throws Exception {
        setupCluster(1, buildDisabledAllocationSettings());
        createTestIndex(INDEX_NAME, 1, 1);
        ensureGreen(INDEX_NAME);

        try {
            final IndexTieringRequest stuckRequest = new IndexTieringRequest("warm", INDEX_NAME);
            AcknowledgedResponse stuckResponse = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, stuckRequest).actionGet();
            assertTrue(stuckResponse.isAcknowledged());

            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shardRouting = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
                assertFalse("Shard should not be relocating", shardRouting.relocating());
                assertIndexShardLocation(INDEX_NAME, false);
            }, 30, TimeUnit.SECONDS);

            // Trigger migration again — should get ack (idempotent)
            final IndexTieringRequest secondRequest = new IndexTieringRequest("warm", INDEX_NAME);
            AcknowledgedResponse secondResponse = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, secondRequest)
                .actionGet();
            assertTrue(secondResponse.isAcknowledged());
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testWarmMigrationForInvalidIndex() throws Exception {
        setupCluster(1);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> migrateToWarm("invalid-index"));
        assertTrue(exception.getMessage().contains("Failed to resolve index: invalid-index"));
    }

    public void testHotToWarmMigrationWithMasterNodeFailure() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder().put(buildDisabledAllocationSettings());

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(2, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        interceptCheckpointUpdates();

        final String[] indices = new String[] { "test-1", "test-2", "test-3" };

        try {
            for (String index : indices) {
                createTestIndex(index, 1, 1);
            }
            ensureGreen(indices);

            for (String index : indices) {
                assertIndexShardLocation(index, false);
            }

            for (String index : indices) {
                flush(index);
                waitForReplication(index);
                AcknowledgedResponse response = client().admin()
                    .indices()
                    .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", index))
                    .actionGet();
                assertTrue(response.isAcknowledged());
            }

            for (String index : indices) {
                assertBusy(() -> {
                    ClusterState state = client().admin().cluster().prepareState().get().getState();
                    ShardRouting shard = state.routingTable().index(index).shard(0).primaryShard();
                    assertFalse("Shard should not be relocating", shard.relocating());
                    assertIndexShardLocation(index, false);
                }, 30, TimeUnit.SECONDS);
            }

            internalCluster().stopCurrentClusterManagerNode();
            ensureStableCluster(6);

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            assertBusy(() -> {
                for (String index : indices) {
                    assertIndexShardLocation(index, true);
                    verifyWarmIndexSettings(index);
                }
            }, 60, TimeUnit.SECONDS);
        } finally {
            for (String index : indices) {
                client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            }
        }
    }

    public void testWarmMigrationReplicaCountFrom0To1() throws Exception {
        setupCluster(1);
        createTestIndex("test-replica-0to1", 1, 0);

        assertEquals(
            "0",
            client().admin()
                .indices()
                .prepareGetSettings("test-replica-0to1")
                .get()
                .getSetting("test-replica-0to1", IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );

        migrateToWarm("test-replica-0to1");

        assertEquals(
            "1",
            client().admin()
                .indices()
                .prepareGetSettings("test-replica-0to1")
                .get()
                .getSetting("test-replica-0to1", IndexMetadata.SETTING_NUMBER_OF_REPLICAS)
        );

        client().admin().indices().delete(new DeleteIndexRequest("test-replica-0to1")).actionGet();
    }

    public void testWarmMigrationWithTieringQueueSizeBreached() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(buildDisabledAllocationSettings())
            .put("cluster.tiering.max_concurrent_hot_to_warm_requests", 2);

        setupCluster(1, settingsBuilder.build());
        createTestIndex(INDEX_NAME, 1, 1);
        createTestIndex("test-index-2", 1, 1);
        createTestIndex("test-index-3", 1, 1);
        refreshClusterInfo();

        List<String> inFlightIndices = List.of(INDEX_NAME, "test-index-2");
        for (String index : inFlightIndices) {
            AcknowledgedResponse resp = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", index))
                .actionGet();
            assertTrue(resp.isAcknowledged());
            TieringTestUtils.safeSleep(4000);
        }

        assertThrows(
            OpenSearchRejectedExecutionException.class,
            () -> client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("WARM", "test-index-3"))
                .actionGet()
        );

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        client().admin().indices().delete(new DeleteIndexRequest("test-index-2")).actionGet();
        client().admin().indices().delete(new DeleteIndexRequest("test-index-3")).actionGet();
    }

    public void testWarmMigrationWithAllNodesWaterMarkBreached() {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "20b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 1);

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(2, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        interceptCheckpointUpdates();

        createTestIndex(INDEX_NAME, 1, 1);
        refreshClusterInfo();

        MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100L, 10L));
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 50L);

        verifyMigrationError(INDEX_NAME, "since we don't have enough space on all warm nodes");
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testWarmMigrationWithLargeTieringIndicesQueue() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "50b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 1.0);

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(2, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        interceptCheckpointUpdates();

        createTestIndex(INDEX_NAME, 1, 1);
        createTestIndex("test-index-2", 1, 1);
        createTestIndex("test-index-3", 1, 1);
        refreshClusterInfo();

        try {
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            final IndexTieringRequest stuckRequest = new IndexTieringRequest("warm", INDEX_NAME);
            AcknowledgedResponse stuckResponse = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, stuckRequest).actionGet();
            assertTrue(stuckResponse.isAcknowledged());

            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shardRouting = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
                assertFalse("Shard should not be relocating", shardRouting.relocating());
                assertIndexShardLocation(INDEX_NAME, false);
            }, 30, TimeUnit.SECONDS);

            final IndexTieringRequest secondRequest = new IndexTieringRequest("warm", "test-index-2");
            AcknowledgedResponse secondResponse = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, secondRequest)
                .actionGet();
            assertTrue(secondResponse.isAcknowledged());

            verifyMigrationError("test-index-3", "since we don't have enough space on all warm nodes");
        } finally {
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
            client().admin().indices().delete(new DeleteIndexRequest("test-index-2")).actionGet();
            client().admin().indices().delete(new DeleteIndexRequest("test-index-3")).actionGet();
        }
    }

    public void testWarmMigrationWithAllNodesWaterMarkBreachedWithExistingTieredIndices() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "150b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "100b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "50b")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 2.0);

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(2, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(1, settingsBuilder.build());
        interceptCheckpointUpdates();

        createTestIndex(INDEX_NAME, 1, 0);
        createTestIndex("test-index-2", 1, 0);

        AcknowledgedResponse resp = client().admin()
            .indices()
            .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", INDEX_NAME))
            .actionGet();
        assertTrue(resp.isAcknowledged());

        assertBusy(() -> assertIndexShardLocation(INDEX_NAME, true), 30, TimeUnit.SECONDS);

        MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 600L);
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, 850L));

        verifyMigrationError("test-index-2", "since we don't have enough space on all warm nodes");

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        client().admin().indices().delete(new DeleteIndexRequest("test-index-2")).actionGet();
    }

    public void testConcurrentMigrationWithStuckIndexAndDataNodesRestarts() throws Exception {
        setupCluster(1, buildDisabledAllocationSettings());
        final String STUCK_INDEX = "stuck-index";

        createTestIndex(STUCK_INDEX, 1, 1);
        ensureGreen(STUCK_INDEX);
        indexBulk(STUCK_INDEX, 100);
        refresh(STUCK_INDEX);

        try {
            flush(STUCK_INDEX);
            waitForReplication(STUCK_INDEX);
            AcknowledgedResponse resp = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", STUCK_INDEX))
                .actionGet();
            assertTrue(resp.isAcknowledged());
            TieringTestUtils.safeSleep(4000);

            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shard = state.routingTable().index(STUCK_INDEX).shard(0).primaryShard();
                assertFalse("Shard should not be relocating", shard.relocating());
                assertIndexShardLocation(STUCK_INDEX, false);
            }, 30, TimeUnit.SECONDS);

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            List<String> dataNodeNames = state.nodes()
                .getNodes()
                .values()
                .stream()
                .filter(node -> node.getRoles().contains(DiscoveryNodeRole.DATA_ROLE))
                .map(DiscoveryNode::getName)
                .collect(Collectors.toList());

            for (String nodeName : dataNodeNames) {
                internalCluster().restartNode(nodeName);
            }

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            assertBusy(() -> {
                assertIndexShardLocation(STUCK_INDEX, true);
                verifyWarmIndexSettings(STUCK_INDEX);
            }, 60, TimeUnit.SECONDS);

            refresh(STUCK_INDEX);
            assertEquals(100, client().prepareSearch(STUCK_INDEX).setSize(0).get().getHits().getTotalHits().value());
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(STUCK_INDEX)).actionGet();
        }
    }

    public void testMigrationWithStuckIndexAndPrimaryShardDataNodeBounceWithReplicas() throws Exception {
        final String STUCK_INDEX = "stuck-index";
        Settings.Builder settingsBuilder = Settings.builder().put(buildDisabledAllocationSettings());

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        String primaryNode = internalCluster().startDataOnlyNode(settingsBuilder.build());
        createTestIndex(STUCK_INDEX, 1, 1);
        ensureYellow(STUCK_INDEX);
        internalCluster().startDataOnlyNode(settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        ensureGreen(STUCK_INDEX);
        interceptCheckpointUpdates();

        indexBulk(STUCK_INDEX, 100);
        refresh(STUCK_INDEX);

        try {
            flush(STUCK_INDEX);
            waitForReplication(STUCK_INDEX);
            AcknowledgedResponse resp = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", STUCK_INDEX))
                .actionGet();
            assertTrue(resp.isAcknowledged());

            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shard = state.routingTable().index(STUCK_INDEX).shard(0).primaryShard();
                assertFalse("Shard should not be relocating", shard.relocating());
                assertIndexShardLocation(STUCK_INDEX, false);
            }, 30, TimeUnit.SECONDS);

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            assertBusy(() -> {
                ensureGreen(STUCK_INDEX);
                assertIndexShardLocation(STUCK_INDEX, true);
                verifyWarmIndexSettings(STUCK_INDEX);
            }, 60, TimeUnit.SECONDS);

            assertEquals(100, client().prepareSearch(STUCK_INDEX).setSize(0).get().getHits().getTotalHits().value());
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(STUCK_INDEX)).actionGet();
        }
    }

    public void testMigrationWithStuckIndexAndReplicaShardDataNodeBounce() throws Exception {
        final String STUCK_INDEX = "stuck-index";
        Settings.Builder settingsBuilder = Settings.builder().put(buildDisabledAllocationSettings());

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNode(settingsBuilder.build());
        createTestIndex(STUCK_INDEX, 1, 1);
        ensureYellow(STUCK_INDEX);
        String replicaNode = internalCluster().startDataOnlyNode(settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        ensureGreen(STUCK_INDEX);
        interceptCheckpointUpdates();

        indexBulk(STUCK_INDEX, 100);
        refresh(STUCK_INDEX);

        try {
            flush(STUCK_INDEX);
            waitForReplication(STUCK_INDEX);
            AcknowledgedResponse resp = client().admin()
                .indices()
                .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", STUCK_INDEX))
                .actionGet();
            assertTrue(resp.isAcknowledged());

            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting shard = state.routingTable().index(STUCK_INDEX).shard(0).primaryShard();
                assertFalse("Shard should not be relocating", shard.relocating());
                assertIndexShardLocation(STUCK_INDEX, false);
            }, 30, TimeUnit.SECONDS);

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            assertBusy(() -> {
                ensureGreen(STUCK_INDEX);
                assertIndexShardLocation(STUCK_INDEX, true);
                verifyWarmIndexSettings(STUCK_INDEX);
            }, 60, TimeUnit.SECONDS);

            assertEquals(100, client().prepareSearch(STUCK_INDEX).setSize(0).get().getHits().getTotalHits().value());
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(STUCK_INDEX)).actionGet();
        }
    }

    public void testHotToWarmMigrationDuringQuorumLoss() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder().put(buildDisabledAllocationSettings());

        String masterNodeId = internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(2, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
        interceptCheckpointUpdates();

        final String[] indices = new String[] { "test-1", "test-2", "test-3" };
        Map<String, Long> indexDocCounts = new HashMap<>();

        try {
            for (String index : indices) {
                createTestIndex(index, 1, 1);
                indexBulk(index, 100);
                indexDocCounts.put(index, 100L);
            }
            ensureGreen(indices);

            for (String index : indices) {
                flush(index);
                waitForReplication(index);
                AcknowledgedResponse response = client().admin()
                    .indices()
                    .execute(HotToWarmTierAction.INSTANCE, new IndexTieringRequest("warm", index))
                    .actionGet();
                assertTrue(response.isAcknowledged());
            }

            for (String index : indices) {
                assertBusy(() -> {
                    ClusterState state = client().admin().cluster().prepareState().get().getState();
                    ShardRouting shard = state.routingTable().index(index).shard(0).primaryShard();
                    assertFalse("Shard should not be relocating", shard.relocating());
                    assertIndexShardLocation(index, false);
                }, 30, TimeUnit.SECONDS);
            }

            internalCluster().restartNode(masterNodeId);
            ensureStableCluster(5);

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            assertBusy(() -> {
                for (String index : indices) {
                    assertIndexShardLocation(index, true);
                    verifyWarmIndexSettings(index);
                }
            }, 60, TimeUnit.SECONDS);

            for (String index : indices) {
                refresh(index);
                assertEquals(
                    indexDocCounts.get(index).longValue(),
                    client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value()
                );
            }
        } finally {
            for (String index : indices) {
                client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            }
        }
    }

    public void testWarmMigrationWithAllNodesJVMUtilizationBreached() {
        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 230L);
        clusterInfoService.setNodeResourceUsageFunctionAndRefresh(
            nodeId -> new NodeResourceUsageStats(nodeId, System.currentTimeMillis(), 99, 20, null)
        );

        verifyMigrationError(INDEX_NAME, "Rejecting tiering request as JVM Utilization is high on all [WARM] nodes");

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testHotToWarmMigrationWithUnassignedReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        internalCluster().startWarmOnlyNodes(1);
        interceptCheckpointUpdates();

        try {
            createTestIndex(INDEX_NAME, 1, 1);
            indexBulk(INDEX_NAME, 100);
            ensureYellow(INDEX_NAME);

            final IndexTieringRequest request = new IndexTieringRequest("warm", INDEX_NAME);
            AcknowledgedResponse response = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                ShardRouting primaryShard = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
                assertTrue("Primary should be assigned", primaryShard.assignedToNode());
                DiscoveryNode primaryNode = state.nodes().get(primaryShard.currentNodeId());
                assertTrue("Primary should be on warm node", primaryNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
                assertIndexShardLocation(INDEX_NAME, true);
                verifyWarmIndexSettings(INDEX_NAME);
            }, 30, TimeUnit.SECONDS);

            refresh(INDEX_NAME);
            assertEquals(100, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testWarmMigrationWithoutClusterInfoMockForJVMUtilization() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
            .put(ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(5000))
            .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500));

        setupCluster(1, settingsBuilder.build());
        createTestIndex(INDEX_NAME, 1, 1);

        // Adding a delay to let the JVM and CPU stats get collected
        TieringTestUtils.safeSleep(6000);
        refreshClusterInfo();
        migrateToWarm(INDEX_NAME);

        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testBulkOnWarmIndexWithFileCacheEvictionHappeningAlways() throws Exception {
        setupCluster(1);
        createTestIndex(INDEX_NAME_2, 1, 1);

        migrateToWarm(INDEX_NAME_2);

        for (int i = 0; i < 5; i++) {
            indexBulk(INDEX_NAME_2, 5000);
            flushAndRefresh(INDEX_NAME_2);
            TieringTestUtils.safeSleep(1000);
        }
        ensureGreen();
        indexBulk(INDEX_NAME_2, 5000);
        flushAndRefresh(INDEX_NAME_2);
        ensureGreen();
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME_2)).actionGet();
    }

    protected MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
    }

    protected static FsInfo.Path setDiskUsage(FsInfo.Path original, long totalBytes, long freeBytes) {
        return new FsInfo.Path(original.getPath(), original.getMount(), totalBytes, freeBytes, freeBytes);
    }
}
