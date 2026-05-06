/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.InternalClusterInfoService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.storage.action.tiering.CancelTieringAction;
import org.opensearch.storage.action.tiering.CancelTieringRequest;
import org.opensearch.storage.action.tiering.HotToWarmTierAction;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.storage.action.tiering.WarmToHotTierAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.storage.tiering.TieringTestUtils.buildDisabledAllocationSettings;
import static org.opensearch.storage.tiering.TieringTestUtils.buildEnabledAllocationSettings;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for Tier Cancel API.
 * Tests end-to-end cancellation scenarios, error cases, and complex failure conditions.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TierCancelIT extends RemoteStoreBaseIntegTestCase {

    protected final String INDEX_NAME = "cancel-test-idx";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

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
        return super.nodePlugins().stream().collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(1, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    public void testCancelHotToWarmMigrationSuccess() throws Exception {
        logger.info("--> Testing successful hot-to-warm migration cancellation");

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        // Index some documents
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        Long expectedDocCount = indexStats.get(TOTAL_OPERATIONS);
        refresh(INDEX_NAME);

        try {
            // Disable allocation to cause stuck migration
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Start migration (it will be stuck due to disabled allocation)
            startMigrationToWarm(INDEX_NAME);
            TieringTestUtils.safeSleep(2000); // Let migration attempt start

            // Verify migration is in progress but stuck
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                Settings indexSettings = state.metadata().index(INDEX_NAME).getSettings();
                assertEquals("Index should have warm setting", "true", indexSettings.get(IS_WARM_INDEX_SETTING.getKey()));
                assertEquals(
                    "Index should be in HOT_TO_WARM state",
                    IndexModule.TieringState.HOT_TO_WARM.toString(),
                    indexSettings.get(INDEX_TIERING_STATE.getKey())
                );
            }, 10, TimeUnit.SECONDS);

            // Cancel the migration
            cancelTiering(INDEX_NAME);

            // Enable allocation to see the effect
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            TieringTestUtils.safeSleep(2000);
            // Verify index returns to hot tier
            verifyIndexInHotTier(INDEX_NAME);

            // Verify document count unchanged
            refresh(INDEX_NAME);
            assertEquals(
                "Document count should be preserved",
                expectedDocCount.longValue(),
                client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value()
            );

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testCancelWarmToHotMigrationSuccess() throws Exception {
        logger.info("--> Testing successful warm-to-hot migration cancellation");

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);
        startMigrationToWarm(INDEX_NAME);
        TieringTestUtils.safeSleep(2000);

        // Index some documents
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        Long expectedDocCount = indexStats.get(TOTAL_OPERATIONS);
        refresh(INDEX_NAME);

        try {
            // Disable allocation to cause stuck migration
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Start migration (it will be stuck due to disabled allocation)
            startMigrationToHot(INDEX_NAME);
            TieringTestUtils.safeSleep(2000); // Let migration attempt start

            // Verify migration is in progress but stuck
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                Settings indexSettings = state.metadata().index(INDEX_NAME).getSettings();
                assertFalse("Index should have warm setting", IS_WARM_INDEX_SETTING.get(indexSettings));
                assertEquals(
                    "Index should be in WARM_TO_HOT state",
                    IndexModule.TieringState.WARM_TO_HOT.toString(),
                    indexSettings.get(INDEX_TIERING_STATE.getKey())
                );
            }, 10, TimeUnit.SECONDS);

            // Cancel the migration
            cancelTiering(INDEX_NAME);

            // Enable allocation to see the effect
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            TieringTestUtils.safeSleep(2000);
            // Verify index returns to hot tier
            verifyIndexInWarmTier(INDEX_NAME);

            // Verify document count unchanged
            refresh(INDEX_NAME);
            assertEquals(
                "Document count should be preserved",
                expectedDocCount.longValue(),
                client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value()
            );

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testCancelIndexNotBeingTiered() {
        logger.info("--> Testing cancellation of index not being tiered");

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        try {
            // Try to cancel without starting migration
            verifyCancellationError(INDEX_NAME, "is not currently undergoing tiering operation");
        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testCancelAlreadyCompletedMigration() throws Exception {
        logger.info("--> Testing cancellation of already completed migration");

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        try {
            // Complete a migration first
            startMigrationToWarm(INDEX_NAME);
            TieringTestUtils.safeSleep(2000);
            verifyIndexInWarmTier(INDEX_NAME);
            // Wait for tiering to fully complete (tieringIndices cleanup)
            TieringTestUtils.safeSleep(5000);
            // Try to cancel completed migration
            verifyCancellationError(INDEX_NAME, "is not currently undergoing tiering operation");

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testIdempotentCancellation() throws Exception {
        logger.info("--> Testing idempotent cancellation (multiple cancels of same index)");

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        try {
            // Disable allocation to cause stuck migration
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Start migration
            startMigrationToWarm(INDEX_NAME);
            TieringTestUtils.safeSleep(2000);

            // First cancellation should succeed
            cancelTiering(INDEX_NAME);

            // Second cancellation should fail gracefully
            verifyCancellationError(INDEX_NAME, "is not currently undergoing tiering operation");

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    public void testCancelWithTimeoutParameters() throws Exception {
        logger.info("--> Testing cancellation with custom timeout parameters");

        setupCluster(1);
        createTestIndex(INDEX_NAME, 1, 1);

        try {
            // Disable allocation to cause stuck migration
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

            // Start migration
            startMigrationToWarm(INDEX_NAME);

            TieringTestUtils.safeSleep(2000);

            // Cancel with custom timeouts
            final CancelTieringRequest request = new CancelTieringRequest();
            request.setIndex(INDEX_NAME);
            request.timeout("30s");
            request.clusterManagerNodeTimeout("60s");

            AcknowledgedResponse response = client().admin().indices().execute(CancelTieringAction.INSTANCE, request).actionGet();
            assertTrue("Cancellation with timeouts should be acknowledged", response.isAcknowledged());

            // Enable allocation
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();

            // Verify cancellation worked
            verifyIndexInHotTier(INDEX_NAME);

        } finally {
            client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
        }
    }

    // Helper Methods
    protected void setupCluster(int numberOfReplicas) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10b")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0);

        internalCluster().startClusterManagerOnlyNode(settingsBuilder.build());
        internalCluster().startDataOnlyNodes(numberOfReplicas + 1, settingsBuilder.build());
        internalCluster().startWarmOnlyNodes(2, settingsBuilder.build());
    }

    protected void createTestIndex(String indexName, int numberOfShards, int numberOfReplicas) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();

        createIndex(indexName, indexSettings);
        refreshClusterInfo();
    }

    protected void startMigrationToWarm(String indexName) {
        final IndexTieringRequest request = new IndexTieringRequest("warm", indexName);
        AcknowledgedResponse response = client().admin().indices().execute(HotToWarmTierAction.INSTANCE, request).actionGet();
        assertTrue("Migration should be acknowledged", response.isAcknowledged());
    }

    protected void startMigrationToHot(String indexName) {
        final IndexTieringRequest request = new IndexTieringRequest("hot", indexName);
        AcknowledgedResponse response = client().admin().indices().execute(WarmToHotTierAction.INSTANCE, request).actionGet();
        assertTrue("Migration should be acknowledged", response.isAcknowledged());
    }

    protected void cancelTiering(String indexName) {
        final CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex(indexName);
        AcknowledgedResponse response = client().admin().indices().execute(CancelTieringAction.INSTANCE, request).actionGet();
        assertTrue("Cancellation should be acknowledged", response.isAcknowledged());
    }

    protected void verifyCancellationError(String indexName, String expectedErrorMessage) {
        final CancelTieringRequest request = new CancelTieringRequest();
        request.setIndex(indexName);
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin().indices().execute(CancelTieringAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    protected void verifyIndexInHotTier(String indexName) throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);

            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                ShardRouting shardRouting = shardRoutingTable.primaryShard();
                if (shardRouting.unassigned() && !shardRouting.primary()) {
                    continue;
                }
                String assignedNodeId = shardRouting.currentNodeId();
                DiscoveryNode assignedNode = state.nodes().get(assignedNodeId);
                assertFalse("Shard should not be on warm node", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            }

            // Verify settings are reverted
            Settings indexSettings = state.metadata().index(indexName).getSettings();
            assertFalse("index.warm setting should be removed", IS_WARM_INDEX_SETTING.get(indexSettings));
        }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected void verifyIndexInWarmTier(String indexName) throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);

            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                ShardRouting shardRouting = shardRoutingTable.primaryShard();
                if (shardRouting.unassigned() && !shardRouting.primary()) {
                    continue;
                }
                String assignedNodeId = shardRouting.currentNodeId();
                DiscoveryNode assignedNode = state.nodes().get(assignedNodeId);
                assertTrue("Shard should be on warm node", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            }
        }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected void refreshClusterInfo() {
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalCluster().getInstance(
            ClusterInfoService.class,
            internalCluster().getClusterManagerName()
        );
        infoService.refresh();
    }
}
