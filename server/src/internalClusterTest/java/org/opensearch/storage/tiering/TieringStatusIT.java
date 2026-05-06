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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.MockInternalClusterInfoService;
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
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.storage.action.tiering.HotToWarmTierAction;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.storage.action.tiering.WarmToHotTierAction;
import org.opensearch.storage.action.tiering.status.GetTieringStatusAction;
import org.opensearch.storage.action.tiering.status.ListTieringStatusAction;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.storage.action.tiering.status.model.TieringStatus.RUNNING_SHARDS;
import static org.opensearch.storage.tiering.TieringTestUtils.buildDisabledAllocationSettings;
import static org.opensearch.storage.tiering.TieringTestUtils.buildEnabledAllocationSettings;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TieringStatusIT extends RemoteStoreBaseIntegTestCase {
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    protected static final String TIERING_IN_PROGRESS_STATUS = "RUNNING_SHARD_RELOCATION";
    private static final String LIST_TIERING_STATUS_NO_FILTER_KEY = "noFilter";
    private static final String LIST_TIERING_STATUS_TARGET_HOT_TIER = "hotTier";
    private static final String LIST_TIERING_STATUS_TARGET_WARM_TIER = "warmTier";

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
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockInternalClusterInfoService.TestPlugin.class))
            .collect(Collectors.toList());
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
        return Settings.builder().put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true).build();
    }

    protected void setupCluster(int numberOfReplicas) {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(numberOfReplicas + 1);
        internalCluster().startWarmOnlyNodes(2);
    }

    protected void createTestIndex(String indexName, int numberOfShards, int numberOfReplicas) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();

        createIndex(indexName, indexSettings);
        ensureGreen(indexName);
        flush(indexName);
        waitForReplication(indexName);
    }

    protected void assertIndexShardLocation(String indexName, boolean onWarmNodes) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);

        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            ShardRouting primaryShard = shardRoutingTable.primaryShard();
            String assignedNodeId = primaryShard.currentNodeId();
            DiscoveryNode assignedNode = state.nodes().get(assignedNodeId);

            if (onWarmNodes) {
                assertTrue("Shard should be on warm node", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            } else {
                assertFalse("Shard should not be on warm node", assignedNode.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));
            }
        }
    }

    protected void verifyIndexSettings(String indexName, boolean isWarm) {
        Settings currentSettings = internalCluster().clusterService().state().metadata().index(indexName).getSettings();

        Map<String, Object> expectedSettings = isWarm
            ? TieringTestUtils.getExpectedWarmIndexSettings()
            : TieringTestUtils.getExpectedHotIndexSettings();

        for (Map.Entry<String, Object> entry : expectedSettings.entrySet()) {
            assertEquals(
                "Incorrect value for setting: " + entry.getKey(),
                entry.getValue().toString(),
                currentSettings.get(entry.getKey())
            );
        }
    }

    public void testTieringStatus() throws Exception {
        setupCluster(1);

        String[] indices = new String[] { "test-1", "test-2", "test-3", "test-4" };
        for (String index : indices) {
            createTestIndex(index, 1, 1);
        }
        ensureGreen(indices);

        Map<String, String> indicesToMigrate = new HashMap<>();
        Map<String, Integer> expectedResponse = new HashMap<>();

        // First migration -> initiate H2W for 2 indices
        indicesToMigrate.put("test-1", "warm");
        indicesToMigrate.put("test-2", "warm");
        expectedResponse.put(LIST_TIERING_STATUS_NO_FILTER_KEY, 2);
        expectedResponse.put(LIST_TIERING_STATUS_TARGET_HOT_TIER, 0);
        expectedResponse.put(LIST_TIERING_STATUS_TARGET_WARM_TIER, 2);
        initiateStuckMigrationToGetStatus(indicesToMigrate, expectedResponse);

        // Second migration -> initiate H2W for 2 indices and W2H for rest indices
        indicesToMigrate.clear();
        expectedResponse.clear();
        indicesToMigrate.put("test-3", "warm");
        indicesToMigrate.put("test-4", "warm");
        indicesToMigrate.put("test-1", "hot");
        indicesToMigrate.put("test-2", "hot");
        expectedResponse.put(LIST_TIERING_STATUS_NO_FILTER_KEY, 4);
        expectedResponse.put(LIST_TIERING_STATUS_TARGET_HOT_TIER, 2);
        expectedResponse.put(LIST_TIERING_STATUS_TARGET_WARM_TIER, 2);
        initiateStuckMigrationToGetStatus(indicesToMigrate, expectedResponse);

        // Cleanup
        for (String index : indices) {
            client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
        }
    }

    protected void initiateStuckMigrationToGetStatus(
        Map<String, String> indexToTargetTier,
        Map<String, Integer> expectedListTieringResponseCount
    ) throws Exception {

        logger.info("Updating cluster setting to pause recoveries");
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildDisabledAllocationSettings()).get();

        try {
            for (Map.Entry<String, String> entry : indexToTargetTier.entrySet()) {
                String indexName = entry.getKey();
                String targetTier = entry.getValue();
                final IndexTieringRequest stuckRequest = new IndexTieringRequest(targetTier, indexName);
                AcknowledgedResponse stuckResponse = client().admin()
                    .indices()
                    .execute(targetTier.equals("warm") ? HotToWarmTierAction.INSTANCE : WarmToHotTierAction.INSTANCE, stuckRequest)
                    .actionGet();
                assertTrue(stuckResponse.isAcknowledged());
            }

            verifyListTieringStatus(expectedListTieringResponseCount);
            verifyIndividualTieringStatus(indexToTargetTier);
        } finally {
            logger.info("Updating cluster setting to allow recoveries");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(buildEnabledAllocationSettings(2)).get();
        }

        waitForMigrationCompletion(indexToTargetTier);
    }

    protected void verifyListTieringStatus(Map<String, Integer> expectedCounts) {
        verifyTieringStatusResponse(new ListTieringStatusRequest(), expectedCounts.get(LIST_TIERING_STATUS_NO_FILTER_KEY));
        verifyTieringStatusResponse(new ListTieringStatusRequest("WARM"), expectedCounts.get(LIST_TIERING_STATUS_TARGET_WARM_TIER));
        verifyTieringStatusResponse(new ListTieringStatusRequest("HOT"), expectedCounts.get(LIST_TIERING_STATUS_TARGET_HOT_TIER));
    }

    protected void verifyTieringStatusResponse(ListTieringStatusRequest request, int expectedCount) {
        ListTieringStatusResponse response = client().admin().indices().execute(ListTieringStatusAction.INSTANCE, request).actionGet();
        assertEquals(expectedCount, response.getTieringStatusList().size());
        if (expectedCount > 0) {
            assertEquals(TIERING_IN_PROGRESS_STATUS, response.getTieringStatusList().getFirst().getStatus());
        }
    }

    protected void verifyIndividualTieringStatus(Map<String, String> indexToTargetTier) {
        for (Map.Entry<String, String> entry : indexToTargetTier.entrySet()) {
            String index = entry.getKey();
            String targetTier = entry.getValue();
            final GetTieringStatusRequest getTieringStatusRequest = new GetTieringStatusRequest(index);
            GetTieringStatusResponse tieringStatusResponse = client().admin()
                .indices()
                .execute(GetTieringStatusAction.INSTANCE, getTieringStatusRequest)
                .actionGet();

            assertEquals(TIERING_IN_PROGRESS_STATUS, tieringStatusResponse.getTieringStatus().getStatus());
            assertEquals(targetTier.toUpperCase(Locale.ROOT), tieringStatusResponse.getTieringStatus().getTarget());
            assertNotEquals(targetTier.toUpperCase(Locale.ROOT), tieringStatusResponse.getTieringStatus().getSource());
            assertEquals(
                0,
                tieringStatusResponse.getTieringStatus().getShardLevelStatus().getShardLevelCounters().get(RUNNING_SHARDS).intValue()
            );
        }
    }

    protected void waitForMigrationCompletion(Map<String, String> indicesToTargetTier) {
        CountDownLatch migrationLatch = new CountDownLatch(indicesToTargetTier.size());
        List<ClusterStateListener> listeners = new ArrayList<>();

        for (Map.Entry<String, String> entry : indicesToTargetTier.entrySet()) {
            String index = entry.getKey();
            String targetTier = entry.getValue();
            ClusterStateListener listener = new TieringTestUtils.MockTieringCompletionListener(migrationLatch, index, targetTier);
            listeners.add(listener);
            clusterService().addListener(listener);
        }

        try {
            assertTrue("Migration timed out", migrationLatch.await(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertBusy(() -> {
                for (Map.Entry<String, String> entry : indicesToTargetTier.entrySet()) {
                    String index = entry.getKey();
                    String targetTier = entry.getValue();
                    if (targetTier.equals("hot")) {
                        assertIndexShardLocation(index, false);
                        verifyIndexSettings(index, false);
                    } else {
                        assertIndexShardLocation(index, true);
                        verifyIndexSettings(index, true);
                    }
                }
            }, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            listeners.forEach(listener -> clusterService().removeListener(listener));
        }
    }
}
