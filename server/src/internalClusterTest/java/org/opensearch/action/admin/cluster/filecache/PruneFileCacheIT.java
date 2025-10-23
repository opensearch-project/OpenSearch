/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for File Cache Prune API.
 * Validates cache pruning with real data in cluster environment.
 *
 * @opensearch.internal
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class PruneFileCacheIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath()).put("compress", randomBoolean());
        settings.put(FsRepository.BASE_PATH_SETTING.getKey(), "file_cache_prune_it");
        return settings;
    }

    /**
     * Tests file cache pruning with real data on single warm node.
     */
    public void testPruneCacheWithRealData() throws Exception {
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        logger.info("--> Create index with documents on data node");
        internalCluster().ensureAtLeastNumDataNodes(1);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName);

        logger.info("--> Create repository and take snapshot");
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        logger.info("--> Start warm node and restore as searchable snapshot");
        internalCluster().ensureAtLeastNumWarmNodes(1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        logger.info("--> Trigger cache population by running multiple queries");
        // Run multiple queries to ensure cache is populated
        for (int i = 0; i < 3; i++) {
            assertDocCount(restoredIndexName, 100L);
        }

        assertBusy(() -> {
            long usage = getFileCacheUsage(client);
            assertTrue("Cache should be populated after index access", usage > 0);
        }, 30, TimeUnit.SECONDS);

        long usageBefore = getFileCacheUsage(client);
        logger.info("--> File cache usage before prune: {} bytes", usageBefore);
        assertTrue("File cache should have data before prune", usageBefore > 0);

        PruneFileCacheRequest request = new PruneFileCacheRequest();
        PlainActionFuture<PruneFileCacheResponse> future = new PlainActionFuture<>();
        client.execute(PruneFileCacheAction.INSTANCE, request, future);
        PruneFileCacheResponse response = future.actionGet();

        logger.info("--> Prune response: pruned {} bytes from {} nodes", response.getTotalPrunedBytes(), response.getNodes().size());

        // Verify response first - this is the key assertion
        assertNotNull("Response should not be null", response);
        assertEquals("Should have 1 successful node", 1, response.getNodes().size());
        assertEquals("Should have no failures", 0, response.failures().size());
        assertTrue("Operation should be successful", response.isCompletelySuccessful());
        assertTrue("Operation should be acknowledged", response.isAcknowledged());

        // The key assertion: pruned bytes should be > 0 (proves API actually worked)
        assertTrue("Should have pruned bytes", response.getTotalPrunedBytes() > 0);

        // Verify cache usage after prune
        long usageAfter = getFileCacheUsage(client);
        logger.info("--> File cache usage after prune: {} bytes", usageAfter);

        // Cache should be reduced (might not be zero if files are still referenced)
        assertTrue("Cache usage should be reduced after prune", usageAfter <= usageBefore);

        // The pruned bytes should roughly match the reduction
        long actualReduction = usageBefore - usageAfter;
        logger.info("--> Actual cache reduction: {} bytes, reported pruned: {} bytes", actualReduction, response.getTotalPrunedBytes());

        assertDocCount(restoredIndexName, 100L);
    }

    /**
     * Tests prune API response structure and metrics validation.
     */
    public void testPruneResponseMetrics() throws Exception {
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        logger.info("--> Setup simple scenario to test API response metrics");
        internalCluster().ensureAtLeastNumDataNodes(1);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        internalCluster().ensureAtLeastNumWarmNodes(1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        logger.info("--> Populate cache and measure before state");
        assertDocCount(restoredIndexName, 100L);

        assertBusy(() -> {
            long usage = getFileCacheUsage(client);
            assertTrue("Cache should be populated", usage > 0);
        }, 30, TimeUnit.SECONDS);

        long usageBefore = getFileCacheUsage(client);

        PruneFileCacheRequest request = new PruneFileCacheRequest();
        PlainActionFuture<PruneFileCacheResponse> future = new PlainActionFuture<>();
        client.execute(PruneFileCacheAction.INSTANCE, request, future);
        PruneFileCacheResponse response = future.actionGet();

        assertNotNull("Response should not be null", response);
        assertTrue("Should report acknowledged", response.isAcknowledged());
        assertEquals("Should target 1 warm node", 1, response.getNodes().size());
        assertEquals("Should have 0 failures", 0, response.failures().size());
        assertTrue("Should be successful", response.isCompletelySuccessful());

        NodePruneFileCacheResponse nodeResponse = response.getNodes().get(0);
        assertNotNull("Node response should not be null", nodeResponse);
        assertTrue("Node should have cache capacity", nodeResponse.getCacheCapacity() > 0);
        assertTrue("Node should report pruned bytes", nodeResponse.getPrunedBytes() >= 0);

        long usageAfter = getFileCacheUsage(client);
        long expectedPruned = usageBefore - usageAfter;
        assertEquals("Response should match actual cache reduction", expectedPruned, response.getTotalPrunedBytes());
    }

    /**
     * Creates index with documents and ensures cluster health is green.
     */
    private void createIndexWithDocsAndEnsureGreen(int numReplicas, int numDocs, String indexName) throws InterruptedException {
        createIndex(
            indexName,
            Settings.builder()
                .put(SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
                .build()
        );
        ensureGreen();
        indexRandomDocs(indexName, numDocs);
        ensureGreen();
    }

    /**
     * Creates snapshot repository with optional custom settings.
     */
    private void createRepositoryWithSettings(Settings.Builder repositorySettings, String repoName) {
        if (repositorySettings == null) {
            createRepository(repoName, FsRepository.TYPE);
        } else {
            createRepository(repoName, FsRepository.TYPE, repositorySettings);
        }
    }

    /**
     * Creates snapshot and validates success.
     */
    private void takeSnapshot(Client client, String snapshotName, String repoName, String... indices) {
        final var response = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indices)
            .get();

        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
    }

    /**
     * Deletes indices and ensures cluster health is green.
     */
    private void deleteIndicesAndEnsureGreen(Client client, String... indices) {
        assertTrue(client.admin().indices().prepareDelete(indices).get().isAcknowledged());
        ensureGreen();
    }

    /**
     * Restores snapshot as searchable snapshot and ensures cluster health is green.
     */
    private void restoreSnapshotAndEnsureGreen(Client client, String snapshotName, String repoName) {
        client.admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, snapshotName)
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setStorageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        ensureGreen();
    }

    /**
     * Validates that indices are configured as remote snapshot type.
     */
    private void assertRemoteSnapshotIndexSettings(Client client, String... indexNames) {
        var settingsResponse = client.admin().indices().prepareGetSettings(indexNames).execute().actionGet();

        assertEquals(indexNames.length, settingsResponse.getIndexToSettings().keySet().size());

        for (String indexName : indexNames) {
            assertEquals(
                IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey(),
                settingsResponse.getSetting(indexName, IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
            );
        }
    }

    /**
     * Returns total file cache usage across all warm nodes in bytes.
     */
    private long getFileCacheUsage(Client client) {
        NodesStatsResponse response = client.admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();

        long totalUsage = 0L;
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                AggregateFileCacheStats fcStats = stats.getFileCacheStats();
                if (fcStats != null) {
                    totalUsage += fcStats.getUsed().getBytes();
                }
            }
        }
        return totalUsage;
    }
}
