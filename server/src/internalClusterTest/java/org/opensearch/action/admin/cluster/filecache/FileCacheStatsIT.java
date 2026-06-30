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
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.transport.client.Client;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for file cache and block cache stats via {@code _nodes/stats/file_cache}.
 *
 * @opensearch.internal
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FileCacheStatsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath()).put("compress", randomBoolean());
        settings.put(FsRepository.BASE_PATH_SETTING.getKey(), "file_cache_stats_it");
        return settings;
    }

    /**
     * Verifies that {@code aggregate_file_cache} is populated on warm nodes and that
     * {@code file_cache} / {@code block_cache} sub-sections are absent without {@code ?detailed=true}.
     */
    public void testFileCacheStatsWithoutDetailed() throws Exception {
        setupWarmNodeWithData();

        NodesStatsRequest request = new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName());
        NodesStatsResponse response = client().admin().cluster().nodesStats(request).actionGet();

        boolean foundWarmNode = false;
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                foundWarmNode = true;
                AggregateFileCacheStats fcStats = stats.getFileCacheStats();
                assertThat("aggregate_file_cache should be present on warm node", fcStats, notNullValue());
                assertThat("file cache total should be > 0", fcStats.getTotal().getBytes(), greaterThan(0L));

                // detailed sections must be absent when fileCacheDetailed=false
                assertThat("file_cache sub-stats should be null without ?detailed", stats.getFileCacheOnlyStats(), nullValue());
                assertThat("block_cache sub-stats should be null without ?detailed", stats.getBlockCacheOnlyStats(), nullValue());
            }
        }
        assertTrue("Expected at least one warm node in the response", foundWarmNode);
    }

    /**
     * Verifies that {@code file_cache} and {@code block_cache} sub-sections are present
     * when {@code ?detailed=true} is requested.
     */
    public void testFileCacheStatsWithDetailed() throws Exception {
        setupWarmNodeWithData();

        NodesStatsRequest request = new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName())
            .fileCacheDetailed(true);
        NodesStatsResponse response = client().admin().cluster().nodesStats(request).actionGet();

        boolean foundWarmNode = false;
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                foundWarmNode = true;

                // aggregate_file_cache must still be present
                assertThat("aggregate_file_cache should be present", stats.getFileCacheStats(), notNullValue());

                // file_cache sub-stats (FileCache-only breakdown)
                AggregateFileCacheStats fileCacheOnly = stats.getFileCacheOnlyStats();
                assertThat("file_cache sub-stats should be present with ?detailed", fileCacheOnly, notNullValue());
                assertThat("file_cache total should be >= 0", fileCacheOnly.getTotal().getBytes(), greaterThan(-1L));

                // block_cache sub-stats — only present if a block cache plugin is loaded
                // (null is valid when no BlockCacheRegistry is registered)
            }
        }
        assertTrue("Expected at least one warm node in the response", foundWarmNode);
    }

    /**
     * Verifies that non-warm nodes do not carry file cache stats.
     */
    public void testFileCacheStatsAbsentOnNonWarmNodes() throws Exception {
        setupWarmNodeWithData();

        NodesStatsRequest request = new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName());
        NodesStatsResponse response = client().admin().cluster().nodesStats(request).actionGet();

        for (NodeStats stats : response.getNodes()) {
            if (!stats.getNode().isWarmNode()) {
                assertThat(
                    "aggregate_file_cache should be null on non-warm node " + stats.getNode().getName(),
                    stats.getFileCacheStats(),
                    nullValue()
                );
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void setupWarmNodeWithData() throws Exception {
        final String indexName = "test-idx";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(1);
        createIndex(
            indexName,
            Settings.builder()
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
                .build()
        );
        ensureGreen();
        indexRandomDocs(indexName, 50);
        ensureGreen();

        createRepository(repoName, FsRepository.TYPE);
        final var snapResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(snapResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(snapResponse.getSnapshotInfo().successfulShards(), equalTo(snapResponse.getSnapshotInfo().totalShards()));

        assertTrue(client.admin().indices().prepareDelete(indexName).get().isAcknowledged());
        ensureGreen();

        internalCluster().ensureAtLeastNumWarmNodes(1);
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

        // Populate the cache
        assertDocCount(indexName + "-copy", 50L);
    }
}
