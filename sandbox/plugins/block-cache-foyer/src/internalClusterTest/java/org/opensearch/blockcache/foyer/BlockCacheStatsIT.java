/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.node.Node;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.Collection;
import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for {@code GET _nodes/stats/file_cache?detailed=true} with the Foyer
 * block cache plugin loaded. Verifies that {@code block_cache} sub-stats are populated on
 * warm nodes when a {@code BlockCacheRegistry} is registered by the plugin.
 *
 * @opensearch.internal
 */
@ThreadLeakFilters(filters = BlockCacheStatsIT.IndexInputCleanerFilter.class)
public class BlockCacheStatsIT extends AbstractSnapshotIntegTestCase {

    /** Suppresses the JVM Cleaner daemon thread spawned by OnDemandBlockSnapshotIndexInput. */
    public static final class IndexInputCleanerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("index-input-cleaner");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlockCacheFoyerPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Enable Foyer via node settings (FeatureFlags.initFromSettings bypasses
            // the security manager that blocks System.getProperty in internalClusterTests).
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), "2gb")
            .build();
    }

    @Override
    protected Settings.Builder randomRepositorySettings() {
        return Settings.builder()
            .put("location", randomRepoPath())
            .put("compress", randomBoolean())
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "block_cache_stats_it");
    }

    /**
     * Verifies that with {@code ?detailed=true}:
     * <ul>
     *   <li>{@code aggregate_file_cache} is present on warm nodes</li>
     *   <li>{@code block_cache} sub-stats are non-null (Foyer registers a BlockCacheRegistry)</li>
     * </ul>
     */
    public void testDetailedStatsIncludeBlockCacheWithFoyerPlugin() throws Exception {
        final Client client = client();
        setupWarmNodeWithData(client);

        NodesStatsResponse response = client.admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName()).fileCacheDetailed(true))
            .actionGet();

        boolean foundWarmNode = false;
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                foundWarmNode = true;

                AggregateFileCacheStats aggregateStats = stats.getFileCacheStats();
                assertThat("aggregate_file_cache should be present", aggregateStats, notNullValue());

                BlockCacheStats blockCacheStats = stats.getBlockCacheOnlyStats();
                assertThat("block_cache sub-stats should be present when Foyer plugin is loaded", blockCacheStats, notNullValue());
                assertTrue("block_cache totalBytes should be >= 0", blockCacheStats.totalBytes() >= 0);
            }
        }
        assertTrue("Expected at least one warm node", foundWarmNode);
    }

    private void setupWarmNodeWithData(Client client) throws Exception {
        final String indexName = "test-idx";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";

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
        assertDocCount(indexName + "-copy", 50L);
    }
}
