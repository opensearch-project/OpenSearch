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

import org.opensearch.action.admin.cluster.blockcache.NodePruneBlockCacheResponse;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheAction;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheRequest;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
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

/**
 * Integration tests for the block cache prune API ({@code POST /_blockcache/prune}).
 * Verifies that the API clears the Foyer block cache on warm nodes without crashing the cluster.
 *
 * @opensearch.internal
 */
@ThreadLeakFilters(filters = PruneBlockCacheIT.IndexInputCleanerFilter.class)
public class PruneBlockCacheIT extends AbstractSnapshotIntegTestCase {

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
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "block_cache_prune_it");
    }

    /**
     * Verifies that POST /_blockcache/prune succeeds on a warm node with Foyer loaded,
     * returns cleared=true, and leaves the cluster healthy with queries still working.
     */
    public void testPruneBlockCacheDoesNotCrashCluster() throws Exception {
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
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

        // Trigger reads to populate the block cache
        assertDocCount(restoredIndexName, 50L);

        // Call POST /_blockcache/prune
        PruneBlockCacheRequest pruneRequest = new PruneBlockCacheRequest();
        PlainActionFuture<PruneBlockCacheResponse> future = new PlainActionFuture<>();
        client.execute(PruneBlockCacheAction.INSTANCE, pruneRequest, future);
        PruneBlockCacheResponse response = future.actionGet();

        // Verify response
        assertNotNull("Response should not be null", response);
        assertEquals("Should have no failures", 0, response.failures().size());
        assertEquals("Should target 1 warm node", 1, response.getNodes().size());

        NodePruneBlockCacheResponse nodeResponse = response.getNodes().get(0);
        assertTrue("Block cache should be cleared", nodeResponse.isCleared());

        // Cluster must still be healthy and queries must still work after prune
        ensureGreen();
        assertDocCount(restoredIndexName, 50L);
    }

    /**
     * Verifies that {@code GET _nodes/stats/file_cache?detailed=true} returns a non-null
     * {@code block_cache} section on warm nodes when the Foyer plugin is loaded.
     */
    public void testBlockCacheStatsDetailedWithFoyerPlugin() throws Exception {
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
        assertDocCount(indexName + "-copy", 50L);

        NodesStatsResponse statsResponse = client.admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName()).fileCacheDetailed(true))
            .actionGet();

        boolean foundWarmNode = false;
        for (NodeStats stats : statsResponse.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                foundWarmNode = true;
                BlockCacheStats blockCacheStats = stats.getBlockCacheOnlyStats();
                assertNotNull("block_cache sub-stats should be present on warm node with Foyer loaded", blockCacheStats);
                // totalBytes reflects the configured Foyer disk budget
                assertTrue("block_cache totalBytes should be >= 0", blockCacheStats.totalBytes() >= 0);
            }
        }
        assertTrue("Expected at least one warm node", foundWarmNode);
    }
}
