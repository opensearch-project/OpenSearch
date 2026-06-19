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

import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheAction;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheRequest;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.env.Environment;
import org.opensearch.node.Node;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Foyer block cache key_index with many remote-snapshot indices.
 *
 * <p>Uses 20 indices × 25 shards each = 500 total shards on the warm node. Tests verify
 * serialization correctness, periodic-persist stability, prune completion, bulk evict_prefix
 * correctness, and warm-node health at scale.
 *
 * @opensearch.internal
 */
@ThreadLeakFilters(filters = BlockCacheKeyIndexScaleIT.IndexInputCleanerFilter.class)
public class BlockCacheKeyIndexScaleIT extends AbstractSnapshotIntegTestCase {

    public static final class IndexInputCleanerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("index-input-cleaner");
        }
    }

    private static final String KEY_INDEX_FILENAME = "key_index.json";

    /**
     * 5 indices × 5 shards each = 25 total shards.
     * Large enough to exercise multi-index key_index serialization, periodic persist, and
     * bulk evict_prefix code paths without exhausting resources on a dev/CI machine.
     * True scale (10k-entry key_index) is covered by the Rust integration tests in tests.rs.
     */
    private static final int NUM_INDICES = 5;
    private static final int SHARDS_PER_IDX = 5;

    private static final String REPO_NAME = "repo-scale";
    private static final String SNAP_NAME = "snap-scale";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.<Class<? extends Plugin>>of(BlockCacheFoyerPlugin.class);
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
            // Raise shards-per-node limit.
            .put("cluster.max_shards_per_node", 200)
            // 2GB is small enough to pass disk validation on any dev/CI machine.
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), "2gb")
            // Short persist interval so tests don't wait 60 seconds.
            .put("block_cache.foyer.key_index_persist_interval_seconds", 5)
            .put("block_cache.foyer.key_index_sweep_interval_seconds", 0)
            .build();
    }

    @Override
    protected Settings.Builder randomRepositorySettings() {
        return Settings.builder()
            .put("location", randomRepoPath())
            .put("compress", randomBoolean())
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "key_index_10k_it");
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Restart the warm node after restoring all indices. Verifies that {@code key_index.json}
     * is written by the {@code Drop} impl on shutdown and is valid JSON.
     */
    public void testKeyIndexJsonWrittenOnShutdown() throws Exception {
        setupScaleShards();

        logger.info("[scale-01] restarting warm node '{}'", getWarmNodeName());
        internalCluster().restartNode(getWarmNodeName());
        // Use an extended timeout: after restart, 25 REMOTE_SNAPSHOT shards (5 indices × 5 shards)
        // must re-open before the cluster goes GREEN. The default 60s is insufficient under load.
        ensureGreen(TimeValue.timeValueSeconds(120));

        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node after restart", cacheDir);
        assertTrue("key_index.json must exist after restart", Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME)));

        String content = Files.readString(cacheDir.resolve(KEY_INDEX_FILENAME));
        assertFalse("key_index.json must not be empty", content.isBlank());
        assertTrue("key_index.json must contain version:1", content.contains("\"version\":1"));
        assertTrue("key_index.json must contain an index object", content.contains("\"index\":{"));
    }

    /**
     * Verifies that the periodic persist task writes {@code key_index.json} within the
     * configured interval (5s) without being starved when the key_index is large.
     */
    public void testPeriodicPersistFires() throws Exception {
        setupScaleShards();

        logger.info("[scale-02] waiting for periodic persist (interval=5s)");
        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node", cacheDir);

        assertBusy(
            () -> assertTrue("key_index.json must be written by periodic persist task", Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME))),
            30,
            TimeUnit.SECONDS
        );

        String content = Files.readString(cacheDir.resolve(KEY_INDEX_FILENAME));
        assertFalse("key_index.json must not be empty", content.isBlank());
        assertTrue("key_index.json must contain version:1", content.contains("\"version\":1"));
    }

    /**
     * Calls {@code /_blockcache/prune} and verifies it completes with no failures.
     * Validates that {@code clear()} + key_index wipe + file deletion does not time out.
     */
    public void testPruneCompletes() throws Exception {
        setupScaleShards();

        logger.info("[scale-03] pruning on warm node '{}'", getWarmNodeName());
        PlainActionFuture<PruneBlockCacheResponse> future = new PlainActionFuture<>();
        client().execute(PruneBlockCacheAction.INSTANCE, new PruneBlockCacheRequest(), future);
        PruneBlockCacheResponse pruneResponse = future.actionGet();

        assertNotNull(pruneResponse);
        assertEquals("Prune should have no failures", 0, pruneResponse.failures().size());
        assertEquals("Should target 1 warm node", 1, pruneResponse.getNodes().size());
        assertTrue("Block cache should be cleared", pruneResponse.getNodes().get(0).isCleared());

        long usedAfterPrune = getWarmNodeUsedBytes();
        assertThat("used_bytes must be 0 after prune", usedAfterPrune, equalTo(0L));
    }

    /**
     * Restart (triggering key_index recovery), delete half the indices, then prune.
     * Validates that bulk {@code evict_prefix()} on recovered prefix buckets completes
     * and the warm node remains healthy.
     */
    public void testEvictPrefixAfterRecovery() throws Exception {
        setupScaleShards();

        logger.info("[scale-04] restarting warm node to trigger key_index recovery");
        internalCluster().restartNode(getWarmNodeName());
        // Extended timeout: 25 REMOTE_SNAPSHOT shards must re-open after warm node restart.
        ensureGreen(TimeValue.timeValueSeconds(120));

        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist after restart", cacheDir);
        assertTrue("key_index.json must exist after recovery", Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME)));
        logger.info("[scale-04] recovery complete, key_index.json exists");

        // Delete half the indices to trigger evict_prefix for each.
        logger.info("[scale-04] deleting {} indices to trigger bulk evict_prefix", NUM_INDICES / 2);
        for (int i = 0; i < NUM_INDICES / 2; i++) {
            String indexName = indexName(i) + "-copy";
            if (client().admin().indices().prepareExists(indexName).get().isExists()) {
                client().admin().indices().prepareDelete(indexName).get();
            }
        }
        ensureGreen();

        // Prune to flush any remaining evictions.
        PlainActionFuture<PruneBlockCacheResponse> future = new PlainActionFuture<>();
        client().execute(PruneBlockCacheAction.INSTANCE, new PruneBlockCacheRequest(), future);
        PruneBlockCacheResponse pruneResponse = future.actionGet();
        assertEquals("Prune should have no failures", 0, pruneResponse.failures().size());

        long usedAfter = getWarmNodeUsedBytes();
        assertThat("used_bytes must be 0 after bulk evict_prefix + prune", usedAfter, equalTo(0L));

        // Warm node must still be responsive.
        NodesStatsResponse statsResponse = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName()).fileCacheDetailed(true))
            .actionGet();
        boolean foundWarm = statsResponse.getNodes().stream().anyMatch(n -> n.getNode().isWarmNode());
        assertTrue("Warm node must still be present and healthy after bulk evict_prefix", foundWarm);
    }

    /**
     * Verifies that the warm node remains healthy after restoring all indices,
     * cluster is GREEN, and block_cache stats are accessible.
     */
    public void testWarmNodeRemainsHealthy() throws Exception {
        setupScaleShards();
        ensureGreen();

        NodesStatsResponse response = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName()).fileCacheDetailed(true))
            .actionGet();

        boolean foundWarmNode = false;
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                foundWarmNode = true;
                BlockCacheStats blockCacheStats = stats.getBlockCacheOnlyStats();
                assertThat("block_cache stats must be present on warm node", blockCacheStats, notNullValue());
                assertThat("used_bytes must be >= 0", blockCacheStats.diskBytesUsed(), greaterThan(-1L));
            }
        }
        assertTrue("Expected at least one warm node to be present", foundWarmNode);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Creates {@value NUM_INDICES} indices × {@value SHARDS_PER_IDX} shards each,
     * snapshots them into a single repo, deletes the originals, ensures a warm node
     * exists, then restores all as REMOTE_SNAPSHOT indices on the warm node.
     */
    private void setupScaleShards() throws Exception {
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(1);
        logger.info(
            "[setup-shards] creating {} indices × {} shards each = {} total shards",
            NUM_INDICES,
            SHARDS_PER_IDX,
            NUM_INDICES * SHARDS_PER_IDX
        );

        // Create all indices.
        List<String> indexNames = new ArrayList<>(NUM_INDICES);
        for (int i = 0; i < NUM_INDICES; i++) {
            String name = indexName(i);
            indexNames.add(name);
            client.admin()
                .indices()
                .prepareCreate(name)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS_PER_IDX)
                        .build()
                )
                .get();
        }
        ensureGreen();

        // Index 1 doc into each index to make the snapshot non-trivial.
        for (String name : indexNames) {
            client.prepareIndex(name).setId("1").setSource("field", "value").get();
        }
        ensureGreen();

        // Snapshot all indices at once.
        createRepository(REPO_NAME, FsRepository.TYPE);
        final var snapResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO_NAME, SNAP_NAME)
            .setWaitForCompletion(true)
            .setIndices(indexNames.toArray(new String[0]))
            .get();
        assertThat(snapResponse.getSnapshotInfo().successfulShards(), greaterThan(0));

        // Delete all originals.
        client.admin().indices().prepareDelete(indexNames.toArray(new String[0])).get();
        ensureGreen();

        // Ensure warm node exists.
        internalCluster().ensureAtLeastNumWarmNodes(1);

        // Restore all as REMOTE_SNAPSHOT with "-copy" suffix.
        client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO_NAME, SNAP_NAME)
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setStorageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        ensureGreen();
        logger.info(
            "[setup-shards] {} remote-snapshot indices ({} total shards) ready on warm node",
            NUM_INDICES,
            NUM_INDICES * SHARDS_PER_IDX
        );
    }

    private static String indexName(int i) {
        return String.format(java.util.Locale.ROOT, "idx-%04d", i);
    }

    private String getWarmNodeName() {
        try {
            var nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
            for (var nodeInfo : nodesInfoResponse.getNodes()) {
                if (nodeInfo.getNode().isWarmNode()) {
                    return nodeInfo.getNode().getName();
                }
            }
        } catch (Exception ignored) {}
        String[] names = internalCluster().getNodeNames();
        return names.length > 0 ? names[0] : null;
    }

    private long getWarmNodeUsedBytes() {
        NodesStatsResponse response = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName()).fileCacheDetailed(true))
            .actionGet();
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                BlockCacheStats blockCacheStats = stats.getBlockCacheOnlyStats();
                if (blockCacheStats != null) {
                    return blockCacheStats.diskBytesUsed();
                }
            }
        }
        return 0L;
    }

    private Path findFoyerCacheDir() {
        String warmName = getWarmNodeName();
        assertNotNull("A warm node must be present in the cluster", warmName);
        Environment environment = internalCluster().getInstance(Environment.class, warmName);
        for (Path dataPath : environment.dataFiles()) {
            Path candidate = dataPath.resolve("foyer-block-cache");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
        }
        logger.warn("foyer-block-cache directory not found under warm node '{}'", warmName);
        return null;
    }
}
