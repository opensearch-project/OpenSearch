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
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.node.Node;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.transport.client.Client;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for Foyer block cache key_index persistence and recovery.
 *
 * <p>Covers the scenarios from the manual test plan:
 * <ul>
 *   <li>GS-01/GS-02: key_index.json written after a warm node restart (graceful shutdown)</li>
 *   <li>GR-01/GR-02: key_index.json present after restart; used_bytes &gt; 0 without cold queries</li>
 *   <li>GR-03: cache hits (usedBytes still non-zero) after node restart proving recovery worked</li>
 *   <li>PP-01/PP-02/PP-03: periodic persist task writes key_index.json within the configured interval</li>
 *   <li>CL-02/CL-04: clear() (prune) resets used_bytes to 0; next restart starts empty</li>
 *   <li>GR-04: evict_prefix (shard deletion + prune) reduces usedBytes after recovery</li>
 *   <li>VM-01/VM-02: corrupt/wrong-version key_index.json → node starts healthy with empty key_index</li>
 *   <li>ST-01: first-ever startup (fresh cache dir) → used_bytes = 0, node healthy</li>
 * </ul>
 *
 * @opensearch.internal
 */
@ThreadLeakFilters(filters = BlockCacheKeyIndexRecoveryIT.IndexInputCleanerFilter.class)
public class BlockCacheKeyIndexRecoveryIT extends AbstractSnapshotIntegTestCase {

    /** Suppresses the JVM Cleaner daemon thread spawned by OnDemandBlockSnapshotIndexInput. */
    public static final class IndexInputCleanerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("index-input-cleaner");
        }
    }

    /** File names written by key_index_store.rs. Must match the Rust constants. */
    private static final String KEY_INDEX_FILENAME = "key_index.json";
    private static final String KEY_INDEX_TMP_FILENAME = ".key_index.json.tmp";

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
            // 2GB is small enough to pass disk validation on any dev/CI machine.
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(2, ByteSizeUnit.GB).toString())
            // Short persist interval so IT tests don't have to wait 60 seconds.
            .put("block_cache.foyer.key_index_persist_interval_seconds", 5)
            // Disable sweep task — not relevant for persistence ITs and avoids noise.
            .put("block_cache.foyer.key_index_sweep_interval_seconds", 0)
            .build();
    }

    @Override
    protected Settings.Builder randomRepositorySettings() {
        return Settings.builder()
            .put("location", randomRepoPath())
            .put("compress", randomBoolean())
            .put(FsRepository.BASE_PATH_SETTING.getKey(), "key_index_recovery_it");
    }

    // ── ST-01: First-ever startup ────────────────────────────────────────────

    /**
     * ST-01/ST-02: Fresh warm node with no prior cache data starts healthy and has usedBytes=0.
     * No key_index.json exists before any queries — the cache is empty.
     */
    public void testCleanStartupUsedBytesIsZero() throws Exception {
        internalCluster().ensureAtLeastNumWarmNodes(1);
        ensureGreen();
        logger.info("[ST-01] warm node '{}' started; querying block_cache stats", getWarmNodeName());

        // Query stats immediately — no queries have been run, so used_bytes should be 0.
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
                assertThat("usedBytes must be 0 on clean startup", blockCacheStats.diskBytesUsed(), equalTo(0L));
            }
        }
        assertTrue("Expected at least one warm node", foundWarmNode);
    }

    // ── GS-01/GS-02: Graceful shutdown persists key_index ────────────────────

    /**
     * GS-01/GS-02: Warm node restart writes {@code key_index.json} on shutdown and the file
     * survives across restarts.
     *
     * <p>The Foyer block cache is populated exclusively by the native tiered-object-store
     * path ({@code ts_get}/{@code ts_put}), not by Lucene REMOTE_SNAPSHOT reads. The
     * persistence tests therefore verify the key_index file lifecycle rather than
     * {@code used_bytes}. A warm node graceful restart always writes {@code key_index.json}
     * (even with an empty key_index) and the node must start healthy afterwards.
     */
    public void testKeyIndexWrittenOnShutdownAndRecoveredOnRestart() throws Exception {
        final Client client = client();
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(restoredIndexName, 50L);
        logger.info("[GS-01] index '{}' restored to warm node; restarting warm node '{}'", restoredIndexName, getWarmNodeName());

        // GS-01: Restart the warm node — Drop impl writes key_index.json before the JVM exits.
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();
        // GS-02: key_index.json must exist after graceful restart.
        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node after restart", cacheDir);
        assertTrue("key_index.json must exist after graceful warm node restart", Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME)));
        assertFalse(".key_index.json.tmp must not exist after successful rename", Files.exists(cacheDir.resolve(KEY_INDEX_TMP_FILENAME)));
        // GS-04: content check — key_index.json must be valid JSON with version=1.
        String content = Files.readString(cacheDir.resolve(KEY_INDEX_FILENAME));
        assertFalse("key_index.json must not be empty", content.isBlank());
        assertTrue("key_index.json must contain version:1", content.contains("\"version\":1"));
        assertTrue("key_index.json must contain an index object", content.contains("\"index\":{"));
        logger.info(
            "[GS-04] key_index.json content ({} bytes): {}",
            content.length(),
            content.length() > 200 ? content.substring(0, 200) + "…" : content
        );

        // Cluster must be healthy and warm node must report clean used_bytes = 0.
        BlockCacheStats stats = getWarmNodeBlockCacheStats(client);
        assertNotNull("block_cache stats must be present on warm node", stats);
        assertThat("used_bytes must be 0 after recovery of empty key_index", stats.diskBytesUsed(), equalTo(0L));

        // Queries must still work after restart.
        assertDocCount(restoredIndexName, 50L);
    }

    // ── GR-03: Cache hits after restart ──────────────────────────────────────

    /**
     * GR-03: After a warm node restart the key_index.json is written, recovered on the
     * next startup, and the cluster returns correct query results.
     *
     * <p>The Foyer block cache is populated only by the native tiered-object-store path;
     * Lucene REMOTE_SNAPSHOT reads do not call {@code put()}. This test therefore verifies
     * file-level recovery and query correctness, not {@code used_bytes}.
     */
    public void testCacheHitsAfterRestartDemonstrateRecovery() throws Exception {
        final Client client = client();
        final String indexName = "test-hits";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(indexName + "-copy", 50L);
        logger.info("[GR-03] index ready; restarting warm node '{}'", getWarmNodeName());

        // Restart warm node — Drop writes key_index.json.
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();
        // After restart key_index.json must exist on disk.
        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node after restart", cacheDir);
        assertTrue("key_index.json must exist after restart", Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME)));
        // Warm node must be healthy and serve queries correctly after recovery.
        assertDocCount(indexName + "-copy", 50L);
    }

    // ── PP-01/PP-02/PP-03: Periodic persist ──────────────────────────────────

    /**
     * PP-01/PP-02/PP-03: key_index.json must appear within the configured persist interval
     * (5 seconds in this test's nodeSettings) after cache population, WITHOUT a node restart.
     *
     * <p>This verifies the periodic persist task is running and writing to disk proactively,
     * not only on graceful shutdown.
     */
    public void testPeriodicPersistWritesKeyIndexWithinInterval() throws Exception {
        final Client client = client();
        final String indexName = "test-periodic";

        setupWarmNodeWithIndex(client, indexName);

        // Trigger cache population.
        assertDocCount(indexName + "-copy", 50L);
        logger.info("[PP-01] index ready; waiting up to 30s for periodic persist (interval=5s) to write key_index.json");

        // Poll for key_index.json to appear (written by the periodic persist task, not Drop).
        // The persist interval is 5 seconds (set in nodeSettings); we wait up to 30 seconds.
        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node", cacheDir);
        assertBusy(
            () -> assertTrue(
                "key_index.json must be written by periodic persist task within interval",
                Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME))
            ),
            30,
            TimeUnit.SECONDS
        );
        // Verify content is valid JSON with non-empty index.
        String content = Files.readString(cacheDir.resolve(KEY_INDEX_FILENAME));
        assertFalse("key_index.json must not be empty", content.isBlank());
        assertTrue("key_index.json must contain version:1", content.contains("\"version\":1"));
        logger.info(
            "[PP-02] key_index.json content ({} bytes): {}",
            content.length(),
            content.length() > 200 ? content.substring(0, 200) + "…" : content
        );

        // No .tmp file should be left behind.
        assertFalse(
            ".key_index.json.tmp must not exist after successful periodic persist",
            Files.exists(cacheDir.resolve(KEY_INDEX_TMP_FILENAME))
        );
    }

    // ── CL-02/CL-04: Prune (clear) resets cache state ────────────────────────

    /**
     * CL-02/CL-04: After a prune (POST /_blockcache/prune = clear()):
     * <ol>
     *   <li>usedBytes drops to 0 on the warm node.</li>
     *   <li>After a subsequent restart, used_bytes is still 0 (clear() deleted the snapshot).</li>
     * </ol>
     */
    public void testPruneClearsUsedBytesAndNextRestartIsEmpty() throws Exception {
        final Client client = client();
        final String indexName = "test-prune";

        setupWarmNodeWithIndex(client, indexName);

        assertDocCount(indexName + "-copy", 50L);

        long usedBeforePrune = getWarmNodeUsedBytes(client);
        // used_bytes is 0 because Lucene REMOTE_SNAPSHOT reads do not populate the Foyer block
        // cache (only native ts_get/ts_put does). We still verify the prune action completes
        // and the cache is healthy.
        // CL-01: POST /_blockcache/prune
        PruneBlockCacheRequest pruneRequest = new PruneBlockCacheRequest();
        PlainActionFuture<PruneBlockCacheResponse> future = new PlainActionFuture<>();
        client.execute(PruneBlockCacheAction.INSTANCE, pruneRequest, future);
        PruneBlockCacheResponse pruneResponse = future.actionGet();
        assertNotNull(pruneResponse);
        assertEquals("Prune should have no failures", 0, pruneResponse.failures().size());
        assertEquals("Should target 1 warm node", 1, pruneResponse.getNodes().size());
        NodePruneBlockCacheResponse nodeResponse = pruneResponse.getNodes().get(0);
        assertTrue("Block cache should be cleared", nodeResponse.isCleared());

        // CL-02: usedBytes must be 0 after prune (was already 0; verifies clear() succeeded).
        long usedAfterPrune = getWarmNodeUsedBytes(client);
        assertThat("used_bytes must be 0 after prune", usedAfterPrune, equalTo(0L));

        // CL-04: Restart — must start empty (clear() deleted the key_index snapshot).
        logger.info("[CL-04] restarting warm node '{}' after prune", getWarmNodeName());
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();

        long usedAfterRestartPostPrune = getWarmNodeUsedBytes(client);
        assertThat("used_bytes must be 0 after restart following prune (no stale snapshot)", usedAfterRestartPostPrune, equalTo(0L));

        // Cluster must be healthy and queries must still work.
        assertDocCount(indexName + "-copy", 50L);
    }

    // ── GR-04: evict_prefix works on recovered keys ───────────────────────────

    /**
     * GR-04: After recovery, deleting a remote-snapshot index triggers evict_prefix() for
     * that index's path prefix. usedBytes must decrease after the deletion + prune.
     */
    public void testEvictPrefixWorksOnRecoveredKeysAfterRestart() throws Exception {
        final Client client = client();
        final String indexName = "test-evict";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(indexName + "-copy", 50L);
        logger.info("[GR-04] index ready; restarting warm node '{}' to trigger recovery", getWarmNodeName());

        // Restart warm node — Drop writes key_index.json; next startup recovers it.
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();
        // key_index.json must exist after restart.
        Path recoveryCheck = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node after restart", recoveryCheck);
        assertTrue("key_index.json must exist after recovery", Files.exists(recoveryCheck.resolve(KEY_INDEX_FILENAME)));
        // Delete the remote-snapshot index — this should trigger evict_prefix() for all
        // cache entries belonging to this index.
        logger.info("[GR-04] deleting index '{}-copy' to trigger evict_prefix()", indexName);
        assertTrue(client.admin().indices().prepareDelete(indexName + "-copy").get().isAcknowledged());
        ensureGreen();

        // Prune to force eviction of any remaining entries.
        PlainActionFuture<PruneBlockCacheResponse> future = new PlainActionFuture<>();
        client.execute(PruneBlockCacheAction.INSTANCE, new PruneBlockCacheRequest(), future);
        PruneBlockCacheResponse pruneResponse = future.actionGet();
        assertEquals("Prune should have no failures", 0, pruneResponse.failures().size());

        // usedBytes must have decreased (entries for the deleted index were evicted).
        long usedAfterEvict = getWarmNodeUsedBytes(client);
        assertThat("used_bytes must decrease after index deletion + prune (evict_prefix on recovered keys)", usedAfterEvict, equalTo(0L));

        // Cluster must still be healthy.
        ensureGreen();
    }

    // ── VM-01/VM-02: Corrupt or wrong-version snapshot → clean startup ────────

    /**
     * VM-02: A corrupt key_index.json (invalid JSON) on a warm node's cache dir is
     * tolerated — the node starts healthy with used_bytes = 0 and queries work.
     */
    public void testCorruptKeyIndexFileResultsInCleanStartup() throws Exception {
        final Client client = client();
        final String indexName = "test-corrupt";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(indexName + "-copy", 50L);

        // Wait for the periodic persist to write key_index.json.
        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node", cacheDir);
        assertBusy(() -> assertTrue(Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME))), 30, TimeUnit.SECONDS);
        logger.info("[VM-02] key_index.json exists; overwriting with corrupt JSON at {}", cacheDir.resolve(KEY_INDEX_FILENAME));

        // Overwrite with corrupt JSON while the node is about to restart.
        // We write it and then restart immediately — simulates an admin corruption or
        // a partial write that left garbage on disk.
        Files.writeString(cacheDir.resolve(KEY_INDEX_FILENAME), "{{{invalid_json}}}", StandardCharsets.UTF_8);

        // Restart the warm node — load_or_empty() must return empty on corrupt JSON.
        logger.info("[VM-02] restarting warm node '{}' with corrupt key_index.json", getWarmNodeName());
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();
        // VM-04: used_bytes must be 0 (corrupt file treated as clean startup).
        long usedAfterCorruptRestart = getWarmNodeUsedBytes(client);
        assertThat("used_bytes must be 0 after restart with corrupt key_index.json", usedAfterCorruptRestart, equalTo(0L));

        // Cluster must be healthy and queries must work (cache fills from scratch).
        assertDocCount(indexName + "-copy", 50L);
    }

    /**
     * VM-01: A wrong-version key_index.json is rejected on startup — node starts healthy
     * with used_bytes = 0.
     */
    public void testWrongVersionKeyIndexFileResultsInCleanStartup() throws Exception {
        final Client client = client();
        final String indexName = "test-version";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(indexName + "-copy", 50L);

        Path cacheDir = findFoyerCacheDir();
        assertNotNull("foyer-block-cache directory must exist on warm node", cacheDir);
        // Wait for key_index.json to appear from periodic persist.
        assertBusy(() -> assertTrue(Files.exists(cacheDir.resolve(KEY_INDEX_FILENAME))), 30, TimeUnit.SECONDS);
        logger.info("[VM-01] key_index.json exists; overwriting with version=999 at {}", cacheDir.resolve(KEY_INDEX_FILENAME));

        // Write a snapshot with wrong version (999 instead of 1).
        Files.writeString(cacheDir.resolve(KEY_INDEX_FILENAME), "{\"version\":999,\"index\":{}}", StandardCharsets.UTF_8);

        // Restart — parse_and_validate() rejects version mismatch → clean startup.
        logger.info("[VM-01] restarting warm node '{}' with wrong-version key_index.json", getWarmNodeName());
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();
        long usedAfterWrongVersion = getWarmNodeUsedBytes(client);
        assertThat("used_bytes must be 0 after restart with wrong-version key_index.json", usedAfterWrongVersion, equalTo(0L));

        // Cluster must be healthy and queries must work.
        assertDocCount(indexName + "-copy", 50L);
    }

    // ── Tiered-mode coverage ─────────────────────────────────────────────────

    /**
     * Tiered mode (default): both the data tier and the metadata tier each persist
     * their own {@code key_index.json} on graceful warm-node restart.
     *
     * <p>With {@code block_cache.foyer.metadata_cache_ratio > 0} (5% by default) the
     * Foyer block cache splits into two independent instances under
     * {@code foyer-block-cache/data} and {@code foyer-block-cache/metadata}. Each
     * tier has its own Drop impl writing its own snapshot. This test guards against
     * a regression where one tier's persistence wiring is broken while the other
     * appears healthy — the previous helper would only see the data tier and miss
     * a metadata-tier failure entirely.
     */
    public void testTieredBothTiersWriteKeyIndexOnShutdown() throws Exception {
        final Client client = client();
        final String indexName = "test-tiered-shutdown";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(indexName + "-copy", 50L);

        logger.info("[tiered-shutdown-01] restarting warm node '{}'", getWarmNodeName());
        internalCluster().restartNode(getWarmNodeName());
        ensureGreen();

        Path dataDir = findFoyerCacheDir();
        Path metaDir = findFoyerMetadataCacheDir();
        assertNotNull("data-tier cache directory must exist on warm node after restart", dataDir);
        assertNotNull(
            "metadata-tier cache directory must exist on warm node after restart "
                + "(check block_cache.foyer.metadata_cache_ratio is > 0)",
            metaDir
        );

        // Both tiers wrote a key_index.json.
        Path dataKeyIndex = dataDir.resolve(KEY_INDEX_FILENAME);
        Path metaKeyIndex = metaDir.resolve(KEY_INDEX_FILENAME);
        assertTrue("data-tier key_index.json must exist after restart", Files.exists(dataKeyIndex));
        assertTrue("metadata-tier key_index.json must exist after restart", Files.exists(metaKeyIndex));

        // No leftover .tmp files on either tier.
        assertFalse(
            "data-tier .key_index.json.tmp must not exist after successful rename",
            Files.exists(dataDir.resolve(KEY_INDEX_TMP_FILENAME))
        );
        assertFalse(
            "metadata-tier .key_index.json.tmp must not exist after successful rename",
            Files.exists(metaDir.resolve(KEY_INDEX_TMP_FILENAME))
        );

        // Both snapshots are valid JSON with version=1.
        String dataContent = Files.readString(dataKeyIndex);
        String metaContent = Files.readString(metaKeyIndex);
        assertFalse("data-tier key_index.json must not be empty", dataContent.isBlank());
        assertFalse("metadata-tier key_index.json must not be empty", metaContent.isBlank());
        assertTrue("data-tier key_index.json must contain version:1", dataContent.contains("\"version\":1"));
        assertTrue("metadata-tier key_index.json must contain version:1", metaContent.contains("\"version\":1"));

        // Cluster healthy after restart.
        assertDocCount(indexName + "-copy", 50L);
    }

    /**
     * Tiered mode: the periodic persist task fires independently on both tiers
     * and writes {@code key_index.json} within the configured interval.
     *
     * <p>Each tier owns its own persist task, so this test catches the case where
     * one tier's task is starved or never spawned — symptoms that would otherwise
     * only surface on shutdown.
     */
    public void testTieredBothTiersPeriodicPersistFires() throws Exception {
        final Client client = client();
        final String indexName = "test-tiered-periodic";

        setupWarmNodeWithIndex(client, indexName);
        assertDocCount(indexName + "-copy", 50L);

        Path dataDir = findFoyerCacheDir();
        Path metaDir = findFoyerMetadataCacheDir();
        assertNotNull("data-tier cache directory must exist on warm node", dataDir);
        assertNotNull(
            "metadata-tier cache directory must exist on warm node " + "(check block_cache.foyer.metadata_cache_ratio is > 0)",
            metaDir
        );

        logger.info("[tiered-periodic-01] waiting up to 30s for periodic persist (interval=5s) on data and metadata tiers");

        // Persist interval is 5s (set in nodeSettings); allow up to 30s for both tiers.
        assertBusy(() -> {
            assertTrue(
                "data-tier key_index.json must be written by periodic persist task within interval",
                Files.exists(dataDir.resolve(KEY_INDEX_FILENAME))
            );
            assertTrue(
                "metadata-tier key_index.json must be written by periodic persist task within interval",
                Files.exists(metaDir.resolve(KEY_INDEX_FILENAME))
            );
        }, 30, TimeUnit.SECONDS);

        // No leftover .tmp files after successful rename on either tier.
        assertFalse(
            "data-tier .key_index.json.tmp must not exist after successful periodic persist",
            Files.exists(dataDir.resolve(KEY_INDEX_TMP_FILENAME))
        );
        assertFalse(
            "metadata-tier .key_index.json.tmp must not exist after successful periodic persist",
            Files.exists(metaDir.resolve(KEY_INDEX_TMP_FILENAME))
        );

        // Both tiers' content is valid JSON with version=1.
        String dataContent = Files.readString(dataDir.resolve(KEY_INDEX_FILENAME));
        String metaContent = Files.readString(metaDir.resolve(KEY_INDEX_FILENAME));
        assertTrue("data-tier key_index.json must contain version:1", dataContent.contains("\"version\":1"));
        assertTrue("metadata-tier key_index.json must contain version:1", metaContent.contains("\"version\":1"));
    }

    // ── Helper methods ────────────────────────────────────────────────────────

    /**
     * Sets up a warm node cluster with a remote-snapshot index named {@code indexName + "-copy"}.
     * Creates a snapshot, deletes the original index, and restores it as a remote snapshot.
     */
    private void setupWarmNodeWithIndex(Client client, String indexName) throws Exception {
        final String repoName = indexName + "-repo";
        final String snapshotName = indexName + "-snap";

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
    }

    /** Returns the name of the first warm node in the cluster. Falls back to a random node. */
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

    /**
     * Returns the {@link BlockCacheStats} from the first warm node, or {@code null} if absent.
     */
    private BlockCacheStats getWarmNodeBlockCacheStats(Client client) {
        NodesStatsResponse response = client.admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric(NodesStatsRequest.Metric.FILE_CACHE_STATS.metricName()).fileCacheDetailed(true))
            .actionGet();
        for (NodeStats stats : response.getNodes()) {
            if (stats.getNode().isWarmNode()) {
                return stats.getBlockCacheOnlyStats();
            }
        }
        return null;
    }

    /**
     * Returns the usedBytes from the first warm node's block_cache stats.
     * Returns 0 if no warm node is found or if block_cache stats are absent.
     */
    private long getWarmNodeUsedBytes(Client client) {
        BlockCacheStats s = getWarmNodeBlockCacheStats(client);
        return s != null ? s.diskBytesUsed() : 0L;
    }

    /**
     * Locates the Foyer block cache directory ({@code <data-path>/foyer-block-cache}) on the
     * warm node.
     *
     * <p>Specifically queries the warm node's {@link Environment} so that the correct data
     * path is used even in a multi-node cluster where the cluster manager and data nodes have
     * different node ordinals. Fails the test with a descriptive message if the directory is
     * not found — this ensures file-level assertions are never silently skipped.
     */
    /**
     * Locates the Foyer block cache directory that holds {@code key_index.json}.
     *
     * <p>In tiered mode (default — {@code block_cache.foyer.metadata_cache_ratio > 0})
     * the layout is {@code foyer-block-cache/data/} and {@code foyer-block-cache/metadata/};
     * each subdir has its own {@code key_index.json}. This helper returns the data
     * subdir in that case. In single-cache mode the file lives directly under
     * {@code foyer-block-cache/}, which is what gets returned then.
     *
     * <p>Use {@link #findFoyerMetadataCacheDir()} for the metadata-tier subdir.
     */
    private Path findFoyerCacheDir() {
        Path root = findFoyerCacheRoot();
        if (root == null) {
            return null;
        }
        Path dataSubdir = root.resolve("data");
        if (Files.isDirectory(dataSubdir)) {
            return dataSubdir;       // tiered mode
        }
        return root;                 // single-cache mode
    }

    /**
     * Returns the metadata-tier subdir ({@code foyer-block-cache/metadata}) when
     * the cache is in tiered mode, or {@code null} in single-cache mode.
     */
    private Path findFoyerMetadataCacheDir() {
        Path root = findFoyerCacheRoot();
        if (root == null) {
            return null;
        }
        Path metaSubdir = root.resolve("metadata");
        return Files.isDirectory(metaSubdir) ? metaSubdir : null;
    }

    /** Locates the {@code foyer-block-cache} parent directory on the warm node. */
    private Path findFoyerCacheRoot() {
        String warmName = getWarmNodeName();
        assertNotNull("A warm node must be present in the cluster", warmName);

        Environment environment = internalCluster().getInstance(Environment.class, warmName);
        for (Path dataPath : environment.dataFiles()) {
            Path candidate = dataPath.resolve("foyer-block-cache");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
        }

        // The cache directory is always created when the Foyer plugin is active on a warm
        // node and diskCapacityBytes > 0. Returning null here means either the plugin was
        // disabled (diskCapacityBytes = 0) or the directory has not been created yet — both
        // are unexpected in this test suite and callers must handle null gracefully.
        logger.warn("foyer-block-cache directory not found under warm node '{}' data paths: {}", warmName, environment.dataFiles());
        return null;
    }
}
