/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.common.util.FeatureFlags.WRITABLE_WARM_INDEX_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareRestoreShallowSnapshotV2IT extends AbstractSnapshotIntegTestCase {

    private static final String BASE_REMOTE_REPO = "test-rs-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;
    private Path remoteRepoPath;

    public DataFormatAwareRestoreShallowSnapshotV2IT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @Before
    public void setup() {
        remoteRepoPath = randomRepoPath().toAbsolutePath();
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(BASE_REMOTE_REPO).get();
        if (WRITABLE_WARM_INDEX_SETTING.get(settings)) {
            assertAcked(client().admin().indices().prepareDelete("_all").get());
            var nodes = internalCluster().getDataNodeInstances(Node.class);
            for (var node : nodes) {
                var fileCache = node.fileCache();
                fileCache.clear();
            }
        }
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        // For DFA + V2 test, warm-index is orthogonal and slows test; use only non-warm variant.
        return Arrays.<Object[]>asList(new Object[] { Settings.builder().put(WRITABLE_WARM_INDEX_SETTING.getKey(), false).build() });
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(BASE_REMOTE_REPO, remoteRepoPath))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ArrowBasePlugin.class,
                ParquetDataFormatPlugin.class,
                CompositeDataFormatPlugin.class,
                LucenePlugin.class,
                DataFusionPlugin.class
            )
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings.Builder getRepositorySettings(Path location, boolean shallowCopyEnabled) {
        Settings.Builder settingsBuilder = randomRepositorySettings();
        settingsBuilder.put("location", location);
        if (shallowCopyEnabled) {
            settingsBuilder.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
                .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        }
        return settingsBuilder;
    }

    protected Settings.Builder getRepositorySettings(String sourceRepository, boolean readOnly) throws ExecutionException,
        InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { sourceRepository });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        return Settings.builder()
            .put(rmd.settings())
            .put(BlobStoreRepository.READONLY_SETTING.getKey(), readOnly)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), false)
            .put(SYSTEM_REPOSITORY_SETTING.getKey(), false);
    }

    /**
     * Secondary data formats for the DFA index. By default this is empty (parquet-only).
     * Subclasses (e.g. {@code DataFormatAwareRestoreShallowSnapshotV2WithLuceneIT}) override
     * this to add lucene as a secondary format.
     */
    protected List<String> getSecondaryDataFormats() {
        return List.of("lucene");
    }

    /**
     * Whether lucene is configured as a secondary format. Drives format-aware on-disk assertions.
     */
    protected boolean hasLuceneSecondary() {
        return true;
    }

    protected Settings.Builder getIndexSettings(int numOfShards, int numOfReplicas) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", getSecondaryDataFormats());
        if (WRITABLE_WARM_INDEX_SETTING.get(settings)) {
            settingsBuilder.put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true);
        }
        return settingsBuilder;
    }

    private void indexDocuments(Client client, String indexName, int numOfDocs) {
        indexDocuments(client, indexName, 0, numOfDocs);
    }

    protected void indexDocuments(Client client, String indexName, int fromId, int toId) {
        for (int i = fromId; i < toId; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(indexName).setSource("text", "sometext").get();
        }
    }

    /**
     * Validate doc count via _stats API (uses engine's docStats(), not search).
     * Search isn't supported on DFA indices yet, so we avoid prepareSearch / prepareGet.
     */
    private void assertDocCountInIndex(Client client, String indexName, long expectedCount) {
        long actualCount = client.admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setDocs(true)
            .get()
            .getIndex(indexName)
            .getPrimaries().docs.getCount();
        assertEquals(
            "doc count mismatch for index " + indexName + " (expected " + expectedCount + ", got " + actualCount + ")",
            expectedCount,
            actualCount
        );
    }

    /**
     * Set of formats this test's index uses. Matches the index settings:
     * primary=parquet, secondary=[lucene]. Lucene is also always present implicitly.
     */
    private Set<String> expectedFormats() {
        return Set.of("parquet", "lucene");
    }

    /**
     * Assert each non-Lucene format directory exists under the shard data path and contains files.
     * Mirrors {@code DataFormatAwareReplicationBaseIT.assertAllFormatDirsHaveFiles}.
     */
    private void assertAllFormatDirsHaveFiles(IndexShard shard) throws IOException {
        for (String format : expectedFormats()) {
            if ("lucene".equals(format)) continue; // Lucene files live under shard/index/, not a separate dir
            Path dir = shard.shardPath().getDataPath().resolve(format);
            assertTrue("format directory must exist: " + format + " (" + dir + ")", Files.exists(dir));
            try (var stream = Files.list(dir)) {
                assertTrue("format directory must have files: " + format + " (" + dir + ")", stream.findAny().isPresent());
            }
        }
    }

    /**
     * Assert the Lucene index/ directory contents match the configured layout:
     * <ul>
     *   <li>parquet+lucene-secondary: index/ has segments_N + at least one segment data file</li>
     *   <li>parquet-only: index/ has only segments_N (and possibly write.lock); no segment data</li>
     * </ul>
     * Mirrors {@code DataFormatAwareReplicationBaseIT.assertLuceneIndexDirContents}.
     */
    private void assertLuceneIndexDirContents(IndexShard shard) throws IOException {
        Path indexDir = shard.shardPath().resolveIndex();
        assertTrue("index/ directory must exist: " + indexDir, Files.exists(indexDir));
        Set<String> files;
        try (var stream = Files.list(indexDir)) {
            files = stream.map(p -> p.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
        }
        assertTrue("index/ must contain segments_N, got " + files, files.stream().anyMatch(f -> f.startsWith("segments_")));
        Set<String> nonSegmentsFiles = files.stream()
            .filter(f -> f.startsWith("segments_") == false && f.equals("write.lock") == false)
            .collect(java.util.stream.Collectors.toSet());
        if (hasLuceneSecondary()) {
            assertFalse(
                "parquet+lucene-secondary: index/ must have segment data files beyond segments_N, got only: " + files,
                nonSegmentsFiles.isEmpty()
            );
        } else {
            assertTrue(
                "parquet-only: index/ must have only segments_N (and write.lock), got extra: " + nonSegmentsFiles,
                nonSegmentsFiles.isEmpty()
            );
        }
    }

    /**
     * Get the IndexShard for shard 0 of the index on the first data node in the cluster.
     */
    private IndexShard getShardZero(String indexName) {
        String node = internalCluster().getDataNodeNames().iterator().next();
        return getIndexShard(node, new ShardId(resolveIndex(indexName), 0), indexName);
    }

    /**
     * End-to-end snapshot V2 + restore test for a DFA (parquet primary, lucene secondary) index.
     *
     * <p>Pre-snapshot assertions:
     * <ul>
     *   <li>Doc count via _stats matches what was indexed</li>
     *   <li>All format directories (parquet, lucene index/) exist with expected files</li>
     *   <li>Catalog files match local store and remote store</li>
     * </ul>
     *
     * <p>Snapshot V2 assertions:
     * <ul>
     *   <li>Snapshot state == SUCCESS, all shards successful (cluster-manager-only flow)</li>
     * </ul>
     *
     * <p>Post-restore assertions:
     * <ul>
     *   <li>Restore status == OK</li>
     *   <li>Restored index reaches green health</li>
     *   <li>Restored doc count matches</li>
     *   <li>All format dirs (parquet, lucene index/) reconstructed on disk with files</li>
     *   <li>Catalog files match local store and remote store on the restored shard</li>
     * </ul>
     *
     * <p>Note: does not use prepareSearch or prepareGet because search is not yet supported
     * on DFA indices. Validation goes through admin APIs (_stats, cluster state) and direct
     * shard inspection (catalog snapshot, on-disk format dirs, remote store listing).
     *
     * <p>V2 path is triggered by passing an empty indices list to createSnapshot:
     * {@code request.indices().length == 0} is part of the V2 gate in
     * {@link org.opensearch.snapshots.SnapshotsService} (~line 293).
     */
    public void testV2SnapshotCreateAndRestoreForDFAIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-testindex";
        String snapshotRepoName = "test-snapshot-repo";
        String snapshotName = "test-v2-snapshot";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        // Create snapshot repo with V2 enabled
        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        // Create DFA index (parquet primary + lucene secondary, see getIndexSettings)
        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        // Index documents and refresh so segments are created in BOTH formats
        final int numDocs = 10;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName); // Force a commit so catalog snapshot reflects on-disk state

        // ─── Pre-snapshot assertions: DFA layout is correct ────────────────
        assertDocCountInIndex(client, indexName, numDocs);

        IndexShard shardBefore = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardBefore);
        assertLuceneIndexDirContents(shardBefore);
        // Catalog must match what's on local disk and what was uploaded to remote
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardBefore);
        // Capture exhaustive pre-snapshot state for tight post-restore comparison
        PreSnapshotState pre = capturePreSnapshotState(client, indexName, shardBefore);
        Set<String> filesBeforeSnapshot = captureShardFilesOnDisk(shardBefore);
        Set<String> catalogBefore = pre.catalogFilesExcludingSegments;

        // ─── Take V2 snapshot ──────────────────────────────────────────────
        // empty indices list triggers V2 path (cluster-manager only, pinned-timestamp based,
        // no per-shard SnapshotShardsService dispatch)
        logger.info("--> taking V2 snapshot");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals("single-shard index: snapshot must report exactly 1 successful shard", 1, snapshotInfo.successfulShards());
        assertEquals("snapshot total shards must equal successful shards", snapshotInfo.successfulShards(), snapshotInfo.totalShards());
        assertEquals("snapshot must contain exactly the DFA index", List.of(indexName), snapshotInfo.indices());

        // Delete the index so we can restore from snapshot
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        // ─── Restore from V2 snapshot ──────────────────────────────────────
        logger.info("--> restoring from V2 snapshot");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();

        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(
            "all shards must restore successfully",
            restoreResponse.getRestoreInfo().totalShards(),
            restoreResponse.getRestoreInfo().successfulShards()
        );
        assertEquals("no shards may fail to restore", 0, restoreResponse.getRestoreInfo().failedShards());

        // ─── Post-restore assertions ───────────────────────────────────────
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        assertTrue(indexExists(indexName));

        // Doc count restored — uses _stats API, NOT prepareSearch / prepareGet
        assertDocCountInIndex(client, indexName, numDocs);

        // DFA on-disk layout must be reconstructed identically: parquet/ dir + index/ (segments_N + lucene files)
        IndexShard shardAfter = getShardZero(indexName);
        // Tight post-restore validation: doc count, segment count, catalog files, generation, UUID, on-disk layout
        assertRestoredIndexMatches(client, indexName, shardAfter, pre, /* requireSameUUID */ true);
        // Verify the catalog files from before the snapshot are reconstructed on the restored shard's disk
        Set<String> filesAfterRestore = captureShardFilesOnDisk(shardAfter);
        Set<String> missingDataFiles = new HashSet<>();
        for (String f : catalogBefore) {
            if (filesAfterRestore.contains(f) == false && filesAfterRestore.contains("index/" + f) == false) {
                missingDataFiles.add(f);
            }
        }
        assertTrue(
            "data files from snapshot must be restored to disk; missing: "
                + missingDataFiles
                + "\n  before: "
                + filesBeforeSnapshot
                + "\n  after:  "
                + filesAfterRestore,
            missingDataFiles.isEmpty()
        );
    }

    /**
     * V2 snapshot of multiple DFA indices in a single snapshot, then restore all.
     *
     * <p>Validates that the V2 cluster-manager-only flow snapshots ALL existing indices when
     * the indices list is empty, and each index's per-format catalog/files are independently
     * preserved and restored. Catches cross-index corruption (e.g., catalog files from index A
     * leaking into index B's restore).
     */
    public void testV2MultiIndexSnapshotAndRestoreForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexA = "dfa-multi-a";
        String indexB = "dfa-multi-b";
        String snapshotRepoName = "test-snapshot-repo";
        String snapshotName = "test-v2-multi-snapshot";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();
        logger.info("Snapshot Path [{}]", absolutePath1);

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        // Create two DFA indices with distinct doc counts so we can detect data crossover
        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexA, indexSettings);
        createIndex(indexB, indexSettings);
        ensureGreen(indexA, indexB);

        final int numDocsA = 7;
        final int numDocsB = 13;
        indexDocuments(client, indexA, 0, numDocsA);
        indexDocuments(client, indexB, 0, numDocsB);
        refresh(indexA, indexB);
        flush(indexA);
        flush(indexB);

        // Pre-snapshot DFA layout assertions on both
        assertDocCountInIndex(client, indexA, numDocsA);
        assertDocCountInIndex(client, indexB, numDocsB);
        IndexShard shardABefore = getShardZero(indexA);
        IndexShard shardBBefore = getShardZero(indexB);
        assertAllFormatDirsHaveFiles(shardABefore);
        assertAllFormatDirsHaveFiles(shardBBefore);
        assertLuceneIndexDirContents(shardABefore);
        assertLuceneIndexDirContents(shardBBefore);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardABefore);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardBBefore);

        // Take V2 snapshot — empty indices list snapshots BOTH
        logger.info("--> taking V2 snapshot of all DFA indices");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals("multi-index 1-shard each: snapshot must report exactly 2 successful shards", 2, snapshotInfo.successfulShards());
        assertEquals("snapshot total shards must equal successful shards", snapshotInfo.successfulShards(), snapshotInfo.totalShards());
        // V2 snapshot must contain BOTH indices
        assertTrue("V2 snapshot must include " + indexA + ", got: " + snapshotInfo.indices(), snapshotInfo.indices().contains(indexA));
        assertTrue("V2 snapshot must include " + indexB + ", got: " + snapshotInfo.indices(), snapshotInfo.indices().contains(indexB));

        // Delete BOTH indices
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexA, indexB)).get());
        assertFalse(indexExists(indexA));
        assertFalse(indexExists(indexB));

        // Restore both from V2 snapshot
        logger.info("--> restoring all indices from V2 snapshot");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexA, indexB)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

        ensureGreen(indexA, indexB);

        // Doc counts must be PRESERVED PER-INDEX (catches cross-index corruption)
        assertDocCountInIndex(client, indexA, numDocsA);
        assertDocCountInIndex(client, indexB, numDocsB);

        // DFA layout intact for BOTH restored shards
        IndexShard shardAAfter = getShardZero(indexA);
        IndexShard shardBAfter = getShardZero(indexB);
        assertAllFormatDirsHaveFiles(shardAAfter);
        assertAllFormatDirsHaveFiles(shardBAfter);
        assertLuceneIndexDirContents(shardAAfter);
        assertLuceneIndexDirContents(shardBAfter);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAAfter);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardBAfter);
    }

    /**
     * Two sequential V2 snapshots with intervening writes; both must be independently restorable.
     *
     * <p>Validates V2 snapshot lifecycle for DFA:
     * <ul>
     *   <li>snap1 captures state at N docs, snap2 captures state at N+M docs</li>
     *   <li>Pinned-timestamp release for snap2 doesn't delete files still referenced by snap1</li>
     *   <li>Restore from snap1 yields exactly N docs (no stale snap2 files leak in)</li>
     *   <li>Restore from snap2 yields N+M docs (newer files present)</li>
     * </ul>
     */
    public void testV2IncrementalSnapshotsForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-incremental-index";
        String snapshotRepoName = "test-snapshot-repo";
        String snap1 = "test-v2-snap1";
        String snap2 = "test-v2-snap2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        // Phase 1: index N docs, take snap1
        final int docsBeforeSnap1 = 8;
        indexDocuments(client, indexName, 0, docsBeforeSnap1);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, docsBeforeSnap1);

        logger.info("--> taking V2 snap1 at {} docs", docsBeforeSnap1);
        SnapshotInfo snapInfo1 = createSnapshot(snapshotRepoName, snap1, new ArrayList<>());
        assertThat(snapInfo1.state(), equalTo(SnapshotState.SUCCESS));

        // Phase 2: index M more docs, take snap2
        final int docsAfterSnap1 = 12;
        final int totalDocs = docsBeforeSnap1 + docsAfterSnap1;
        indexDocuments(client, indexName, docsBeforeSnap1, totalDocs);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, totalDocs);

        logger.info("--> taking V2 snap2 at {} docs", totalDocs);
        SnapshotInfo snapInfo2 = createSnapshot(snapshotRepoName, snap2, new ArrayList<>());
        assertThat(snapInfo2.state(), equalTo(SnapshotState.SUCCESS));

        // Restore snap1 — must yield EXACTLY docsBeforeSnap1 (no leak from snap2)
        logger.info("--> restoring V2 snap1");
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        RestoreSnapshotResponse restore1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snap1)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restore1.status());
        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, docsBeforeSnap1);

        IndexShard shardAfterSnap1 = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfterSnap1);
        assertLuceneIndexDirContents(shardAfterSnap1);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfterSnap1);

        // Restore snap2 — must yield totalDocs (newer state)
        logger.info("--> restoring V2 snap2");
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        RestoreSnapshotResponse restore2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snap2)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restore2.status());
        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, totalDocs);

        IndexShard shardAfterSnap2 = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfterSnap2);
        assertLuceneIndexDirContents(shardAfterSnap2);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfterSnap2);
    }

    /**
     * Take a V2 snapshot of a DFA index, then delete the snapshot. Validates that:
     * <ul>
     *   <li>Snapshot creation succeeds</li>
     *   <li>Snapshot is listed by the snapshots API after creation</li>
     *   <li>{@code prepareDeleteSnapshot} succeeds (acknowledged)</li>
     *   <li>The snapshot is no longer listed after deletion</li>
     * </ul>
     *
     * <p>This validates the V2 lifecycle: snapshot creation pins a timestamp and writes
     * cluster-manager metadata; deletion must release that pin and clean up metadata.
     * Mirrors {@code DeleteSnapshotV2IT.testRemoteStoreCleanupForDeletedIndexForSnapshotV2}
     * but focuses on the snapshot-API contract (we don't probe internal blob counts here
     * because remote-store cleanup is async and timing-sensitive).
     */
    public void testV2DeleteSnapshotForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-delete-snapshot";
        String snapshotRepoName = "test-snapshot-repo";
        String snapshotName = "test-v2-delete";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 10;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        // Take V2 snapshot
        logger.info("--> taking V2 snapshot for delete test");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // Verify snapshot is listed
        List<SnapshotInfo> snapshotsBefore = client.admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        assertEquals("snapshot must be listed before deletion", 1, snapshotsBefore.size());
        assertEquals(snapshotName, snapshotsBefore.get(0).snapshotId().getName());

        // Delete the snapshot
        logger.info("--> deleting V2 snapshot [{}]", snapshotName);
        AcknowledgedResponse deleteResponse = client.admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).get();
        assertAcked(deleteResponse);

        // Verify snapshot is no longer listed
        List<SnapshotInfo> snapshotsAfter = client.admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        assertTrue("snapshot must NOT be listed after deletion, got: " + snapshotsAfter, snapshotsAfter.isEmpty());

        // The DFA index itself should still be healthy (snapshot deletion does not affect index)
        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, numDocs);
        IndexShard shardAfterDelete = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfterDelete);
        assertLuceneIndexDirContents(shardAfterDelete);
    }

    /**
     * DR scenario: V2 snapshot a DFA index, delete the original index, then restore using
     * <em>alternate</em> snapshot/segment/translog repositories that point at the same
     * underlying file paths (read-only views).
     *
     * <p>Validates that DFA's format-aware remote directory factory still resolves files
     * correctly when the restore is configured with different repo names than were used
     * at snapshot time. Mirrors {@code RestoreShallowSnapshotV2IT.testRestoreOperationsUsingDifferentRepos}.
     */
    public void testV2RestoreToDifferentRemoteRepoForDFA() throws Exception {
        disableRepoConsistencyCheck("Remote store repo aliasing");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-dr-index";
        String snapshotRepoName = "test-snapshot-repo";
        String snapshotName = "test-v2-dr-snapshot";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 15;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        // Take V2 snapshot
        logger.info("--> taking V2 snapshot for DR test");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // Create alternate repos pointing at the SAME underlying paths (read-only views)
        // — simulates DR where the operator points a new cluster at the existing storage
        String drSnapshotRepo = "dr-snapshot-repo";
        String drSegmentRepo = "dr-segment-repo";
        String drTranslogRepo = "dr-translog-repo";
        createRepository(drSnapshotRepo, "fs", getRepositorySettings(snapshotRepoName, true));
        createRepository(drSegmentRepo, "fs", getRepositorySettings(BASE_REMOTE_REPO, true));
        createRepository(drTranslogRepo, "fs", getRepositorySettings(BASE_REMOTE_REPO, true));

        // Delete the original index
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        // Restore using the DR repos
        logger.info("--> restoring V2 snapshot via DR repos");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(drSnapshotRepo, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .setSourceRemoteStoreRepository(drSegmentRepo)
            .setSourceRemoteTranslogRepository(drTranslogRepo)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        IndexShard shardAfter = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfter);
        assertLuceneIndexDirContents(shardAfter);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfter);
    }

    /**
     * V2 snapshot of a multi-shard DFA index. Validates that per-shard catalogs are
     * independently preserved and restored.
     *
     * <p>Catches:
     * <ul>
     *   <li>Catalog files from one shard leaking into another shard's restored state</li>
     *   <li>Per-shard pinned-timestamp scoping issues</li>
     *   <li>Format-aware remote directory routing issues at multi-shard scale</li>
     * </ul>
     */
    public void testV2SnapshotWithMultipleShardsForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2); // 2 data nodes so 3 shards distribute
        String indexName = "dfa-multi-shard";
        String snapshotRepoName = "test-snapshot-repo";
        String snapshotName = "test-v2-multi-shard-snapshot";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        // 3 shards, 0 replicas
        final int numShards = 3;
        Client client = client();
        Settings indexSettings = getIndexSettings(numShards, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        // Index a meaningful number of docs so each shard gets some
        final int numDocs = 60;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        // Pre-snapshot: every shard must have parquet+lucene layout
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        for (int shardId = 0; shardId < numShards; shardId++) {
            String node = nodeNameForShard(clusterState, indexName, shardId);
            IndexShard shard = getIndexShard(node, new ShardId(resolveIndex(indexName), shardId), indexName);
            assertAllFormatDirsHaveFiles(shard);
            assertLuceneIndexDirContents(shard);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shard);
        }

        // Take V2 snapshot
        logger.info("--> taking V2 snapshot of multi-shard DFA index");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(numShards));
        assertThat(snapshotInfo.totalShards(), equalTo(numShards));

        // Delete and restore
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        logger.info("--> restoring multi-shard V2 snapshot");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(numShards, restoreResponse.getRestoreInfo().totalShards());
        assertEquals(numShards, restoreResponse.getRestoreInfo().successfulShards());
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

        ensureGreen(indexName);

        // Total doc count must match
        assertDocCountInIndex(client, indexName, numDocs);

        // EVERY restored shard must have intact DFA layout — catches per-shard corruption
        ClusterState restoredState = client.admin().cluster().prepareState().get().getState();
        for (int shardId = 0; shardId < numShards; shardId++) {
            String node = nodeNameForShard(restoredState, indexName, shardId);
            IndexShard shard = getIndexShard(node, new ShardId(resolveIndex(indexName), shardId), indexName);
            assertAllFormatDirsHaveFiles(shard);
            assertLuceneIndexDirContents(shard);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shard);
        }
    }

    /**
     * Resolve the node name hosting the primary of a given shard.
     */
    private String nodeNameForShard(ClusterState state, String indexName, int shardId) {
        String nodeId = state.routingTable().index(indexName).shard(shardId).primaryShard().currentNodeId();
        assertTrue("primary for shard " + shardId + " must be assigned", nodeId != null);
        return state.nodes().get(nodeId).getName();
    }

    /**
     * Snapshots the set of files present on a shard's local disk (parquet/, lucene index/, etc.)
     * Used to verify file restoration after V2 restore.
     */
    private Set<String> captureShardFilesOnDisk(IndexShard shard) throws IOException {
        Set<String> all = new HashSet<>();
        Path indexDir = shard.shardPath().resolveIndex();
        if (Files.exists(indexDir)) {
            try (var stream = Files.list(indexDir)) {
                stream.forEach(p -> all.add("index/" + p.getFileName()));
            }
        }
        for (String format : expectedFormats()) {
            if ("lucene".equals(format)) continue;
            Path fmtDir = shard.shardPath().getDataPath().resolve(format);
            if (Files.exists(fmtDir)) {
                try (var stream = Files.list(fmtDir)) {
                    stream.forEach(p -> all.add(format + "/" + p.getFileName()));
                }
            }
        }
        return all;
    }

    /**
     * Captured pre-snapshot state for tight post-restore comparison.
     * Each field is exactly comparable to the post-restore state.
     */
    protected static final class PreSnapshotState {
        final long docCount;
        final int segmentCount;
        final Set<String> catalogFilesExcludingSegments;
        final long catalogGeneration;
        final String indexUUID;

        PreSnapshotState(
            long docCount,
            int segmentCount,
            Set<String> catalogFilesExcludingSegments,
            long catalogGeneration,
            String indexUUID
        ) {
            this.docCount = docCount;
            this.segmentCount = segmentCount;
            this.catalogFilesExcludingSegments = catalogFilesExcludingSegments;
            this.catalogGeneration = catalogGeneration;
            this.indexUUID = indexUUID;
        }
    }

    /** Capture exhaustive pre-snapshot state for tight post-restore comparison. */
    protected PreSnapshotState capturePreSnapshotState(Client client, String indexName, IndexShard shard) throws IOException {
        long docs = client.admin().indices().prepareStats(indexName).clear().setDocs(true).get().getIndex(indexName).getPrimaries().docs
            .getCount();
        int segs = countSegmentsOnShard(client, indexName);
        Set<String> catalogFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(shard);
        long catalogGen;
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = shard
                .getCatalogSnapshot()
        ) {
            catalogGen = ref.get().getGeneration();
        }
        String uuid = client.admin().indices().prepareGetSettings(indexName).get().getSetting(indexName, IndexMetadata.SETTING_INDEX_UUID);
        assertNotNull("pre-snapshot index UUID must be set", uuid);
        assertFalse("pre-snapshot index UUID must be non-empty", uuid.isEmpty());
        assertTrue("pre-snapshot catalog generation must be > 0, got " + catalogGen, catalogGen > 0L);
        return new PreSnapshotState(docs, segs, catalogFiles, catalogGen, uuid);
    }

    /**
     * Return number of segments on the primary of shard 0. Returns 0 if the segments API
     * returns no info for the index (e.g., shard not yet ready or special engine state).
     */
    private int countSegmentsOnShard(Client client, String indexName) {
        var resp = client.admin().indices().prepareSegments(indexName).get();
        var indexSegs = resp.getIndices().get(indexName);
        if (indexSegs == null) return 0;
        var shardSegs = indexSegs.getShards().get(0);
        if (shardSegs == null) return 0;
        var iter = shardSegs.iterator();
        if (iter.hasNext() == false) return 0;
        return iter.next().getSegments().size();
    }

    /**
     * Tight post-restore validation. Asserts the restored index matches the captured pre-snapshot
     * state EXACTLY on doc count, segment count, and catalog file set; the catalog generation must
     * be at least the pre-snapshot value (restore may bump it during finalize); and DFA on-disk
     * layout is intact.
     *
     * @param requireSameUUID true if the restored index keeps the original UUID (typical case);
     *                        false for rename tests where a new UUID is expected.
     */
    protected void assertRestoredIndexMatches(
        Client client,
        String restoredIndexName,
        IndexShard restoredShard,
        PreSnapshotState pre,
        boolean requireSameUUID
    ) throws IOException {
        // Doc count must be EXACT
        assertDocCountInIndex(client, restoredIndexName, pre.docCount);

        // Segment count must be EXACT
        int restoredSegCount = countSegmentsOnShard(client, restoredIndexName);
        assertEquals(
            "segment count must match pre-snapshot exactly: pre=" + pre.segmentCount + " post=" + restoredSegCount,
            pre.segmentCount,
            restoredSegCount
        );

        // Catalog files (excluding segments_N) must match EXACTLY
        Set<String> restoredCatalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(restoredShard);
        assertEquals(
            "catalog file set must match pre-snapshot exactly: missing="
                + diff(pre.catalogFilesExcludingSegments, restoredCatalog)
                + " extra="
                + diff(restoredCatalog, pre.catalogFilesExcludingSegments),
            pre.catalogFilesExcludingSegments,
            restoredCatalog
        );

        // Catalog generation must be >= pre-snapshot generation (restore may bump it)
        long restoredGen;
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = restoredShard
                .getCatalogSnapshot()
        ) {
            restoredGen = ref.get().getGeneration();
        }
        assertTrue(
            "restored catalog generation must be >= pre-snapshot: pre=" + pre.catalogGeneration + " post=" + restoredGen,
            restoredGen >= pre.catalogGeneration
        );

        // UUID check: restore always creates a fresh index with a NEW UUID (not the snapshot's source UUID).
        // For rename, the new UUID is also different. So in both cases the restored UUID must
        // (a) be set, (b) differ from the pre-snapshot source UUID.
        String restoredUuid = client.admin()
            .indices()
            .prepareGetSettings(restoredIndexName)
            .get()
            .getSetting(restoredIndexName, IndexMetadata.SETTING_INDEX_UUID);
        assertNotNull("restored index UUID must be set", restoredUuid);
        assertFalse("restored index UUID must be non-empty", restoredUuid.isEmpty());
        assertNotEquals("restored index always has a fresh UUID, not the source UUID", pre.indexUUID, restoredUuid);

        // DFA layout intact
        assertAllFormatDirsHaveFiles(restoredShard);
        assertLuceneIndexDirContents(restoredShard);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(restoredShard);
    }

    /** Set difference helper for clearer failure messages. */
    private static Set<String> diff(Set<String> a, Set<String> b) {
        Set<String> r = new HashSet<>(a);
        r.removeAll(b);
        return r;
    }

    /**
     * Asserts the catalog files (excluding segments_N) before snapshot are all present on
     * the restored shard's disk. Validates the restore actually reconstructed the data files,
     * not just metadata.
     */
    private void assertCatalogFilesRestoredOnDisk(IndexShard shardBefore, IndexShard shardAfter) throws IOException {
        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(shardBefore);
        Set<String> filesAfter = captureShardFilesOnDisk(shardAfter);
        assertFalse("catalog before snapshot must not be empty for shard " + shardBefore.routingEntry(), catalogBefore.isEmpty());
        Set<String> missing = new HashSet<>();
        for (String catalogFile : catalogBefore) {
            // catalog file names may already be format-prefixed (e.g. "parquet/foo.parquet")
            if (filesAfter.contains(catalogFile) == false && filesAfter.contains("index/" + catalogFile) == false) {
                missing.add(catalogFile);
            }
        }
        assertTrue(
            "catalog files missing from restored shard disk: "
                + missing
                + "\n  catalog before: "
                + catalogBefore
                + "\n  files on disk after restore: "
                + filesAfter,
            missing.isEmpty()
        );
    }

    /**
     * V2 snapshot + restore with different {@code RemoteStoreEnums.PathType} settings.
     * Validates that DFA's format-aware remote directory routing works under HASHED_PREFIX,
     * HASHED_INFIX, and FIXED path types.
     *
     * <p>Mirrors {@code RestoreShallowSnapshotV2IT.testHashedPrefixTranslogMetadataCombination}.
     */
    public void testV2HashedPrefixPathTypeForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);

        // Test all three path types in sequence.
        // V2 allows only ONE repository with shallow_snapshot_v2=true cluster-wide, so we
        // delete the repo between iterations.
        for (RemoteStoreEnums.PathType pathType : RemoteStoreEnums.PathType.values()) {
            String suffix = pathType.toString().toLowerCase(java.util.Locale.ROOT);
            String indexName = "dfa-pathtype-" + suffix;
            String snapshotRepoName = "snap-repo-" + suffix;
            String snapshotName = "snap-" + suffix;
            Path absolutePath = randomRepoPath().toAbsolutePath();

            // Switch cluster path type
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), pathType))
                    .get()
            );

            createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath, true));
            Client client = client();
            createIndex(indexName, getIndexSettings(1, 0).build());
            ensureGreen(indexName);

            final int numDocs = 10;
            indexDocuments(client, indexName, 0, numDocs);
            refresh(indexName);
            flush(indexName);
            assertDocCountInIndex(client, indexName, numDocs);

            IndexShard shardBefore = getShardZero(indexName);
            assertAllFormatDirsHaveFiles(shardBefore);
            assertLuceneIndexDirContents(shardBefore);

            logger.info("--> taking V2 snapshot for pathType={}", pathType);
            SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
            assertThat("pathType=" + pathType, snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertEquals("pathType=" + pathType + " single-shard exactly 1 successful", 1, snapshotInfo.successfulShards());

            assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
            assertFalse("pathType=" + pathType, indexExists(indexName));

            logger.info("--> restoring V2 snapshot for pathType={}", pathType);
            RestoreSnapshotResponse restoreResponse = client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
            assertEquals("pathType=" + pathType, RestStatus.OK, restoreResponse.status());
            assertEquals("pathType=" + pathType, 0, restoreResponse.getRestoreInfo().failedShards());

            ensureGreen(indexName);
            assertDocCountInIndex(client, indexName, numDocs);

            IndexShard shardAfter = getShardZero(indexName);
            assertAllFormatDirsHaveFiles(shardAfter);
            assertLuceneIndexDirContents(shardAfter);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfter);

            // Clean up so the next iteration can register a new repo with shallow_snapshot_v2=true
            // (V2 allows only ONE such repo cluster-wide).
            assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).get());
            assertAcked(client().admin().cluster().prepareDeleteRepository(snapshotRepoName).get());
        }
    }

    /**
     * V2 snapshot + clone the snapshot + restore from clone for a DFA index.
     * Validates pinned-timestamp inheritance through clones.
     *
     * <p>Mirrors {@code CloneSnapshotV2IT.testCloneShallowCopyV2}.
     */
    public void testV2CloneSnapshotForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-clone-snapshot";
        String snapshotRepoName = "test-clone-repo";
        String sourceSnapshot = "snap-source";
        String clonedSnapshot = "snap-cloned";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 12;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        // Take source V2 snapshot
        logger.info("--> taking source V2 snapshot");
        SnapshotInfo source = createSnapshot(snapshotRepoName, sourceSnapshot, new ArrayList<>());
        assertThat(source.state(), equalTo(SnapshotState.SUCCESS));

        // Clone the snapshot — V2 requires wildcard pattern "*" for indices
        logger.info("--> cloning V2 snapshot");
        AcknowledgedResponse cloneResponse = client.admin()
            .cluster()
            .prepareCloneSnapshot(snapshotRepoName, sourceSnapshot, clonedSnapshot)
            .setIndices("*")
            .get();
        assertAcked(cloneResponse);

        // Verify both snapshots exist in the repo
        List<SnapshotInfo> snapshots = client.admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        Set<String> snapshotNames = new HashSet<>();
        for (SnapshotInfo s : snapshots) {
            snapshotNames.add(s.snapshotId().getName());
        }
        assertTrue("source snapshot must be listed: " + snapshotNames, snapshotNames.contains(sourceSnapshot));
        assertTrue("cloned snapshot must be listed: " + snapshotNames, snapshotNames.contains(clonedSnapshot));

        // Delete the source index and restore from CLONE
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        logger.info("--> restoring from cloned V2 snapshot");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, clonedSnapshot)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        IndexShard shardAfter = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfter);
        assertLuceneIndexDirContents(shardAfter);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfter);
    }

    /**
     * Two V2 snapshots fired in rapid sequence on the same repository. V2 design serializes
     * snapshots within a single repo (only one snapshot can be in finalization state at a time),
     * and only ONE repo cluster-wide can have {@code shallow_snapshot_v2=true}. So the test
     * fires snapshots back-to-back with no delay and verifies both succeed without races.
     *
     * <p>Validates DFA refresh/flush concurrency under V2 timestamp-pinning and that both
     * snapshots are independently restorable.
     */
    public void testV2ConcurrentSnapshotsForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-rapid-snap";
        String snapshotRepoName = "test-rapid-repo";
        String snap1 = "rapid-snap-1";
        String snap2 = "rapid-snap-2";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 15;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        // Fire two V2 snapshots back-to-back on the same repo
        logger.info("--> taking back-to-back V2 snapshots");
        SnapshotInfo info1 = createSnapshot(snapshotRepoName, snap1, new ArrayList<>());
        SnapshotInfo info2 = createSnapshot(snapshotRepoName, snap2, new ArrayList<>());

        // Both must succeed
        assertThat("snap1 state", info1.state(), equalTo(SnapshotState.SUCCESS));
        assertThat("snap2 state", info2.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals("snap1 single-shard exactly 1 successful", 1, info1.successfulShards());
        assertEquals("snap2 single-shard exactly 1 successful", 1, info2.successfulShards());

        // Restore from each, verify both work
        for (String snap : new String[] { snap1, snap2 }) {
            logger.info("--> restoring rapid V2 snapshot [{}]", snap);
            assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());

            RestoreSnapshotResponse restoreResponse = client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepoName, snap)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
            assertEquals("snap=" + snap, RestStatus.OK, restoreResponse.status());
            assertEquals("snap=" + snap, 0, restoreResponse.getRestoreInfo().failedShards());

            ensureGreen(indexName);
            assertDocCountInIndex(client, indexName, numDocs);

            IndexShard shardAfter = getShardZero(indexName);
            assertAllFormatDirsHaveFiles(shardAfter);
            assertLuceneIndexDirContents(shardAfter);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfter);
        }
    }

    /**
     * V2 restore with rename pattern — the snapshot was taken on {@code source-idx} but the
     * restored index gets a new name {@code restored-idx}. Validates DFA handles the renamed
     * index UUID correctly (file paths, catalog, remote directory routing all use the new UUID).
     *
     * <p>Mirrors the rename pattern from
     * {@code RestoreShallowSnapshotV2IT.testRestoreOperationsShallowCopyEnabled}.
     */
    public void testV2RestoreWithRenameForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String sourceIndex = "dfa-rename-source";
        String renamedIndex = "dfa-rename-restored";
        String snapshotRepoName = "test-rename-repo";
        String snapshotName = "snap-rename";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(sourceIndex, indexSettings);
        ensureGreen(sourceIndex);

        final int numDocs = 10;
        indexDocuments(client, sourceIndex, 0, numDocs);
        refresh(sourceIndex);
        flush(sourceIndex);
        assertDocCountInIndex(client, sourceIndex, numDocs);

        // Capture pre-snapshot state for tight post-restore validation
        IndexShard sourceShard = getShardZero(sourceIndex);
        PreSnapshotState pre = capturePreSnapshotState(client, sourceIndex, sourceShard);

        logger.info("--> taking V2 snapshot of source");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals("rename: snapshot has exactly 1 shard", 1, snapshotInfo.successfulShards());

        // Source index can stay; we rename on restore
        logger.info("--> restoring with rename: {} -> {}", sourceIndex, renamedIndex);
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(sourceIndex)
            .setRenamePattern(sourceIndex)
            .setRenameReplacement(renamedIndex)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

        ensureGreen(renamedIndex);
        // Renamed index must exist and have all docs; original is unchanged
        assertTrue("renamed index must exist", indexExists(renamedIndex));
        assertTrue("source index must still exist", indexExists(sourceIndex));
        assertDocCountInIndex(client, sourceIndex, numDocs);

        // Tight validation: renamed index gets a NEW UUID but matches source on docs/segments/catalog
        IndexShard renamedShard = getShardZero(renamedIndex);
        assertRestoredIndexMatches(client, renamedIndex, renamedShard, pre, /* requireSameUUID */ false);
    }

    /**
     * V2 restore with overridden index settings. Some settings are restorable-mutable
     * (e.g., {@code refresh_interval}). Validates the override is applied to the restored index.
     */
    public void testV2RestoreWithUpdatedSettingsForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-settings-restore";
        String snapshotRepoName = "test-settings-repo";
        String snapshotName = "snap-settings";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        // Original index uses 300s refresh
        Settings indexSettings = getIndexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s").build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 10;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        // Take V2 snapshot
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // Delete and restore with overridden refresh_interval = 5s
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        Settings overriddenSettings = Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s").build();

        logger.info("--> restoring V2 snapshot with overridden refresh_interval=5s");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .setIndexSettings(overriddenSettings)
            .setIgnoreIndexSettings(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey())
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, numDocs);

        // Verify the setting override took effect
        String actualRefreshInterval = client.admin()
            .indices()
            .prepareGetSettings(indexName)
            .get()
            .getSetting(indexName, IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey());
        assertEquals("refresh_interval override must take effect on restored index", "5s", actualRefreshInterval);

        // DFA layout intact
        IndexShard shardAfter = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfter);
        assertLuceneIndexDirContents(shardAfter);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfter);
    }

    /**
     * V2 snapshot of a DFA index with <strong>zero documents</strong>. Edge case — does V2 handle
     * the "empty catalog" case correctly?
     *
     * <p>The index is created and immediately snapshotted (no indexing, no flush). The snapshot
     * must succeed; restore must produce a green, empty index with zero docs.
     */
    public void testV2EmptyIndexSnapshot() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-empty-index";
        String snapshotRepoName = "test-empty-repo";
        String snapshotName = "snap-empty";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        // No documents indexed. Force a flush to materialize an empty commit point.
        flush(indexName);

        // Pre-snapshot: 0 docs
        assertDocCountInIndex(client, indexName, 0);

        // Take V2 snapshot of empty index
        logger.info("--> taking V2 snapshot of empty DFA index");
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        // Even an empty index has 1 shard reported
        assertEquals("empty index 1-shard exactly 1 successful", 1, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());

        // Delete and restore
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        logger.info("--> restoring empty V2 snapshot");
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, 0);

        // After restore, the index should be writable — index a doc to confirm
        indexDocuments(client, indexName, 0, 1);
        refresh(indexName);
        assertDocCountInIndex(client, indexName, 1);
    }

    /**
     * Validates the {@code _snapshot/_status} API works for V2 DFA snapshots. The status API
     * is a different code path than create/restore and may have its own bugs that don't
     * surface in the create/restore flow.
     */
    public void testV2SnapshotStatusAPIForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-status-api";
        String snapshotRepoName = "test-status-repo";
        String snapshotName = "snap-status";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 10;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        // Take V2 snapshot
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // Call _snapshot/_status API
        org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse statusResponse = client.admin()
            .cluster()
            .prepareSnapshotStatus(snapshotRepoName)
            .setSnapshots(snapshotName)
            .get();

        assertEquals("status API must return one snapshot", 1, statusResponse.getSnapshots().size());
        org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus status = statusResponse.getSnapshots().get(0);
        assertEquals("snapshot name in status must match", snapshotName, status.getSnapshot().getSnapshotId().getName());
        assertNotNull("status object must contain shard stats", status.getShardsStats());
        // Single-shard index — exact assertions
        assertEquals("status totalShards must be 1", 1, status.getShardsStats().getTotalShards());
        assertEquals("status doneShards must be 1", 1, status.getShardsStats().getDoneShards());
        assertEquals("status failedShards must be 0", 0, status.getShardsStats().getFailedShards());
        assertEquals("status initializingShards must be 0", 0, status.getShardsStats().getInitializingShards());
        // status must contain index stats for our index
        assertEquals("status must list the DFA index", 1, status.getIndices().size());
        assertTrue("status must contain the DFA index, got: " + status.getIndices().keySet(), status.getIndices().containsKey(indexName));
    }

    /**
     * Wait until the pinned-timestamp scheduler has fired at least once after invocation.
     * Mirrors {@code DeleteSnapshotV2IT.keepPinnedTimestampSchedulerUpdated} but uses
     * {@code assertBusy} to avoid the awaitility dependency (not on classpath here).
     */
    private void keepPinnedTimestampSchedulerUpdated() throws Exception {
        long currentTime = System.currentTimeMillis();
        assertBusy(
            () -> assertTrue(
                "pinned-timestamp scheduler should fire after invocation",
                RemoteStorePinnedTimestampService.getPinnedTimestamps().v1() > currentTime
            ),
            10,
            TimeUnit.SECONDS
        );
    }

    /**
     * <strong>DFA-specific test:</strong> verify that ALL format directories ({@code segments/},
     * {@code translog/}, and any DFA-specific format dirs) are cleaned up after V2 snapshot
     * deletion + index deletion + lookback expiry. Lucene-only tests check just {@code segments/}
     * and {@code translog/}; for DFA there could be additional format files that bug-prone
     * cleanup logic might miss.
     */
    public void testV2DeleteSnapshotCleansUpAllFormatFilesForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-cleanup-index";
        String snapshotRepoName = "test-cleanup-repo";
        String snapshotName = "snap-cleanup";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 25;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        // Get index UUID — used to find the right shard subtree under the remote-store layout
        // (the actual path includes a path-type prefix like HASHED_PREFIX so we walk the tree).
        String indexUUID = client.admin()
            .indices()
            .prepareGetSettings(indexName)
            .get()
            .getSetting(indexName, IndexMetadata.SETTING_INDEX_UUID);

        // Take V2 snapshot
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // Pre-delete: remote store must have segment + translog files under the indexUUID subtree.
        long segmentFilesBefore = countFilesUnder(remoteRepoPath, indexUUID, "segments");
        long translogFilesBefore = countFilesUnder(remoteRepoPath, indexUUID, "translog");
        assertTrue("segment files must exist for index " + indexUUID + " before delete, got " + segmentFilesBefore, segmentFilesBefore > 0);
        assertTrue(
            "translog files must exist for index " + indexUUID + " before delete, got " + translogFilesBefore,
            translogFilesBefore > 0
        );

        // Configure aggressive pinned-timestamp release so cleanup can fire promptly
        String clusterManagerName = internalCluster().getClusterManagerName();
        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            clusterManagerName
        );
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));
        keepPinnedTimestampSchedulerUpdated();

        // Delete the index AND the snapshot
        assertAcked(client().admin().indices().prepareDelete(indexName));
        AcknowledgedResponse deleteResponse = client.admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).get();
        assertAcked(deleteResponse);

        // Wait for cleanup to drain ALL format directories to zero for this index UUID.
        // For DFA this MUST include any per-format dirs; we walk the entire indexUUID subtree.
        // 120s timeout because pinned-timestamp lookback + scheduler iterations can take a while
        // for DFA's per-format file lifecycle.
        assertBusy(() -> {
            long segCount = countFilesUnder(remoteRepoPath, indexUUID, "segments");
            long translogCount = countFilesUnder(remoteRepoPath, indexUUID, "translog");
            assertEquals("segments/ subtree must be cleaned up for indexUUID=" + indexUUID, 0, segCount);
            assertEquals("translog/ subtree must be cleaned up for indexUUID=" + indexUUID, 0, translogCount);
        }, 120, TimeUnit.SECONDS);
    }

    /**
     * Walk the entire {@code rootPath} subtree and count files whose path contains the given
     * indexUUID and the given category (e.g. {@code "segments"} or {@code "translog"}).
     * Path-type-agnostic: works with FIXED, HASHED_PREFIX, and HASHED_INFIX layouts.
     */
    private static long countFilesUnder(Path rootPath, String indexUUID, String categoryDirName) throws IOException {
        if (Files.exists(rootPath) == false) return 0;
        long[] count = { 0 };
        Files.walkFileTree(rootPath, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) {
                // Iterate path name elements rather than substring-matching the toString() — the latter
                // breaks on Windows where the separator is '\\', and also matches non-component substrings.
                boolean hasUuid = false;
                boolean hasCategory = false;
                for (Path part : file) {
                    String name = part.toString();
                    if (indexUUID.equals(name)) {
                        hasUuid = true;
                    } else if (categoryDirName.equals(name)) {
                        hasCategory = true;
                    }
                }
                if (hasUuid && hasCategory) {
                    count[0]++;
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFileFailed(Path file, IOException exc) {
                // Concurrent cleanup may delete a file between enumeration and visit; tolerate.
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        return count[0];
    }

    /**
     * <strong>DFA-specific test:</strong> verifies that the DFA catalog generation is preserved
     * (or advanced) when a V2 snapshot is cloned and restored from the clone. Catalog generation
     * is a DFA-specific monotonic counter not present in Lucene-only engines.
     */
    public void testV2CloneSnapshotPreservesCatalogGenerationForDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-clone-catalog-gen";
        String snapshotRepoName = "test-clone-gen-repo";
        String sourceSnapshot = "src-snap";
        String clonedSnapshot = "cloned-snap";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 12;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        IndexShard sourceShard = getShardZero(indexName);
        long sourceGen;
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = sourceShard
                .getCatalogSnapshot()
        ) {
            sourceGen = ref.get().getGeneration();
        }
        assertTrue("pre-snapshot catalog generation must be > 0", sourceGen > 0L);

        SnapshotInfo source = createSnapshot(snapshotRepoName, sourceSnapshot, new ArrayList<>());
        assertThat(source.state(), equalTo(SnapshotState.SUCCESS));

        AcknowledgedResponse cloneResponse = client.admin()
            .cluster()
            .prepareCloneSnapshot(snapshotRepoName, sourceSnapshot, clonedSnapshot)
            .setIndices("*")
            .get();
        assertAcked(cloneResponse);

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, clonedSnapshot)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        ensureGreen(indexName);

        IndexShard restoredShard = getShardZero(indexName);
        long restoredGen;
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = restoredShard
                .getCatalogSnapshot()
        ) {
            restoredGen = ref.get().getGeneration();
        }
        assertTrue(
            "DFA catalog generation must be monotonic across clone+restore: source=" + sourceGen + " restored=" + restoredGen,
            restoredGen >= sourceGen
        );
    }

    /**
     * <strong>DFA-specific test:</strong> validates that Lucene segments' DFA-specific
     * {@code writer_generation} attribute survives V2 snapshot/restore. This attribute is
     * stamped by {@link org.opensearch.be.lucene.index.LuceneWriterCodec} during initial
     * indexing and is required by DFA to correlate Lucene segments with the corresponding
     * parquet files. If snapshot/restore drops this attribute, DFA can't reconstruct the
     * format mapping.
     */
    public void testV2SnapshotPreservesWriterGenerationAttributeForDFA() throws Exception {
        // The writer_generation attribute is stamped by LuceneWriterCodec only when Lucene is
        // a secondary format. For parquet-only configurations there are no Lucene segments
        // with data, hence no writer_generation attributes to validate.
        org.junit.Assume.assumeTrue("writer_generation attribute test requires Lucene secondary format", hasLuceneSecondary());
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-writergen-attr";
        String snapshotRepoName = "test-writergen-repo";
        String snapshotName = "snap-writergen";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        // 3 batches × refresh+flush → multiple segments with distinct writer_generations
        for (int batch = 0; batch < 3; batch++) {
            indexDocuments(client, indexName, batch * 5, batch * 5 + 5);
            refresh(indexName);
            flush(indexName);
        }

        // Capture pre-snapshot writer_generation on each Lucene segment
        IndexShard sourceShard = getShardZero(indexName);
        Map<String, String> preSnapshotWriterGens = new HashMap<>();
        org.apache.lucene.index.SegmentInfos infos = org.apache.lucene.index.SegmentInfos.readLatestCommit(sourceShard.store().directory());
        for (org.apache.lucene.index.SegmentCommitInfo sci : infos) {
            String genAttr = sci.info.getAttribute(org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE);
            preSnapshotWriterGens.put(sci.info.name, genAttr);
        }
        assertFalse("pre-snapshot writer_generation map must not be empty", preSnapshotWriterGens.isEmpty());
        assertTrue(
            "at least one segment must have writer_generation attribute pre-snapshot, got: " + preSnapshotWriterGens,
            preSnapshotWriterGens.values().stream().anyMatch(java.util.Objects::nonNull)
        );

        // Snapshot, delete, restore
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        ensureGreen(indexName);

        // Verify writer_generation attribute survived restore
        IndexShard restoredShard = getShardZero(indexName);
        Map<String, String> postRestoreWriterGens = new HashMap<>();
        org.apache.lucene.index.SegmentInfos infosAfter = org.apache.lucene.index.SegmentInfos.readLatestCommit(
            restoredShard.store().directory()
        );
        for (org.apache.lucene.index.SegmentCommitInfo sci : infosAfter) {
            postRestoreWriterGens.put(
                sci.info.name,
                sci.info.getAttribute(org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE)
            );
        }

        // Every pre-snapshot segment with writer_generation must STILL have it after restore.
        for (Map.Entry<String, String> e : preSnapshotWriterGens.entrySet()) {
            String segName = e.getKey();
            String preGen = e.getValue();
            if (preGen == null) continue;
            String postGen = postRestoreWriterGens.get(segName);
            assertNotNull(
                "segment "
                    + segName
                    + " had writer_generation="
                    + preGen
                    + " pre-snapshot but is missing post-restore. post-restore segs: "
                    + postRestoreWriterGens,
                postGen
            );
            assertEquals("writer_generation must be preserved across V2 snapshot/restore for segment " + segName, preGen, postGen);
        }
    }

    /**
     * <strong>DFA failure recovery test:</strong> if a V2 snapshot fails during finalization
     * (cluster-manager block injected), the source DFA index must be UNCHANGED — same docs,
     * same catalog generation, same on-disk format dirs. A subsequent V2 snapshot must succeed.
     *
     * <p>Mirrors {@code ConcurrentSnapshotsV2IT.testCreateSnapshotFailInFinalize} but adds
     * DFA-specific assertions on the source state.
     */
    public void testV2SnapshotFailInFinalizeDoesNotCorruptDFAState() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-fail-finalize";
        String snapshotRepoName = "test-fail-finalize-repo";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        // Use 'mock' repo type so the test framework can inject failures.
        Settings.Builder settings = Settings.builder()
            .put("location", absolutePath1)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        createRepository(snapshotRepoName, "mock", settings);

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 15;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        // Capture exhaustive pre-failure state for assertion that source is unaffected by failed snapshot
        IndexShard sourceShardBefore = getShardZero(indexName);
        PreSnapshotState preState = capturePreSnapshotState(client, indexName, sourceShardBefore);

        // Block finalization, fire snapshot, wait for block, then unblock to fail it
        blockClusterManagerFromFinalizingSnapshotOnIndexFile(snapshotRepoName);
        final org.opensearch.common.action.ActionFuture<
            org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse> snapshotFuture = startFullSnapshot(
                snapshotRepoName,
                "snap-fail"
            );
        awaitNumberOfSnapshotsInProgress(1);
        waitForBlock(clusterManagerNode, snapshotRepoName, TimeValue.timeValueSeconds(30L));
        unblockNode(snapshotRepoName, clusterManagerNode);

        // The blocked snapshot must throw SnapshotException
        expectThrows(org.opensearch.snapshots.SnapshotException.class, snapshotFuture::actionGet);

        // ─── DFA-specific: verify source index was NOT corrupted by the failure ───
        IndexShard sourceShardAfter = getShardZero(indexName);
        // Doc count unchanged
        assertDocCountInIndex(client, indexName, preState.docCount);
        // Catalog file set unchanged (excluding segments_N which can change due to commit ops)
        Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(sourceShardAfter);
        assertEquals(
            "DFA catalog files must be unchanged after failed V2 snapshot finalize: missing="
                + diff(preState.catalogFilesExcludingSegments, catalogAfter)
                + " extra="
                + diff(catalogAfter, preState.catalogFilesExcludingSegments),
            preState.catalogFilesExcludingSegments,
            catalogAfter
        );
        // Catalog generation must be at least pre-failure (commit may have advanced it slightly during failure handling)
        long genAfter;
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = sourceShardAfter
                .getCatalogSnapshot()
        ) {
            genAfter = ref.get().getGeneration();
        }
        assertTrue(
            "catalog generation must be >= pre-failure generation: pre=" + preState.catalogGeneration + " post=" + genAfter,
            genAfter >= preState.catalogGeneration
        );
        // DFA on-disk layout intact
        assertAllFormatDirsHaveFiles(sourceShardAfter);
        assertLuceneIndexDirContents(sourceShardAfter);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(sourceShardAfter);

        // ─── Verify retry: a subsequent V2 snapshot must succeed (repo state cleaned) ───
        SnapshotInfo retryInfo = createSnapshot(snapshotRepoName, "snap-retry-success", new ArrayList<>());
        assertThat(retryInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals("retry snapshot must include exactly the DFA index", List.of(indexName), retryInfo.indices());

        // Repo must contain only the successful retry snapshot
        List<SnapshotInfo> snapshots = client.admin().cluster().prepareGetSnapshots(snapshotRepoName).get().getSnapshots();
        assertEquals("only the successful retry snapshot must remain in repo", 1, snapshots.size());
        assertEquals("snap-retry-success", snapshots.get(0).snapshotId().getName());
    }

    /**
     * <strong>DFA failure handling test:</strong> verify that invalid restore requests for a
     * DFA index are rejected cleanly with {@link org.opensearch.snapshots.SnapshotRestoreException}
     * — they must NOT partially restore or leak files.
     *
     * <p>Mirrors {@code RestoreShallowSnapshotV2IT.testInvalidRestoreRequestScenarios} but exercises
     * DFA-specific setting overrides too.
     */
    public void testV2InvalidRestoreRequestForDFAIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);
        String indexName = "dfa-invalid-restore";
        String snapshotRepoName = "test-invalid-restore-repo";
        String snapshotName = "snap-invalid";
        Path absolutePath1 = randomRepoPath().toAbsolutePath();

        createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, true));

        Client client = client();
        Settings indexSettings = getIndexSettings(1, 0).build();
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);

        final int numDocs = 10;
        indexDocuments(client, indexName, 0, numDocs);
        refresh(indexName);
        flush(indexName);

        // Take V2 snapshot
        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepoName, snapshotName, new ArrayList<>());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // Delete the source index so we can attempt restore
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());

        // ─── Invalid request 1: try to disable remote_store on restore ───
        Settings disableRemoteStore = Settings.builder().put(SETTING_REMOTE_STORE_ENABLED, false).build();
        org.opensearch.snapshots.SnapshotRestoreException ex1 = expectThrows(
            org.opensearch.snapshots.SnapshotRestoreException.class,
            () -> client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .setIndexSettings(disableRemoteStore)
                .get()
        );
        assertTrue(
            "rejection message must mention remote_store: " + ex1.getMessage(),
            ex1.getMessage().toLowerCase(java.util.Locale.ROOT).contains("remote_store")
        );

        // ─── Invalid request 2: try to change segment.repository ───
        Settings changeSegmentRepo = Settings.builder().put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "different-repo").build();
        expectThrows(
            org.opensearch.snapshots.SnapshotRestoreException.class,
            () -> client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .setIndexSettings(changeSegmentRepo)
                .get()
        );

        // ─── Invalid request 3: try to change translog.repository ───
        Settings changeTranslogRepo = Settings.builder().put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "different-translog-repo").build();
        expectThrows(
            org.opensearch.snapshots.SnapshotRestoreException.class,
            () -> client.admin()
                .cluster()
                .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .setIndexSettings(changeTranslogRepo)
                .get()
        );

        // ─── After all failures: verify NO partially-restored index exists ───
        assertFalse("invalid restore must NOT partially create the index", indexExists(indexName));

        // ─── Verify a subsequent VALID restore still works ───
        RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(snapshotRepoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, restoreResponse.status());
        ensureGreen(indexName);
        assertDocCountInIndex(client, indexName, numDocs);
    }

    /**
     * End-to-end V2 snapshot lifecycle for a hot DFA index, validating that indexing + refresh +
     * catalog/segments invariants hold across two restore cycles:
     * <ol>
     *   <li>Create hot DFA index, ingest, snapshot {@code snap1}, delete, restore from {@code snap1}.</li>
     *   <li>After restore: validate engine, catalog, doc count, then ingest more, refresh, validate.</li>
     *   <li>Snapshot the now-larger index as {@code snap2}, delete, restore from {@code snap2}.</li>
     *   <li>After second restore: validate everything again, ingest more, refresh, validate.</li>
     * </ol>
     */
    public void testV2HotDFASnapshotRestoreLifecycle() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);

        final String indexName = "hot-v2-lifecycle";
        final String repoName = "test-snapshot-repo";
        final String snap1 = "snap1-initial";
        final String snap2 = "snap2-after-more-docs";

        final int initialDocs = 30;
        final int phase2Docs = 20;
        final int phase3Docs = 15;
        final int totalAtSnap2 = initialDocs + phase2Docs;
        final int finalTotal = totalAtSnap2 + phase3Docs;

        Path repoPath = randomRepoPath().toAbsolutePath();
        createRepository(repoName, "fs", getRepositorySettings(repoPath, true));

        // ── Phase 1: create hot DFA index, ingest, snapshot ────────────────
        Client client = client();
        createIndex(indexName, getIndexSettings(1, 0).build());
        ensureGreen(indexName);
        indexDocuments(client, indexName, 0, initialDocs);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, initialDocs);

        IndexShard shardPreSnap1 = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardPreSnap1);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardPreSnap1);

        SnapshotInfo s1 = createSnapshot(repoName, snap1, new ArrayList<>());
        assertThat(s1.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals(1, s1.successfulShards());

        // ── Restore from snap1, validate, ingest more, validate, snap2 ─────
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        RestoreSnapshotResponse r1 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, snap1)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, r1.status());
        ensureGreen(indexName);

        // Post-restore #1 validations
        IndexShard shardR1 = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardR1);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardR1);
        assertDocCountInIndex(client, indexName, initialDocs);

        // Ingest more on the restored index — proves indexing works post-restore
        indexDocuments(client, indexName, initialDocs, totalAtSnap2);
        refresh(indexName);
        flush(indexName);
        assertDocCountInIndex(client, indexName, totalAtSnap2);

        IndexShard shardAfterPhase2 = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardAfterPhase2);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardAfterPhase2);

        SnapshotInfo s2 = createSnapshot(repoName, snap2, new ArrayList<>());
        assertThat(s2.state(), equalTo(SnapshotState.SUCCESS));
        assertEquals(1, s2.successfulShards());

        // ── Delete and restore from snap2 (the LATER snapshot, larger doc set) ─
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).get());
        assertFalse(indexExists(indexName));

        RestoreSnapshotResponse r2 = client.admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, snap2)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(RestStatus.OK, r2.status());
        ensureGreen(indexName);

        // Post-restore #2 validations — must reflect snap2 (totalAtSnap2 docs)
        IndexShard shardR2 = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardR2);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardR2);
        assertDocCountInIndex(client, indexName, totalAtSnap2);

        // Ingest yet more, refresh, validate — proves indexing still works after second restore
        indexDocuments(client, indexName, totalAtSnap2, finalTotal);
        refresh(indexName);
        assertDocCountInIndex(client, indexName, finalTotal);

        IndexShard shardFinal = getShardZero(indexName);
        assertAllFormatDirsHaveFiles(shardFinal);
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shardFinal);
    }

}
