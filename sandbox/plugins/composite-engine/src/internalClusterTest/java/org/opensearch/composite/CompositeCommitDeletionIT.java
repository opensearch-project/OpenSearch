/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration tests for commit deletion behavior in DataFormatAwareEngine
 * with composite (Lucene + Parquet) data format.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeCommitDeletionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-commit-deletion";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private void createCompositeIndex() {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats")
            )
            .setMapping("field", "type=keyword")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void indexDocs(int count, int startId) {
        for (int i = startId; i < startId + count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex().setIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("field", "value_" + i).get().status()
            );
        }
    }

    private FlushResponse flush() {
        return client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();
    }

    private IndexShard getPrimaryShard() {
        String nodeName = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeNameResolved = getClusterState().nodes().get(nodeName).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeNameResolved);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private DataFormatAwareEngine getEngine(IndexShard shard) {
        return (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
    }

    private Set<String> listFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) return Set.of();
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    private int commitCount(IndexShard shard) throws IOException {
        List<IndexCommit> commits = DirectoryReader.listCommits(shard.store().directory());
        return commits.size();
    }

    // ---- Test 1: Old commit files deleted after flush ----

    public void testOldCommitDeletedAfterFlush() throws Exception {
        createCompositeIndex();

        indexDocs(10, 0);
        flush();

        indexDocs(10, 10);
        flush();

        indexDocs(10, 20);
        flush();

        IndexShard shard = getPrimaryShard();
        // In single-shard 0-replica, GCP advances to local checkpoint.
        // After multiple flushes, old commits should be deleted by the deletion policy.
        // We may have at most 2 commits: the safe commit and the latest (which may be the same).
        assertTrue("Old commits should be deleted", commitCount(shard) <= 2);

        CommitStats commitStats = getEngine(shard).commitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getGeneration() > 0);
    }

    // ---- Test 2: Recovery after close/reopen ----

    public void testRecoveryAfterCloseReopen() throws Exception {
        createCompositeIndex();

        indexDocs(10, 0);
        flush();

        indexDocs(10, 10);
        flush();

        // Close and reopen — simulates recovery
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        // Engine should recover cleanly
        IndexShard shard = getPrimaryShard();
        assertNotNull(getEngine(shard).commitStats());

        // Verify we can still index and flush
        indexDocs(5, 20);
        FlushResponse flushResponse = flush();
        assertEquals(RestStatus.OK, flushResponse.getStatus());
    }

    // ---- Test 3: Snapshot prevents file deletion ----

    public void testAcquireSnapshotPreventsFileDeletion() throws Exception {
        createCompositeIndex();

        indexDocs(10, 0);
        flush();

        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = getEngine(shard);
        Path dataPath = shard.shardPath().getDataPath();

        // Acquire snapshot — holds a ref on the current catalog snapshot
        GatedCloseable<CatalogSnapshot> snapshotHold = engine.acquireSnapshot();
        Set<String> filesBeforeMoreFlushes = listFiles(dataPath.resolve("index"));

        // Index more and flush multiple times
        indexDocs(10, 10);
        flush();
        indexDocs(10, 20);
        flush();

        // Files from the held snapshot should still exist (snapshot hold prevents deletion)
        Set<String> filesAfterFlushes = listFiles(dataPath.resolve("index"));
        for (String f : filesBeforeMoreFlushes) {
            if (f.startsWith("segments_") || f.equals("write.lock")) continue;
            assertTrue("File should survive due to snapshot hold: " + f, filesAfterFlushes.contains(f));
        }

        // Release snapshot
        snapshotHold.close();

        // Flush again to trigger cleanup
        indexDocs(5, 30);
        flush();

        // Now old files should be eligible for deletion — only latest commits remain
        assertTrue("Old commits should be cleaned after release", commitCount(shard) <= 2);
    }

    // ---- Test 4: acquireSnapshot returns latest state ----

    public void testAcquireSnapshotReturnsLatestState() throws Exception {
        createCompositeIndex();

        indexDocs(10, 0);
        flush();

        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = getEngine(shard);

        long gen1;
        try (GatedCloseable<CatalogSnapshot> snapshot = engine.acquireSnapshot()) {
            CatalogSnapshot cs = snapshot.get();
            assertNotNull(cs);
            assertTrue("Snapshot should have a valid generation", cs.getGeneration() >= 0);
            assertNotNull("Snapshot should have userData", cs.getUserData());
            gen1 = cs.getGeneration();
        }

        // After more indexing + flush, new snapshot should reflect new state
        indexDocs(10, 10);
        flush();

        try (GatedCloseable<CatalogSnapshot> snapshot = engine.acquireSnapshot()) {
            CatalogSnapshot cs = snapshot.get();
            assertTrue("Generation should advance after flush", cs.getGeneration() > gen1);
        }
    }

    // ---- Test 5: Multi-format file cleanup ----

    public void testMultiFormatFilesCleanedUpOnDeletion() throws Exception {
        createCompositeIndex();

        indexDocs(10, 0);
        flush();

        IndexShard shard = getPrimaryShard();
        Path dataPath = shard.shardPath().getDataPath();
        Path indexDir = dataPath.resolve("index");

        Set<String> luceneFilesAfterCS1 = listFiles(indexDir);
        assertFalse("Lucene files should exist after flush", luceneFilesAfterCS1.isEmpty());

        // Create more commits
        indexDocs(10, 10);
        flush();
        indexDocs(10, 20);
        flush();

        // After GCP advances and old commits are deleted:
        Set<String> luceneFilesNow = listFiles(indexDir);
        assertFalse("Lucene files should still exist", luceneFilesNow.isEmpty());

        // Only latest commits should remain (safe + last, which may be the same)
        assertTrue("Only latest commit remains", commitCount(shard) <= 2);
    }

    // ---- Test 6: Translog recovery after node restart ----

    public void testTranslogRecoveryAfterNodeRestart() throws Exception {
        createCompositeIndex();

        // Index docs and flush to establish a base commit
        indexDocs(5, 0);
        flush();

        // Index more docs WITHOUT flushing — these live only in translog
        indexDocs(10, 5);

        // Get the max seq no before restart — should be 14 (0-indexed, 15 docs total)
        IndexShard shard = getPrimaryShard();
        long maxSeqNoBeforeRestart = getEngine(shard).getSeqNoStats(-1).getMaxSeqNo();
        assertEquals("Max seq no should reflect all 15 indexed docs", 14, maxSeqNoBeforeRestart);

        // Restart the node — translog replay should recover unflushed docs
        internalCluster().restartRandomDataNode();
        ensureGreen(INDEX_NAME);

        // After recovery, flush to materialize all docs
        flush();

        // Verify max seq no is preserved (translog replayed all ops)
        IndexShard shardAfter = getPrimaryShard();
        CommitStats commitStats = getEngine(shardAfter).commitStats();
        assertNotNull(commitStats);
        String maxSeqNoStr = commitStats.getUserData().get(SequenceNumbers.MAX_SEQ_NO);
        assertNotNull("Commit should have max_seq_no", maxSeqNoStr);
        assertEquals("Max seq no should survive restart", 14, Long.parseLong(maxSeqNoStr));

        // Verify engine is functional after recovery
        indexDocs(5, 15);
        flush();
        CommitStats finalStats = getEngine(getPrimaryShard()).commitStats();
        assertEquals(19, Long.parseLong(finalStats.getUserData().get(SequenceNumbers.MAX_SEQ_NO)));
    }
}
