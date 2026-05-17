/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration tests for commit deletion behavior in DataFormatAwareEngine
 * with composite (Lucene + Parquet) data format.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeCommitDeletionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-commit-deletion";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    @Override
    public void setUp() throws Exception {
        enableMerge();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist if test failed before creation
        }
        super.tearDown();
        disableMerge();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    private static void enableMerge() {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
    }

    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    private static void disableMerge() {
        System.clearProperty(MERGE_ENABLED_PROPERTY);
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
                    .putList("index.composite.secondary_data_formats", "lucene")
            )
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void indexDocs(int count, int startId) {
        for (int i = startId; i < startId + count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setId(String.valueOf(i))
                    .setSource("name", "doc_" + i, "value", i)
                    .get()
                    .status()
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

    private Set<String> listParquetFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) return Set.of();
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.map(p -> p.getFileName().toString()).filter(f -> f.endsWith(".parquet")).collect(Collectors.toSet());
        }
    }

    /** Returns the set of unique segment prefixes (the N in _N.cfs, _N_xxx.dvd, etc.) */
    private Set<String> getSegmentPrefixes(Path luceneDir) throws IOException {
        return listFiles(luceneDir).stream()
            .filter(f -> f.startsWith("_") && !f.startsWith("__"))
            .map(f -> f.substring(1).split("[_.]")[0])
            .collect(Collectors.toSet());
    }

    private int commitCount(IndexShard shard) throws IOException {
        List<IndexCommit> commits = DirectoryReader.listCommits(shard.store().directory());
        return (int) commits.stream().map(c -> {
            try {
                return c.getUserData().get(CatalogSnapshot.CATALOG_SNAPSHOT_ID);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).distinct().count();
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

        // GCP is advanced asynchronously via the global checkpoint sync action.
        // Wait for it to catch up to local checkpoint, then flush once more so the
        // deletion policy sees the updated GCP and deletes all older commits.
        assertBusy(() -> assertEquals(shard.getLocalCheckpoint(), shard.getLastSyncedGlobalCheckpoint()));
        flush();

        assertEquals("Only latest commit should remain", 1, commitCount(shard));

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
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        Path luceneDir = shard.shardPath().resolveIndex();

        // Acquire snapshot — holds a ref on gen 1 catalog snapshot
        GatedCloseable<CatalogSnapshot> snapshotHold = engine.acquireSnapshot();
        Set<String> parquetFilesHeld = listParquetFiles(parquetDir);
        Set<String> luceneSegmentPrefixesHeld = getSegmentPrefixes(luceneDir);
        assertFalse("Should have parquet files from gen 1", parquetFilesHeld.isEmpty());
        assertFalse("Should have lucene segments from gen 1", luceneSegmentPrefixesHeld.isEmpty());

        // Index more batches and flush
        indexDocs(10, 10);
        flush();
        indexDocs(10, 20);
        flush();

        // Force merge to 1 segment
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();

        // Wait for merge to complete
        assertBusy(() -> {
            DataFormatAwareEngine eng = getEngine(shard);
            try (GatedCloseable<CatalogSnapshot> ref = eng.acquireSnapshot()) {
                DataformatAwareCatalogSnapshot snap = (DataformatAwareCatalogSnapshot) ref.get();
                assertEquals("Merge should reduce to 1 segment", 1, snap.getSegments().size());
            }
        });

        // Wait for GCP, flush to trigger deletion policy
        assertBusy(() -> assertEquals(shard.getLocalCheckpoint(), shard.getLastSyncedGlobalCheckpoint()));
        flush();

        // Old parquet files should still exist — snapshot hold prevents deletion
        Set<String> parquetFilesAfterMerge = listParquetFiles(parquetDir);
        for (String f : parquetFilesHeld) {
            assertTrue("Parquet file should survive due to snapshot hold: " + f, parquetFilesAfterMerge.contains(f));
        }

        // Old lucene segments should still exist — snapshot hold prevents deletion
        Set<String> lucenePrefixesAfterMerge = getSegmentPrefixes(luceneDir);
        for (String prefix : luceneSegmentPrefixesHeld) {
            assertTrue("Lucene segment prefix should survive due to snapshot hold: _" + prefix, lucenePrefixesAfterMerge.contains(prefix));
        }

        // Release snapshot
        snapshotHold.close();

        // Flush to trigger cleanup of now-unreferenced files
        flush();

        // Old parquet files should now be deleted — only merged file remains
        Set<String> parquetFilesFinal = listParquetFiles(parquetDir);
        assertEquals("Only 1 merged parquet file should remain, got: " + parquetFilesFinal, 1, parquetFilesFinal.size());
        for (String f : parquetFilesHeld) {
            assertFalse("Old parquet file should be deleted after snapshot release: " + f, parquetFilesFinal.contains(f));
        }

        // Lucene: only one segment generation should remain
        Set<String> lucenePrefixesFinal = getSegmentPrefixes(luceneDir);
        assertEquals("Only 1 lucene segment generation should remain, got: " + lucenePrefixesFinal, 1, lucenePrefixesFinal.size());

        assertEquals("Only latest commit should remain", 1, commitCount(shard));
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

    // ---- Test 5: Force merge deletes old segment files across both formats ----

    public void testForceMergeDeletesOldSegmentFiles() throws Exception {
        createCompositeIndex();

        // 3 batches → 3 flush cycles → 3 parquet generations + 3 lucene segment sets
        indexDocs(10, 0);
        flush();
        indexDocs(10, 10);
        flush();
        indexDocs(10, 20);
        flush();

        IndexShard shard = getPrimaryShard();
        Path dataPath = shard.shardPath().getDataPath();
        Path parquetDir = dataPath.resolve("parquet");
        Path luceneDir = shard.shardPath().resolveIndex();

        // Before merge: should have 3 parquet files (one per generation)
        Set<String> parquetFilesBefore = listParquetFiles(parquetDir);
        assertTrue("Should have multiple parquet files before merge, got: " + parquetFilesBefore, parquetFilesBefore.size() >= 3);

        // Before merge: lucene dir should have files from multiple generations
        Set<String> luceneFilesBefore = listFiles(luceneDir);
        assertFalse("Lucene dir should have files before merge", luceneFilesBefore.isEmpty());

        // Force merge to 1 segment
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();

        // Wait for merge to complete (refresh happens internally during merge)
        assertBusy(() -> {
            DataFormatAwareEngine eng = getEngine(shard);
            try (GatedCloseable<CatalogSnapshot> ref = eng.acquireSnapshot()) {
                DataformatAwareCatalogSnapshot snap = (DataformatAwareCatalogSnapshot) ref.get();
                assertEquals("Merge should reduce to 1 segment", 1, snap.getSegments().size());
            }
        });

        // Wait for GCP to catch up, then flush to trigger deletion policy
        assertBusy(() -> assertEquals(shard.getLocalCheckpoint(), shard.getLastSyncedGlobalCheckpoint()));
        flush();

        // After merge committed: only 1 parquet file should survive
        Set<String> parquetFilesAfter = listParquetFiles(parquetDir);
        assertEquals("Only 1 merged parquet file should remain, got: " + parquetFilesAfter, 1, parquetFilesAfter.size());

        // Lucene: all segment files should belong to a single generation
        Set<String> lucenePrefixesAfter = getSegmentPrefixes(luceneDir);
        assertEquals(
            "All lucene segment files should belong to one generation, got prefixes: " + lucenePrefixesAfter,
            1,
            lucenePrefixesAfter.size()
        );

        // Only 1 commit should remain
        assertEquals("Only latest commit should remain", 1, commitCount(shard));

        // Verify catalog snapshot has both formats in the merged segment
        DataFormatAwareEngine engine = getEngine(shard);
        try (GatedCloseable<CatalogSnapshot> snapshotRef = engine.acquireSnapshot()) {
            DataformatAwareCatalogSnapshot dfSnapshot = (DataformatAwareCatalogSnapshot) snapshotRef.get();
            assertTrue(
                "Segment should have parquet files",
                dfSnapshot.getSegments().get(0).dfGroupedSearchableFiles().containsKey("parquet")
            );
            assertTrue(
                "Segment should have lucene files",
                dfSnapshot.getSegments().get(0).dfGroupedSearchableFiles().containsKey("lucene")
            );
        }

        // Verify total indexed doc count = 30 via stats API
        long indexCount = client().admin()
            .indices()
            .prepareStats(INDEX_NAME)
            .clear()
            .setIndexing(true)
            .get()
            .getIndex(INDEX_NAME)
            .getShards()[0].getStats().indexing.getTotal()
            .getIndexCount();
        assertEquals("Total indexed docs should be 30", 30, indexCount);
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
