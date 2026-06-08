/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CompositeDataCoherenceIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Contract tests: InternalEngine equivalents for Parquet+Lucene composite
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Concurrent indexing must not lose docs. Both formats must have identical row counts.
     * Reference: InternalEngineTests.testAppendConcurrently
     */
    public void testConcurrentAppendsCrossFormatConsistency() throws Exception {
        String indexName = "test-concurrent-appends";
        createParquetLuceneIndex(indexName);

        int numThreads = 4;
        int docsPerThread = 25;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger successCount = new AtomicInteger();

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        IndexResponse resp = client().prepareIndex(indexName)
                            .setSource("field_keyword", "t" + threadId + "_d" + i, "field_number", i)
                            .get();
                        if (resp.status() == RestStatus.CREATED) successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join();
        }

        int totalExpected = numThreads * docsPerThread;
        assertEquals("all must succeed", totalExpected, successCount.get());

        flushIndex(indexName);
        assertParquetLuceneRowCountsMatch(indexName, totalExpected);
    }

    /**
     * Bulk indexing must be per-doc atomic — all docs visible, formats consistent.
     * Reference: InternalEngineTests.testConcurrentWritesAndCommits
     */
    public void testBulkIndexCrossFormatConsistency() throws Exception {
        String indexName = "test-bulk-index";
        createParquetLuceneIndex(indexName);

        int batchSize = 50;
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < batchSize; i++) {
            bulk.add(client().prepareIndex(indexName).setSource("field_keyword", "bulk_" + i, "field_number", i));
        }
        BulkResponse resp = bulk.get();
        assertFalse("no failures in bulk", resp.hasFailures());

        flushIndex(indexName);
        assertParquetLuceneRowCountsMatch(indexName, batchSize);
    }

    /**
     * Multiple flush cycles produce additive, consistent state across formats.
     * Reference: InternalEngineTests.testShouldPeriodicallyFlush
     */
    public void testMultipleFlushCyclesAdditive() throws Exception {
        String indexName = "test-multi-flush";
        createParquetLuceneIndex(indexName);

        indexDocsTo(indexName, 5);
        flushIndex(indexName);
        assertParquetLuceneRowCountsMatch(indexName, 5);

        indexDocsTo(indexName, 7);
        flushIndex(indexName);
        assertParquetLuceneRowCountsMatch(indexName, 12);

        indexDocsTo(indexName, 3);
        flushIndex(indexName);
        assertParquetLuceneRowCountsMatch(indexName, 15);
    }

    /**
     * Refresh must make all buffered docs visible atomically across both formats.
     * After a single refresh, both parquet and lucene must show the same new segment
     * with the same row count — no partial visibility where one format lags.
     * Reference: InternalEngineTests.testRefreshScopedSearcher
     */
    public void testRefreshAtomicityAcrossFormats() throws Exception {
        String indexName = "test-refresh-atomicity";
        createParquetLuceneIndex(indexName);

        indexDocsTo(indexName, 10);

        // Single refresh — not flush. Docs should become visible in both formats simultaneously.
        DataFormatAwareEngine engine = getEngine(indexName);
        engine.refresh("test");

        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            long parquetRows = snapshot.getSearchableFiles("parquet").stream().mapToLong(WriterFileSet::numRows).sum();
            long luceneRows = snapshot.getSearchableFiles("lucene").stream().mapToLong(WriterFileSet::numRows).sum();
            assertEquals("refresh must make same docs visible in both formats", parquetRows, luceneRows);
            assertEquals("all 10 docs must be visible after refresh", 10, parquetRows);
        }
    }

    /**
     * flushAndClose must commit all buffered data. After reopen, both formats must have
     * all docs that were indexed before shutdown.
     * Reference: InternalEngineTests.testFlushAndClose
     */
    public void testFlushAndClosePreservesAllData() throws Exception {
        String indexName = "test-flush-and-close";
        createParquetLuceneIndex(indexName);

        indexDocsTo(indexName, 10);
        // Do NOT flush explicitly — flushAndClose should handle it

        // flushAndClose via index close (which triggers flushAndClose on the engine)
        client().admin().indices().prepareClose(indexName).get();
        client().admin().indices().prepareOpen(indexName).get();
        ensureGreen(indexName);

        DataFormatAwareEngine recovered = getEngine(indexName);
        assertEquals("all 10 ops recovered via translog", 9, recovered.getSeqNoStats(-1).getMaxSeqNo());

        // Flush + refresh to make recovered data visible in catalog
        flushIndex(indexName);
        recovered.refresh("verify");

        long parquetRows = getRowCount(indexName, "parquet");
        long luceneRows = getRowCount(indexName, "lucene");
        assertEquals("post-close parquet and lucene must match", parquetRows, luceneRows);
        assertEquals("all 10 docs must survive flushAndClose", 10, parquetRows);
    }

    /**
     * Translog replay after clean shutdown (close/reopen) must produce identical state
     * in both formats. Index docs, flush some, then index more WITHOUT flushing. Close/reopen.
     * The close triggers flushAndClose which persists uncommitted data via translog.
     * After reopen, both formats must have all docs.
     * Reference: InternalEngineTests.testTranslogReplay + testRecoverFromLocalTranslog
     */
    public void testTranslogReplayCleanShutdownCrossFormatConsistency() throws Exception {
        String indexName = "test-translog-replay-clean";
        createParquetLuceneIndex(indexName);

        // Phase 1: committed data
        indexDocsTo(indexName, 8);
        flushIndex(indexName);

        // Phase 2: uncommitted data (in translog only)
        indexDocsTo(indexName, 5);
        // NO explicit flush — close will trigger flushAndClose, translog holds these ops

        DataFormatAwareEngine engine = getEngine(indexName);
        assertEquals("13 ops total", 12, engine.getSeqNoStats(-1).getMaxSeqNo());

        // Close/reopen — close triggers flushAndClose, reopen replays from translog if needed
        client().admin().indices().prepareClose(indexName).get();
        client().admin().indices().prepareOpen(indexName).get();
        ensureGreen(indexName);

        DataFormatAwareEngine recovered = getEngine(indexName);
        assertEquals("all 13 ops recovered", 12, recovered.getSeqNoStats(-1).getMaxSeqNo());

        flushIndex(indexName);
        recovered.refresh("verify");

        long parquetRows = getRowCount(indexName, "parquet");
        long luceneRows = getRowCount(indexName, "lucene");
        assertEquals("post-replay: formats must match", parquetRows, luceneRows);
        assertEquals("post-replay: all 13 docs present", 13, parquetRows);
    }

    /**
     * Translog replay after unclean shutdown (failEngine) must produce identical state
     * in both formats. Index docs, flush some, then index more WITHOUT flushing.
     * Fail engine (simulates crash). After recovery, both formats must have all docs
     * (committed + replayed from translog).
     * Reference: InternalEngineTests.testTranslogReplay
     */
    public void testTranslogReplayUncleanShutdownCrossFormatConsistency() throws Exception {
        String indexName = "test-translog-replay-crash";
        createParquetLuceneIndex(indexName);

        // Phase 1: committed data
        indexDocsTo(indexName, 8);
        flushIndex(indexName);

        // Phase 2: uncommitted data (in translog only)
        indexDocsTo(indexName, 5);
        // NO flush — these are only in the translog

        DataFormatAwareEngine engine = getEngine(indexName);
        assertEquals("13 ops total", 12, engine.getSeqNoStats(-1).getMaxSeqNo());

        // Simulate unclean shutdown — engine fails without flushing
        engine.failEngine("simulated-crash", new java.io.IOException("disk error"));

        // Reopen — triggers translog replay of the 5 uncommitted docs
        client().admin().indices().prepareClose(indexName).get();
        client().admin().indices().prepareOpen(indexName).get();
        ensureGreen(indexName);

        DataFormatAwareEngine recovered = getEngine(indexName);
        assertEquals("all 13 ops recovered", 12, recovered.getSeqNoStats(-1).getMaxSeqNo());

        flushIndex(indexName);
        recovered.refresh("verify");

        long parquetRows = getRowCount(indexName, "parquet");
        long luceneRows = getRowCount(indexName, "lucene");
        assertEquals("post-replay: formats must match", parquetRows, luceneRows);
        assertEquals("post-replay: all 13 docs present", 13, parquetRows);
    }

    /**
     * Concurrent indexing + refresh must produce consistent cross-format snapshots.
     * While indexing threads are active, refresh threads take snapshots. Every snapshot
     * must have matching row counts across both formats.
     * Reference: InternalEngineTests.testConcurrentAppendUpdateAndRefresh
     */
    public void testConcurrentIndexAndRefreshConsistency() throws Exception {
        String indexName = "test-concurrent-refresh";
        createParquetLuceneIndex(indexName);

        int numIndexThreads = 3;
        int docsPerThread = 20;
        int numRefreshes = 10;
        CyclicBarrier barrier = new CyclicBarrier(numIndexThreads + 1);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger inconsistencyCount = new AtomicInteger();

        // Indexing threads
        Thread[] indexThreads = new Thread[numIndexThreads];
        for (int t = 0; t < numIndexThreads; t++) {
            final int threadId = t;
            indexThreads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        IndexResponse resp = client().prepareIndex(indexName)
                            .setSource("field_keyword", "t" + threadId + "_d" + i, "field_number", i)
                            .get();
                        if (resp.status() == RestStatus.CREATED) successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            indexThreads[t].start();
        }

        // Refresh thread — checks cross-format consistency at each refresh point
        Thread refreshThread = new Thread(() -> {
            try {
                barrier.await();
                DataFormatAwareEngine eng = getEngine(indexName);
                for (int r = 0; r < numRefreshes; r++) {
                    eng.refresh("concurrent-check-" + r);
                    try (GatedCloseable<CatalogSnapshot> ref = eng.acquireSnapshot()) {
                        CatalogSnapshot snap = ref.get();
                        long pq = snap.getSearchableFiles("parquet").stream().mapToLong(WriterFileSet::numRows).sum();
                        long lc = snap.getSearchableFiles("lucene").stream().mapToLong(WriterFileSet::numRows).sum();
                        if (pq != lc) {
                            inconsistencyCount.incrementAndGet();
                        }
                    }
                    Thread.sleep(5);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        refreshThread.start();

        for (Thread t : indexThreads) {
            t.join();
        }
        refreshThread.join();

        assertEquals("every intermediate refresh must show consistent cross-format counts", 0, inconsistencyCount.get());

        // Final verification
        flushIndex(indexName);
        assertParquetLuceneRowCountsMatch(indexName, numIndexThreads * docsPerThread);
    }

    /**
     * After multiple refresh cycles, each segment must have matching numRows in both
     * its parquet and lucene WriterFileSets. This verifies that row IDs are
     * correctly assigned within each writer generation across formats.
     * (No direct InternalEngine analog — unique to multi-format)
     */
    public void testSegmentGenerationAlignmentAcrossFormats() throws Exception {
        String indexName = "test-segment-alignment";
        createParquetLuceneIndex(indexName);

        // Create 3 segments with different sizes
        indexDocsTo(indexName, 4);
        flushIndex(indexName);

        indexDocsTo(indexName, 7);
        flushIndex(indexName);

        indexDocsTo(indexName, 2);
        flushIndex(indexName);

        DataFormatAwareEngine engine = getEngine(indexName);
        engine.refresh("verify");

        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            for (Segment segment : snapshot.getSegments()) {
                WriterFileSet parquetWfs = segment.dfGroupedSearchableFiles().get("parquet");
                WriterFileSet luceneWfs = segment.dfGroupedSearchableFiles().get("lucene");

                assertNotNull("segment gen=" + segment.generation() + " must have parquet files", parquetWfs);
                assertNotNull("segment gen=" + segment.generation() + " must have lucene files", luceneWfs);
                assertEquals(
                    "segment gen=" + segment.generation() + " must have same numRows in both formats",
                    parquetWfs.numRows(),
                    luceneWfs.numRows()
                );
                assertTrue("segment gen=" + segment.generation() + " must have positive numRows", parquetWfs.numRows() > 0);
            }
        }
    }

    /**
     * Writer generation counter must resume from the committed value after recovery,
     * not restart from 1. Otherwise new segments collide with committed segments in
     * the CatalogSnapshot.
     * Reference: InternalEngineTests.testSequenceNumberAdvancesToMaxSeqOnEngineOpenOnPrimary
     */
    public void testWriterGenerationResumesAfterRestart() throws Exception {
        String indexName = "test-writer-gen-resume";
        createParquetLuceneIndex(indexName);

        // Create 3 segments — writer generations 1, 2, 3
        indexDocsTo(indexName, 5);
        flushIndex(indexName);
        indexDocsTo(indexName, 5);
        flushIndex(indexName);
        indexDocsTo(indexName, 5);
        flushIndex(indexName);

        // Close and reopen
        client().admin().indices().prepareClose(indexName).get();
        client().admin().indices().prepareOpen(indexName).get();
        ensureGreen(indexName);

        // Index more after reopen — should NOT collide with existing segment generations
        indexDocsTo(indexName, 5);
        flushIndex(indexName);

        DataFormatAwareEngine engine = getEngine(indexName);
        engine.refresh("verify");

        // Verify: 20 total docs, no generation conflicts
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            long totalParquet = snapshot.getSearchableFiles("parquet").stream().mapToLong(WriterFileSet::numRows).sum();
            long totalLucene = snapshot.getSearchableFiles("lucene").stream().mapToLong(WriterFileSet::numRows).sum();
            assertEquals("all 20 docs must be present in parquet", 20, totalParquet);
            assertEquals("all 20 docs must be present in lucene", 20, totalLucene);

            // Verify no duplicate generations
            java.util.List<Long> generations = snapshot.getSegments()
                .stream()
                .map(Segment::generation)
                .collect(java.util.stream.Collectors.toList());
            assertEquals(
                "all segment generations must be unique (no collision after restart)",
                generations.size(),
                generations.stream().distinct().count()
            );
        }
    }

    /**
     * Concurrent indexing + flush must not lose documents. All docs that got
     * CREATED responses must be visible after flush settles.
     * Reference: InternalEngineTests.testConcurrentWritesAndCommits
     */
    public void testConcurrentIndexAndFlushNoDataLoss() throws Exception {
        String indexName = "test-concurrent-flush";
        createParquetLuceneIndex(indexName);

        int numIndexThreads = 3;
        int docsPerThread = 30;
        CyclicBarrier barrier = new CyclicBarrier(numIndexThreads + 1);
        AtomicInteger successCount = new AtomicInteger();

        Thread[] indexThreads = new Thread[numIndexThreads];
        for (int t = 0; t < numIndexThreads; t++) {
            final int tid = t;
            indexThreads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        IndexResponse resp = client().prepareIndex(indexName)
                            .setSource("field_keyword", "t" + tid + "_d" + i, "field_number", i)
                            .get();
                        if (resp.status() == RestStatus.CREATED) successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            indexThreads[t].start();
        }

        // Flush thread — flushes while indexing is in progress
        Thread flushThread = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(10);
                    flushIndex(indexName);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        flushThread.start();

        for (Thread t : indexThreads) {
            t.join();
        }
        flushThread.join();

        // Final flush to commit any remaining buffered data
        flushIndex(indexName);
        DataFormatAwareEngine engine = getEngine(indexName);
        engine.refresh("verify");

        int expected = successCount.get();
        long parquetRows = getRowCount(indexName, "parquet");
        long luceneRows = getRowCount(indexName, "lucene");
        assertEquals("parquet must have all successful docs", expected, parquetRows);
        assertEquals("lucene must have all successful docs", expected, luceneRows);
    }

    // ── Helpers for Parquet+Lucene tests ──

    private void createParquetLuceneIndex(String indexName) {
        CompositeEngineHelper.createCompositeIndexWithMapping(this, indexName, "parquet", Settings.EMPTY, "lucene");
    }

    private void indexDocsTo(String indexName, int count) {
        CompositeEngineHelper.indexDocs(this, indexName, count);
    }

    private void flushIndex(String indexName) {
        CompositeEngineHelper.flush(this, indexName);
    }

    private DataFormatAwareEngine getEngine(String indexName) {
        return CompositeEngineHelper.getEngine(clusterService(), internalCluster(), indexName);
    }

    private long getRowCount(String indexName, String formatName) throws IOException {
        return CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), indexName, formatName);
    }

    private void assertParquetLuceneRowCountsMatch(String indexName, long expected) throws IOException {
        DataFormatAwareEngine engine = getEngine(indexName);
        engine.refresh("test");
        assertEquals("parquet row count", expected, getRowCount(indexName, "parquet"));
        assertEquals("lucene row count", expected, getRowCount(indexName, "lucene"));

        CompositeEngineHelper.assertPerSegmentRowCountsMatch(engine, "parquet", "lucene");

        // Checkpoint must be consistent with seqNo
        long maxSeq = engine.getSeqNoStats(-1).getMaxSeqNo();
        long processedCp = engine.getProcessedLocalCheckpoint();
        assertEquals("processed checkpoint must equal maxSeqNo", maxSeq, processedCp);
    }
}
