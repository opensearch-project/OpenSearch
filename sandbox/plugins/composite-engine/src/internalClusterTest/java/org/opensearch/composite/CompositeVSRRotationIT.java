/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests exercising Parquet VSR (VectorSchemaRoot) rotation under multi-format
 * composite indexing (Parquet primary + Lucene secondary).
 *
 * Uses a low {@code parquet.max_rows_per_vsr} (5 rows) to force frequent VSR rotation
 * during normal indexing. This validates that:
 * <ul>
 *   <li>VSR rotation boundaries don't break cross-format atomicity</li>
 *   <li>Background native writes complete without data loss</li>
 *   <li>Concurrent indexing across rotation boundaries maintains format consistency</li>
 *   <li>High-throughput batches spanning multiple rotations produce correct totals</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeVSRRotationIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-vsr-rotation";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put("parquet.max_rows_per_vsr", 5)
            .build();
    }

    /**
     * Indexing more docs than the VSR threshold in a single writer generation must
     * trigger rotation and still produce consistent cross-format state.
     * With max_rows_per_vsr=5, indexing 20 docs triggers 4 rotations.
     */
    public void testRotationDuringSequentialIndexingMaintainsConsistency() throws Exception {
        createParquetLuceneIndex();

        indexDocs(20);
        flush();

        assertCrossFormatConsistency(20);
    }

    /**
     * Multiple flush cycles where each cycle exceeds the VSR threshold.
     * Each cycle triggers rotations independently; cross-format state must be
     * additive and consistent after each flush.
     */
    public void testMultipleFlushCyclesWithRotation() throws Exception {
        createParquetLuceneIndex();

        // Cycle 1: 12 docs → 2+ rotations
        indexDocs(12);
        flush();
        assertCrossFormatConsistency(12);

        // Cycle 2: 8 docs → 1+ rotation
        indexDocs(8);
        flush();
        assertCrossFormatConsistency(20);

        // Cycle 3: 15 docs → 3 rotations
        indexDocs(15);
        flush();
        assertCrossFormatConsistency(35);
    }

    /**
     * Bulk indexing a batch larger than the VSR threshold must not lose docs
     * at rotation boundaries. All docs in the bulk must appear in both formats.
     */
    public void testBulkIndexSpanningMultipleRotations() throws Exception {
        createParquetLuceneIndex();

        int batchSize = 30; // 6 rotations at threshold=5
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < batchSize; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("field_keyword", "bulk_" + i, "field_number", i));
        }
        BulkResponse resp = bulk.get();
        assertFalse("no failures in bulk", resp.hasFailures());

        flush();
        assertCrossFormatConsistency(batchSize);
    }

    /**
     * Concurrent indexing from multiple threads where total docs far exceed the
     * VSR threshold. Rotations happen under contention — must not lose docs or
     * produce cross-format mismatches.
     */
    public void testConcurrentIndexingAcrossRotationBoundaries() throws Exception {
        createParquetLuceneIndex();

        int numThreads = 4;
        int docsPerThread = 15; // 60 total → 12 rotations at threshold=5
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger successCount = new AtomicInteger();

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int tid = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        IndexResponse resp = client().prepareIndex(INDEX_NAME)
                            .setSource("field_keyword", "t" + tid + "_d" + i, "field_number", i)
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

        int expected = successCount.get();
        assertEquals("all should succeed", numThreads * docsPerThread, expected);

        flush();
        assertCrossFormatConsistency(expected);
    }

    /**
     * Alternating flush and index cycles where each batch crosses rotation boundaries.
     * Tests that rotation state resets correctly between writer generations.
     */
    public void testRotationStateResetsAcrossWriterGenerations() throws Exception {
        createParquetLuceneIndex();

        // Each flush retires the current writer; new writer starts fresh VSR state
        for (int cycle = 0; cycle < 5; cycle++) {
            indexDocs(7); // crosses threshold once per cycle
            flush();
        }

        assertCrossFormatConsistency(35);
    }

    /**
     * After VSR rotation, a refresh must capture all docs including those from the
     * rotated (background-written) batch. No docs lost at rotation seams.
     */
    public void testRefreshAfterRotationCapturesAllDocs() throws Exception {
        createParquetLuceneIndex();

        // Index exactly at threshold boundaries: 5 + 5 + 3 = 13 docs
        indexDocs(5); // fills first VSR exactly
        indexDocs(5); // triggers rotation, fills second VSR
        indexDocs(3); // partial third VSR

        // Refresh without flush — must see all 13
        DataFormatAwareEngine engine = getEngine();
        engine.refresh("test");

        long parquetRows = getRowCount("parquet");
        long luceneRows = getRowCount("lucene");
        assertEquals("parquet must see all docs after rotation + refresh", 13, parquetRows);
        assertEquals("lucene must match parquet", parquetRows, luceneRows);
    }

    /**
     * Index docs, flush, then index more docs spanning rotation, then close/reopen.
     * After recovery, both formats must have all committed + replayed docs.
     */
    public void testRecoveryAfterRotation() throws Exception {
        createParquetLuceneIndex();

        // Committed: 12 docs (2+ rotations)
        indexDocs(12);
        flush();

        // Uncommitted: 8 more docs (1+ rotation) — only in translog
        indexDocs(8);

        // Close/reopen — flushAndClose commits everything, reopen recovers
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        flush();
        getEngine().refresh("verify");

        long parquetRows = getRowCount("parquet");
        long luceneRows = getRowCount("lucene");
        assertEquals("both formats must match after recovery", parquetRows, luceneRows);
        assertEquals("all 20 docs must survive recovery", 20, parquetRows);
    }

    /**
     * Concurrent indexing + flush where flushes happen while VSR rotation is in progress.
     * Tests the interaction between: rotation (freezes VSR, submits background write) and
     * flush (awaits pending write, then writes remaining). Under low threshold (5), almost
     * every flush will race with a pending rotation write.
     */
    public void testConcurrentFlushDuringVSRRotation() throws Exception {
        createParquetLuceneIndex();

        int numIndexThreads = 3;
        int docsPerThread = 20;
        CyclicBarrier barrier = new CyclicBarrier(numIndexThreads + 1);
        AtomicInteger successCount = new AtomicInteger();

        Thread[] indexThreads = new Thread[numIndexThreads];
        for (int t = 0; t < numIndexThreads; t++) {
            final int tid = t;
            indexThreads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        IndexResponse resp = client().prepareIndex(INDEX_NAME)
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

        // Flush thread — flushes repeatedly while indexing is ongoing
        Thread flushThread = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 8; i++) {
                    Thread.sleep(5);
                    flush();
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
        flush();

        int expected = successCount.get();
        assertCrossFormatConsistency(expected);
    }

    /**
     * Concurrent indexing + refresh where refreshes race with VSR rotation.
     * Each refresh acquires the writer pool, flushes writers (triggering awaitPendingWrite
     * inside ParquetWriter.flush → VSRManager.flush), and commits a new CatalogSnapshot.
     * Tests that concurrent refresh during rotation doesn't lose docs or break atomicity.
     */
    public void testConcurrentRefreshDuringVSRRotation() throws Exception {
        createParquetLuceneIndex();

        int numIndexThreads = 3;
        int docsPerThread = 15;
        int numRefreshes = 10;
        CyclicBarrier barrier = new CyclicBarrier(numIndexThreads + 1);
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger inconsistencyCount = new AtomicInteger();

        Thread[] indexThreads = new Thread[numIndexThreads];
        for (int t = 0; t < numIndexThreads; t++) {
            final int tid = t;
            indexThreads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        IndexResponse resp = client().prepareIndex(INDEX_NAME)
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

        // Refresh thread — refreshes repeatedly, checking cross-format consistency each time
        Thread refreshThread = new Thread(() -> {
            try {
                barrier.await();
                DataFormatAwareEngine eng = getEngine();
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
                    Thread.sleep(3);
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

        assertEquals("every mid-stream refresh must be cross-format consistent during rotation", 0, inconsistencyCount.get());

        flush();
        assertCrossFormatConsistency(successCount.get());
    }

    /**
     * High-throughput burst that triggers many rapid rotations back-to-back.
     * With max_rows_per_vsr=5 and 100 docs, this creates 20 rotation cycles.
     * Tests that the background write pipeline doesn't drop batches under pressure.
     */
    public void testHighThroughputBurstWithRapidRotations() throws Exception {
        createParquetLuceneIndex();

        int totalDocs = 100;
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < totalDocs; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("field_keyword", "burst_" + i, "field_number", i));
        }
        BulkResponse resp = bulk.get();
        assertFalse("no failures in high-throughput burst", resp.hasFailures());

        flush();
        assertCrossFormatConsistency(totalDocs);
    }

    // ── Helpers ──

    private void createParquetLuceneIndex() {
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
            .setMapping("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(INDEX_NAME)
                    .setSource("field_keyword", randomAlphaOfLength(10), "field_number", randomIntBetween(0, 1000))
                    .get()
                    .status()
            );
        }
    }

    private void flush() {
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();
    }

    private DataFormatAwareEngine getEngine() {
        String nodeId = clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeName = clusterService().state().nodes().get(nodeId).getName();
        IndexService svc = internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(resolveIndex(INDEX_NAME));
        IndexShard shard = svc.getShard(0);
        return (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
    }

    private long getRowCount(String formatName) throws IOException {
        DataFormatAwareEngine engine = getEngine();
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSearchableFiles(formatName).stream().mapToLong(WriterFileSet::numRows).sum();
        }
    }

    private void assertCrossFormatConsistency(long expected) throws IOException {
        DataFormatAwareEngine engine = getEngine();
        engine.refresh("verify");

        long parquetRows = getRowCount("parquet");
        long luceneRows = getRowCount("lucene");
        assertEquals("parquet row count", expected, parquetRows);
        assertEquals("lucene row count", expected, luceneRows);

        // Per-segment: both formats must have matching rows
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            for (Segment seg : ref.get().getSegments()) {
                WriterFileSet pq = seg.dfGroupedSearchableFiles().get("parquet");
                WriterFileSet lc = seg.dfGroupedSearchableFiles().get("lucene");
                assertNotNull("segment gen=" + seg.generation() + " missing parquet", pq);
                assertNotNull("segment gen=" + seg.generation() + " missing lucene", lc);
                assertEquals(
                    "segment gen=" + seg.generation() + " row counts must match across formats",
                    pq.numRows(),
                    lc.numRows()
                );
            }
        }
    }
}
