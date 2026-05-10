/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.OpenSearchException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.stub.FileBackedDataFormatPlugin;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Failure handling IT for the composite engine using {@link FileBackedDataFormatPlugin}
 * (format "filebacked") alongside {@link LucenePlugin} (format "lucene").
 * <p>
 * Each test creates its own index configuration:
 * <ul>
 *   <li>filebacked as primary, no secondaries — primary write failure path</li>
 *   <li>lucene as primary, filebacked as secondary — secondary write failure
 *       (FlushAndCloseWriterException path: writer flushed and retired)</li>
 *   <li>filebacked as primary, lucene as secondary — primary failure in multi-format
 *       (writer stays in pool, secondary never called for failed doc)</li>
 * </ul>
 * <p>
 * Tests:
 * <ul>
 *   <li>{@code testPrimaryFailureEngineStaysOpen} — single format, 1 doc fails, engine stays open</li>
 *   <li>{@code testIntermittentPrimaryFailures} — single format, every 3rd doc fails (3/9), exact counts verified</li>
 *   <li>{@code testEngineFailureAndRecovery} — failEngine + close/reopen, committed data survives</li>
 *   <li>{@code testSecondaryFailureEngineStaysOpen} — multi-format, secondary fails, cross-format data consistency verified</li>
 *   <li>{@code testIntermittentSecondaryFailures} — multi-format, 3 secondary failures, cross-format consistency verified</li>
 *   <li>{@code testPrimaryFailureInMultiFormatEngineStaysOpen} — multi-format, primary fails, engine stays open</li>
 *   <li>{@code testBurstIOFailuresThenRecovery} — 5 consecutive I/O failures, engine recovers</li>
 *   <li>{@code testIOFailureOnSecondaryMaintainsConsistency} — I/O failure on secondary, cross-format consistency verified</li>
 * </ul>
 * <p>
 * Cross-format consistency verification reads Lucene's DirectoryReader and filebacked files
 * referenced by CatalogSnapshot, verifying: matching row counts, docId == rowId in Lucene,
 * and identical rowId sets across both formats.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressTempFileChecks(bugUrl = "Flushed LuceneWriter segments persist in temp dir until catalog snapshot cleanup")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeEngineFailureIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-failure";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(FileBackedDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        FileBackedDataFormatPlugin.clearFailure();
        FileBackedDataFormatPlugin.setDataDirectory(createTempDir("filebacked"));
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        FileBackedDataFormatPlugin.clearFailure();
        super.tearDown();
    }

    // --- Single format: filebacked as primary, no secondaries ---

    /** 5 docs succeed (seq 0-4), 1 fails (seq 5 assigned), 3 more succeed (seq 6-8). Engine stays open. */
    public void testPrimaryFailureEngineStaysOpen() {
        createCompositeIndex("filebacked");
        indexDocs(5);
        long seqBefore = getEngine().getSeqNoStats(-1).getMaxSeqNo();
        assertEquals("5 docs indexed", 4, seqBefore);

        FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
        BulkItemResponse failedItem = indexSingleDoc("fail");
        assertTrue("doc should have failed", failedItem.isFailed());
        assertNotNull("failure message should be present", failedItem.getFailureMessage());
        FileBackedDataFormatPlugin.clearFailure();

        // Engine must still be open — verify by indexing more docs
        indexDocs(3);
        flush();
        DataFormatAwareEngine engine = getEngine();
        assertEquals("5 + 1 failed + 3 = 9 ops, maxSeqNo = 8", 8, engine.getSeqNoStats(-1).getMaxSeqNo());
        assertNotNull("engine should have valid commit", engine.commitStats());
        assertTrue("commit generation should be positive", engine.commitStats().getGeneration() > 0);
    }

    /** Every 3rd doc fails. Engine stays healthy, failed docs skipped, successful docs committed. */
    public void testIntermittentPrimaryFailures() {
        createCompositeIndex("filebacked");
        indexDocs(5);
        flush();

        // failEveryNthDoc(3): docs at counter 3, 6, 9 fail = 3 failures, 6 successes
        FileBackedDataFormatPlugin.setFailEveryNthDoc(3);
        int successes = 0;
        int failures = 0;
        for (int i = 0; i < 9; i++) {
            try {
                IndexResponse r = client().prepareIndex(INDEX_NAME).setSource("field", "v" + i).get();
                if (r.status() == RestStatus.CREATED) {
                    successes++;
                } else {
                    failures++;
                }
            } catch (OpenSearchException e) {
                failures++;
            }
        }
        assertEquals("3 out of 9 docs should fail (every 3rd)", 3, failures);
        assertEquals("6 out of 9 docs should succeed", 6, successes);
        FileBackedDataFormatPlugin.clearFailure();

        flush();
        DataFormatAwareEngine engine = getEngine();
        assertNotNull("engine should be healthy", engine.commitStats());
        // All 14 ops (5 initial + 9 attempted) get seqNos assigned
        assertEquals(13, engine.getSeqNoStats(-1).getMaxSeqNo());
    }

    /** failEngine → close → reopen. 5 committed docs survive (seq 0-4). New indexing works (seq 5-7). */
    public void testEngineFailureAndRecovery() throws Exception {
        createCompositeIndex("filebacked");
        indexDocs(5);
        flush();

        DataFormatAwareEngine engineBefore = getEngine();
        assertEquals(4, engineBefore.getSeqNoStats(-1).getMaxSeqNo());

        engineBefore.failEngine("test", new IOException("simulated"));

        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        DataFormatAwareEngine recovered = getEngine();
        assertEquals("committed docs survive recovery", 4, recovered.getSeqNoStats(-1).getMaxSeqNo());

        indexDocs(3);
        flush();
        assertEquals("new docs indexed after recovery", 7, recovered.getSeqNoStats(-1).getMaxSeqNo());
    }

    // --- Multi format: lucene primary, filebacked secondary ---

    /** Secondary fails → CompositeWriter rolls back primary → FlushAndCloseWriterException → writer retired. Engine stays open. */
    public void testSecondaryFailureEngineStaysOpen() throws Exception {
        createCompositeIndex("lucene", "filebacked");
        indexDocs(5);
        flush();
        long seqBefore = getEngine().getSeqNoStats(-1).getMaxSeqNo();
        assertEquals(4, seqBefore);

        FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
        BulkItemResponse failedItem = indexSingleDoc("sec-fail");
        assertTrue("doc should have failed on secondary", failedItem.isFailed());
        assertNotNull("failure message should be present", failedItem.getFailureMessage());
        FileBackedDataFormatPlugin.clearFailure();

        // Engine must still be open
        indexDocs(3);
        flush();
        DataFormatAwareEngine engine = getEngine();
        engine.refresh("verify");
        assertNotNull("engine should be healthy after secondary failure", engine.commitStats());
        assertTrue("commit generation should be positive", engine.commitStats().getGeneration() > 0);
        assertTrue("seqNo should have advanced", engine.getSeqNoStats(-1).getMaxSeqNo() > seqBefore);

        // Cross-format data consistency
        long luceneRows = getRowCount("lucene");
        long filebackedRows = getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME);
        assertEquals("lucene and filebacked should have same row count", luceneRows, filebackedRows);

        // Deep consistency: verify filebacked files have correct data
        assertFilebackedConsistency((int) filebackedRows);
        assertCrossFormatDataConsistency();
    }

    /** Multiple secondary failures retire multiple writers. Pool creates fresh writers each time. Engine healthy. */
    public void testIntermittentSecondaryFailures() throws Exception {
        createCompositeIndex("lucene", "filebacked");
        indexDocs(5);
        flush();

        // First 3 docs fail on secondary, next 3 succeed
        FileBackedDataFormatPlugin.setFailOnNextNDocs(3);
        int failures = 0;
        int successes = 0;
        for (int i = 0; i < 6; i++) {
            try {
                IndexResponse r = client().prepareIndex(INDEX_NAME).setSource("field", "v" + i).get();
                if (r.status() == RestStatus.CREATED) {
                    successes++;
                } else {
                    failures++;
                }
            } catch (OpenSearchException e) {
                failures++;
            }
        }
        assertEquals("first 3 docs should fail on secondary", 3, failures);
        assertEquals("last 3 docs should succeed", 3, successes);
        FileBackedDataFormatPlugin.clearFailure();

        // Engine recovers — new indexing works
        indexDocs(3);
        flush();
        DataFormatAwareEngine engine = getEngine();
        assertNotNull("engine should be healthy", engine.commitStats());
        // 5 initial + 6 attempted + 3 post-clear = 14 ops
        assertEquals(13, engine.getSeqNoStats(-1).getMaxSeqNo());

        // Cross-format data consistency
        engine.refresh("verify");
        assertCrossFormatDataConsistency();
        assertFilebackedConsistency((int) getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME));
    }

    // --- Multi format: filebacked primary, lucene secondary ---

    /** Primary fails before secondary is called. Writer stays in pool (no rollback needed). Engine stays open. */
    public void testPrimaryFailureInMultiFormatEngineStaysOpen() {
        createCompositeIndex("filebacked", "lucene");
        indexDocs(5);
        flush();
        long seqBefore = getEngine().getSeqNoStats(-1).getMaxSeqNo();

        FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
        BulkItemResponse failedItem = indexSingleDoc("pri-fail");
        assertTrue("doc should have failed on primary", failedItem.isFailed());
        assertNotNull("failure message should be present", failedItem.getFailureMessage());
        FileBackedDataFormatPlugin.clearFailure();

        indexDocs(3);
        flush();
        DataFormatAwareEngine engine = getEngine();
        assertNotNull("engine should be healthy", engine.commitStats());
        assertTrue("seqNo should have advanced", engine.getSeqNoStats(-1).getMaxSeqNo() > seqBefore);
    }

    // --- I/O error simulation ---

    /** Burst of I/O failures followed by recovery — engine survives and committed data is consistent. */
    public void testBurstIOFailuresThenRecovery() {
        createCompositeIndex("filebacked");
        indexDocs(5);
        flush();

        // Burst: 5 consecutive failures
        FileBackedDataFormatPlugin.setFailOnNextNDocs(5);
        int failures = 0;
        for (int i = 0; i < 5; i++) {
            BulkItemResponse item = indexSingleDoc("io-fail-" + i);
            if (item.isFailed()) failures++;
        }
        assertEquals("all 5 docs should fail during burst", 5, failures);
        FileBackedDataFormatPlugin.clearFailure();

        // Engine should recover — new indexing works
        indexDocs(5);
        flush();
        DataFormatAwareEngine engine = getEngine();
        assertNotNull("engine should be healthy after I/O burst", engine.commitStats());
        assertTrue("seqNo should reflect all ops", engine.getSeqNoStats(-1).getMaxSeqNo() >= 14);
    }

    /** I/O failure on secondary in multi-format — engine stays open, cross-format consistency maintained. */
    public void testIOFailureOnSecondaryMaintainsConsistency() throws Exception {
        createCompositeIndex("lucene", "filebacked");
        indexDocs(5);
        flush();

        // Fail 3 docs on secondary (simulates intermittent disk errors)
        FileBackedDataFormatPlugin.setFailOnNextNDocs(3);
        int failures = 0;
        for (int i = 0; i < 3; i++) {
            BulkItemResponse item = indexSingleDoc("io-sec-fail-" + i);
            if (item.isFailed()) failures++;
        }
        assertEquals("all 3 should fail on secondary", 3, failures);
        FileBackedDataFormatPlugin.clearFailure();

        // Index more after recovery
        indexDocs(5);
        flush();

        // Cross-format consistency
        assertCrossFormatDataConsistency();
    }

    // --- Helpers ---

    private void createCompositeIndex(String primary, String... secondaries) {
        CompositeEngineHelper.createCompositeIndex(this, INDEX_NAME, primary, secondaries);
    }

    private org.opensearch.index.shard.IndexShard getPrimaryShard() {
        return CompositeEngineHelper.getPrimaryShard(clusterService(), internalCluster(), INDEX_NAME);
    }

    private DataFormatAwareEngine getEngine() {
        return CompositeEngineHelper.getEngine(clusterService(), internalCluster(), INDEX_NAME);
    }

    private void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(INDEX_NAME).setSource("field", "v" + randomIntBetween(0, 100000)).get().status()
            );
        }
    }

    private BulkItemResponse indexSingleDoc(String value) {
        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setSource("field", value)).get();
        assertEquals(1, bulk.getItems().length);
        return bulk.getItems()[0];
    }

    private void flush() {
        CompositeEngineHelper.flush(this, INDEX_NAME);
    }

    private long getRowCount(String formatName) throws IOException {
        return CompositeEngineHelper.getRowCount(getEngine(), formatName);
    }

    /** Verifies filebacked data: correct doc count, no duplicate entries, and fields are non-empty. */
    private void assertFilebackedConsistency(int expectedDocs) throws IOException {
        List<String> entries = FileBackedDataFormatPlugin.readAllEntries();
        assertEquals("filebacked should have " + expectedDocs + " docs", expectedDocs, entries.size());
        // No duplicate entries
        long uniqueCount = entries.stream().distinct().count();
        assertEquals("no duplicate entries allowed", entries.size(), uniqueCount);
        // Every entry should have fields data
        for (String entry : entries) {
            assertTrue("entry should contain fields: " + entry, entry.contains("fields="));
            assertFalse("fields should not be empty: " + entry, entry.endsWith("fields={}"));
        }
    }

    /**
     * Verifies cross-format data consistency:
     * 1. CatalogSnapshot row counts match between formats
     * 2. Filebacked file entries (from CatalogSnapshot) match row count
     * 3. Lucene DirectoryReader numDocs matches, docId == rowId for every doc
     * 4. RowId sets match across Lucene and filebacked
     */
    private void assertCrossFormatDataConsistency() throws Exception {
        flush();
        getEngine().refresh("pre-verify");

        long luceneSnapshotRows;
        long fbSnapshotRows;
        List<String> fbFileEntries = new ArrayList<>();

        // Read CatalogSnapshot and filebacked file contents
        try (GatedCloseable<CatalogSnapshot> ref = getEngine().acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            luceneSnapshotRows = snapshot.getSearchableFiles("lucene").stream().mapToLong(WriterFileSet::numRows).sum();
            fbSnapshotRows = snapshot.getSearchableFiles(FileBackedDataFormatPlugin.FORMAT_NAME)
                .stream()
                .mapToLong(WriterFileSet::numRows)
                .sum();

            // Read actual file contents for filebacked format from CatalogSnapshot-referenced files
            for (WriterFileSet wfs : snapshot.getSearchableFiles(FileBackedDataFormatPlugin.FORMAT_NAME)) {
                for (String fileName : wfs.files()) {
                    Path file = Path.of(wfs.directory()).resolve(fileName);
                    if (Files.exists(file)) {
                        fbFileEntries.addAll(Files.readAllLines(file));
                    }
                }
            }
        }

        // 1. Row counts match
        assertEquals("CatalogSnapshot row counts must match across formats", luceneSnapshotRows, fbSnapshotRows);

        // 2. Filebacked file entries match CatalogSnapshot row count
        assertEquals("filebacked file entries must match CatalogSnapshot rows", fbSnapshotRows, fbFileEntries.size());

        // 3. Lucene: numDocs matches, every doc has rowId, and docId == rowId
        java.util.Set<Long> luceneRowIds = new java.util.TreeSet<>();
        org.opensearch.index.store.Store store = getPrimaryShard().store();
        store.incRef();
        try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(store.directory())) {
            assertEquals("Lucene numDocs must match CatalogSnapshot", luceneSnapshotRows, reader.numDocs());
            for (org.apache.lucene.index.LeafReaderContext leaf : reader.leaves()) {
                org.apache.lucene.index.NumericDocValues rowIdDV = leaf.reader()
                    .getNumericDocValues(org.opensearch.index.engine.dataformat.DocumentInput.ROW_ID_FIELD);
                assertNotNull("rowId doc values must exist", rowIdDV);
                for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
                    assertTrue("rowId should advance to docId " + docId, rowIdDV.advanceExact(docId));
                    assertEquals("docId must equal rowId (index sorted on rowId)", docId, rowIdDV.longValue());
                    luceneRowIds.add(rowIdDV.longValue());
                }
            }
        } finally {
            store.decRef();
        }

        // 4. Filebacked rowIds must match Lucene rowIds
        java.util.Set<Long> fbRowIds = new java.util.TreeSet<>();
        for (String entry : fbFileEntries) {
            // entry format: gen=N,rowId=M,fields={...}
            for (String part : entry.split(",")) {
                if (part.startsWith("rowId=")) {
                    fbRowIds.add(Long.parseLong(part.substring("rowId=".length())));
                }
            }
        }
        assertEquals("rowId sets must match across formats", luceneRowIds, fbRowIds);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Core contract tests: InternalEngine equivalents
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Docs indexed before a secondary write failure must be visible after refresh.
     * Core contract from InternalEngineTests.testHandleDocumentFailure: a per-document
     * failure must not lose previously accepted writes in the same writer.
     */
    public void testDocsBeforeSecondaryFailureVisibleAfterRefresh() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        // Index 5 docs — all succeed, accumulate in the current writer
        indexDocs(5);

        // Inject a single failure on secondary
        FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
        BulkItemResponse failedItem = indexSingleDoc("trigger-failure");
        assertTrue("doc should fail on secondary", failedItem.isFailed());
        FileBackedDataFormatPlugin.clearFailure();

        // Core contract: the 5 previously successful docs MUST be visible
        flush();
        getEngine().refresh("test");

        long luceneRows = getRowCount("lucene");
        long fbRows = getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME);
        assertEquals("formats must be consistent", luceneRows, fbRows);
        assertEquals("5 docs written before failure must survive", 5, luceneRows);
    }

    /**
     * Sequence numbers must advance monotonically through failures.
     * Core contract from InternalEngineTests.testSeqNoAndCheckpoints: failed ops
     * consume seqNos and the checkpoint advances past them.
     */
    public void testSeqNoAdvancesThroughFailures() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        indexDocs(3);
        FileBackedDataFormatPlugin.setFailOnNextNDocs(2);
        indexSingleDoc("fail-1");
        indexSingleDoc("fail-2");
        FileBackedDataFormatPlugin.clearFailure();
        indexDocs(2);

        // 3 + 2 failed + 2 = 7 total ops → maxSeqNo = 6
        DataFormatAwareEngine engine = getEngine();
        assertEquals("all ops must consume seqNos", 6, engine.getSeqNoStats(-1).getMaxSeqNo());
        assertEquals("checkpoint must advance past all ops", 6, engine.getProcessedLocalCheckpoint());
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Multi-format atomicity: failure modes and races
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Rapid alternating success/failure pattern must not corrupt cross-format state.
     * Every successful doc must appear in BOTH formats; no phantom docs from partially
     * committed failures.
     * Reference: InternalEngineTests.testConcurrentWritesAndCommits
     */
    public void testRapidAlternatingSuccessFailureCrossFormatAtomicity() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        // Pattern: 2 succeed, 1 fail, repeat 5 times = 10 succeed, 5 fail
        int successExpected = 0;
        for (int round = 0; round < 5; round++) {
            indexDocs(2);
            successExpected += 2;
            FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
            indexSingleDoc("fail-round-" + round);
            FileBackedDataFormatPlugin.clearFailure();
        }

        flush();
        getEngine().refresh("verify");

        long luceneRows = getRowCount("lucene");
        long fbRows = getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME);
        assertEquals("formats must be exactly equal", luceneRows, fbRows);
        assertEquals("exactly 10 successful docs", successExpected, luceneRows);
    }

    /**
     * Secondary failure mid-batch (bulk) must not corrupt other docs in the same batch.
     * InternalEngine guarantees per-doc independence — a single doc failure doesn't
     * affect other docs in the same bulk request.
     * Reference: InternalEngineTests.testHandleDocumentFailure
     */
    public void testSecondaryFailureMidBulkDoesNotCorruptBatch() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        indexDocs(5);
        flush();

        // Fail 1 doc out of a 10-doc bulk — the other 9 must succeed
        FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
        int successes = 0;
        int failures = 0;
        org.opensearch.action.bulk.BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("field", "bulk_" + i));
        }
        org.opensearch.action.bulk.BulkResponse resp = bulk.get();
        for (BulkItemResponse item : resp.getItems()) {
            if (item.isFailed()) failures++;
            else successes++;
        }
        FileBackedDataFormatPlugin.clearFailure();

        assertTrue("at least 1 failure", failures >= 1);
        assertTrue("most should succeed", successes >= 9);

        flush();
        getEngine().refresh("verify");

        long luceneRows = getRowCount("lucene");
        long fbRows = getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME);
        assertEquals("formats must match after partial bulk failure", luceneRows, fbRows);
        assertEquals("5 baseline + successes from bulk", 5 + successes, luceneRows);
    }

    /**
     * Concurrent indexing threads with intermittent failures must maintain
     * cross-format atomicity — every refresh snapshot must show equal counts.
     * Tests that the writer pool, retired writer handling, and RowId generation
     * remain consistent under contention.
     * Reference: InternalEngineTests.testConcurrentAppendUpdateAndRefresh
     */
    public void testConcurrentIndexWithFailuresCrossFormatAtomicity() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        indexDocs(5);
        flush();

        // Fail every 7th doc
        FileBackedDataFormatPlugin.setFailEveryNthDoc(7);

        int numThreads = 4;
        int docsPerThread = 20;
        java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(numThreads);
        java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger();

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int tid = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        try {
                            org.opensearch.action.index.IndexResponse r = client().prepareIndex(INDEX_NAME)
                                .setSource("field", "t" + tid + "_d" + i)
                                .get();
                            if (r.status() == RestStatus.CREATED) successCount.incrementAndGet();
                        } catch (Exception e) {
                            // failure expected
                        }
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
        FileBackedDataFormatPlugin.clearFailure();

        // Engine must be healthy
        assertNotNull("engine must be healthy", getEngine().commitStats());

        flush();
        getEngine().refresh("verify");

        long luceneRows = getRowCount("lucene");
        long fbRows = getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME);
        assertEquals("cross-format counts must match under concurrent failures", luceneRows, fbRows);
        assertEquals("5 baseline + concurrent successes", 5 + successCount.get(), luceneRows);
    }

    /**
     * Flush after a secondary failure must commit consistent state — only the
     * successfully indexed docs, with matching counts in both formats.
     * The commit data (CatalogSnapshot) must be self-consistent.
     * Reference: InternalEngineTests.testCommitAdvancesMinTranslogForRecovery
     */
    public void testFlushAfterSecondaryFailureCommitsConsistentSnapshot() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        indexDocs(5);
        FileBackedDataFormatPlugin.setFailOnNextNDocs(1);
        indexSingleDoc("fail");
        FileBackedDataFormatPlugin.clearFailure();
        indexDocs(3);

        flush();

        // Verify committed snapshot is self-consistent
        DataFormatAwareEngine engine = getEngine();
        engine.refresh("verify");

        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            long luceneRows = snapshot.getSearchableFiles("lucene").stream().mapToLong(WriterFileSet::numRows).sum();
            long fbRows = snapshot.getSearchableFiles(FileBackedDataFormatPlugin.FORMAT_NAME)
                .stream()
                .mapToLong(WriterFileSet::numRows)
                .sum();
            assertEquals("committed snapshot must have equal format counts", luceneRows, fbRows);
            assertEquals("5 + 3 = 8 successful docs in committed snapshot", 8, luceneRows);
        }

        // SeqNo consistency
        assertEquals("9 total ops (5+1fail+3)", 8, engine.getSeqNoStats(-1).getMaxSeqNo());
        assertEquals("checkpoint advances past all", 8, engine.getProcessedLocalCheckpoint());
    }

    /**
     * When the secondary writer THROWS IOException (not WriteResult.Failure), the primary
     * writer has already accepted the doc but no rollback occurs. The writer must not be
     * left in an inconsistent state where primary has N+1 docs and secondary has N.
     * After flush, cross-format counts must still match.
     * This tests the boundary between WriteResult.Failure (handled gracefully) and
     * IOException (which bypasses the rollback path in CompositeWriter.addDoc).
     */
    public void testSecondaryThrowsIOExceptionCrossFormatConsistency() throws Exception {
        createCompositeIndex("lucene", "filebacked");

        indexDocs(5);
        flush();

        // Trigger IOException throw (not WriteResult.Failure) on secondary
        FileBackedDataFormatPlugin.setThrowOnNextDoc(true);
        BulkItemResponse failedItem = indexSingleDoc("throw-trigger");
        assertTrue("doc should fail", failedItem.isFailed());
        FileBackedDataFormatPlugin.clearFailure();

        // Index more docs after the throw
        indexDocs(3);
        flush();
        getEngine().refresh("verify");

        // Cross-format consistency: both formats must have the same count
        long luceneRows = getRowCount("lucene");
        long fbRows = getRowCount(FileBackedDataFormatPlugin.FORMAT_NAME);
        assertEquals("formats must be consistent after IOException throw", luceneRows, fbRows);
        assertEquals("5 baseline + 3 post-throw = 8 (thrown doc must not be in either format)", 8, luceneRows);
    }
}
