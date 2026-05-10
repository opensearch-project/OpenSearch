/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.stub.FileBackedDataFormatPlugin;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
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
 * </ul>
 * <p>
 * Cross-format consistency verification reads Lucene's DirectoryReader and filebacked files
 * referenced by CatalogSnapshot, verifying: matching row counts, docId == rowId in Lucene,
 * and identical rowId sets across both formats.
 */
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
            } catch (Exception e) {
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
            } catch (Exception e) {
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

    // --- Helpers ---

    private void createCompositeIndex(String primary, String... secondaries) {
        Settings.Builder sb = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", primary);
        sb.putList("index.composite.secondary_data_formats", secondaries);
        createIndex(INDEX_NAME, sb.build());
        ensureGreen(INDEX_NAME);
    }

    private org.opensearch.index.shard.IndexShard getPrimaryShard() {
        String nodeId = clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeName = clusterService().state().nodes().get(nodeId).getName();
        IndexService svc = internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(resolveIndex(INDEX_NAME));
        return svc.getShard(0);
    }

    private DataFormatAwareEngine getEngine() {
        return (DataFormatAwareEngine) IndexShardTestCase.getIndexer(getPrimaryShard());
    }

    private void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(INDEX_NAME).setSource("field", "v" + randomIntBetween(0, 100000)).get().status()
            );
        }
    }

    /** Indexes a single doc via bulk API and returns the per-item response for failure inspection. */
    private BulkItemResponse indexSingleDoc(String value) {
        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setSource("field", value)).get();
        assertEquals(1, bulk.getItems().length);
        return bulk.getItems()[0];
    }

    private void flush() {
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
    }

    /** Returns total row count for a format from the current CatalogSnapshot. */
    private long getRowCount(String formatName) throws IOException {
        try (GatedCloseable<CatalogSnapshot> ref = getEngine().acquireSnapshot()) {
            return ref.get().getSearchableFiles(formatName).stream().mapToLong(WriterFileSet::numRows).sum();
        }
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
}
