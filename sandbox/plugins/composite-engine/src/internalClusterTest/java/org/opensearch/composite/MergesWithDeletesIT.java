/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end merges-with-deletes coverage for the composite (parquet primary + lucene secondary)
 * {@code DataFormatAwareEngine}, including concurrent multi-shard ingest/delete traffic. Merges
 * reconcile deletes and superseded update copies lazily, so these tests verify that after a merge
 * the surviving rows are physically correct in the written parquet files and stay row-aligned with
 * the merged lucene segment, not just that doc counts look right.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MergesWithDeletesIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "merges_with_deletes";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    @Override
    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    public void setUp() throws Exception {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
        super.setUp();
    }

    @Override
    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX).get();
        } catch (Exception ignored) {
            // index may not exist if the test failed before creating it
        }
        super.tearDown();
        System.clearProperty(MERGE_ENABLED_PROPERTY);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Index setup
    // ══════════════════════════════════════════════════════════════════════

    /** Manual-refresh composite index (lucene secondary for _id resolution), unsorted. */
    private void createIndex() {
        createIndex(false);
    }

    private void createIndex(boolean sorted) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1);
        if (sorted) {
            settings.putList("index.sort.field", "value").putList("index.sort.order", "desc");
        }
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX);
    }

    /** Multi-shard composite index for concurrent-traffic tests. */
    private void createConcurrentIndex(int shards) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX);
    }

    private IndexResponse indexDoc(String id, int value) {
        return client().prepareIndex(INDEX).setId(id).setSource("name", "doc_" + id, "value", value).get();
    }

    private DeleteResponse deleteDoc(String id) {
        return client().prepareDelete(INDEX, id).get();
    }

    private boolean exists(String id) {
        return client().prepareGet(INDEX, id).setRealtime(false).get().isExists();
    }

    private void refresh() {
        client().admin().indices().prepareRefresh(INDEX).get();
    }

    private ForceMergeResponse forceMergeToOne() {
        return client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get();
    }

    /**
     * Physical parquet row count — the logical live-row truth. The inherited {@code getTotalRowCount}
     * sums numRows across <em>every</em> format in each segment (parquet + lucene), so on a composite
     * index it returns 2× the logical rows; here we sum only the parquet ("primary") writer file sets,
     * whose footer numRows is the authoritative source.
     */
    private long parquetRows() throws IOException {
        return acquireAndGetSnapshot(INDEX).getSearchableFiles("parquet").stream().mapToLong(WriterFileSet::numRows).sum();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 1 — delete BEFORE the merge: row must be physically gone from merged parquet
    // ══════════════════════════════════════════════════════════════════════

    /**
     * A doc deleted and refreshed <em>before</em> a force-merge is excluded from the frozen snapshot,
     * so the merged parquet must not physically contain it: merged row count == survivors, the deleted
     * id is unresolvable, and merged parquet row count == merged lucene doc count (formats aligned).
     */
    public void testDeleteBeforeMergeIsPhysicallyDroppedFromMergedParquet() throws Exception {
        createIndex();

        int perSegment = 10;
        int segments = 4;
        int total = perSegment * segments;
        for (int s = 0; s < segments; s++) {
            for (int i = 0; i < perSegment; i++) {
                assertEquals(RestStatus.CREATED, indexDoc("s" + s + "_d" + i, s * perSegment + i).status());
            }
            refresh();
        }

        // Delete a known subset (one per segment) and refresh so the deletes are in the committed state.
        Set<String> deleted = new HashSet<>();
        for (int s = 0; s < segments; s++) {
            String id = "s" + s + "_d0";
            assertEquals(DocWriteResponse.Result.DELETED, deleteDoc(id).getResult());
            deleted.add(id);
        }
        refresh();

        ForceMergeResponse fm = forceMergeToOne();
        assertEquals(0, fm.getFailedShards());

        int survivors = total - deleted.size();

        // Physical parquet truth: exactly `survivors` rows across all merged segments.
        assertEquals("merged parquet must physically drop pre-merge deletes", survivors, (int) parquetRows());
        assertParquetFileRowCountsMatchCatalog();

        // Every deleted id is unresolvable; every survivor resolves.
        for (String id : deleted) {
            assertFalse("deleted-before-merge doc must be physically gone: " + id, exists(id));
        }
        for (int s = 0; s < segments; s++) {
            for (int i = 1; i < perSegment; i++) {
                assertTrue("survivor must resolve after merge", exists("s" + s + "_d" + i));
            }
        }

        // Cross-format alignment: merged parquet rows == merged lucene docs, row_id sequential.
        assertCrossFormatRowAligned(survivors);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 2 — ~12% deletes spread across many segments, then force-merge to one
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Deletes ~12% of docs spread deterministically across many segments, then force-merges to a
     * single segment. Asserts the exact survivor count physically, every survivor/deleted id, and
     * full cross-format row alignment.
     */
    public void testPartialDeletesAcrossSegmentsThenForceMerge() throws Exception {
        createIndex();

        int perSegment = 25;
        int segments = 8;
        int total = perSegment * segments;
        List<String> allIds = new ArrayList<>();
        for (int s = 0; s < segments; s++) {
            for (int i = 0; i < perSegment; i++) {
                String id = "seg" + s + "_" + i;
                assertEquals(RestStatus.CREATED, indexDoc(id, s * perSegment + i).status());
                allIds.add(id);
            }
            refresh();
        }

        // Delete deterministically: every 8th doc ≈ 12.5%.
        Set<String> deleted = new HashSet<>();
        for (int idx = 0; idx < allIds.size(); idx++) {
            if (idx % 8 == 0) {
                String id = allIds.get(idx);
                assertEquals(DocWriteResponse.Result.DELETED, deleteDoc(id).getResult());
                deleted.add(id);
            }
        }
        refresh();

        assertEquals(0, forceMergeToOne().getFailedShards());

        int survivors = total - deleted.size();
        assertEquals("physical survivor row count after ~12% delete + merge", survivors, (int) parquetRows());
        assertParquetFileRowCountsMatchCatalog();

        for (String id : allIds) {
            if (deleted.contains(id)) {
                assertFalse("deleted doc must be gone: " + id, exists(id));
            } else {
                assertTrue("survivor must resolve: " + id, exists(id));
            }
        }
        assertCrossFormatRowAligned(survivors);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 3 — heavy update churn then merge resolves to exactly one latest row per id
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Heavy same-id update churn across segments (each update = internal delete-old + index-new),
     * then a force-merge. The merged output must carry exactly one row per surviving id at the latest
     * version/value — no stale superseded copies survive the merge.
     */
    public void testUpdateChurnThenMergeResolvesToLatest() throws Exception {
        createIndex();

        int keys = 20;
        for (int k = 0; k < keys; k++) {
            assertEquals(DocWriteResponse.Result.CREATED, indexDoc("k" + k, 0).getResult());
        }
        refresh();

        // Each key updated several times, interleaved with refreshes so copies land across generations.
        int rounds = 5;
        for (int r = 1; r <= rounds; r++) {
            for (int k = 0; k < keys; k++) {
                IndexResponse resp = indexDoc("k" + k, r);
                assertEquals(DocWriteResponse.Result.UPDATED, resp.getResult());
            }
            refresh();
        }

        assertEquals(0, forceMergeToOne().getFailedShards());

        // Exactly one physical row per key must survive the merge (all stale copies compacted away).
        assertEquals("merged output must hold exactly one row per key", keys, (int) parquetRows());
        assertParquetFileRowCountsMatchCatalog();

        // Every key resolves to the latest value and version.
        for (int k = 0; k < keys; k++) {
            GetResponse g = client().prepareGet(INDEX, "k" + k).setRealtime(false).get();
            assertTrue("key must resolve after churn+merge", g.isExists());
            assertEquals("latest value must win", rounds, ((Number) g.getSourceAsMap().get("value")).intValue());
            assertEquals("version reflects all updates", (long) (rounds + 1), g.getVersion());
        }
        assertCrossFormatRowAligned(keys);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 4 — every doc in a generation deleted → generation dropped on merge
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Deleting every document of a generation, then merging, drops that generation entirely while
     * other generations survive.
     */
    public void testFullyDeletedGenerationDroppedOnMerge() throws Exception {
        createIndex();

        // Generation A — will be fully deleted.
        indexDoc("a0", 0);
        indexDoc("a1", 1);
        refresh();
        // Generation B — survives.
        indexDoc("b0", 2);
        indexDoc("b1", 3);
        refresh();

        assertEquals(4L, parquetRows());

        assertEquals(DocWriteResponse.Result.DELETED, deleteDoc("a0").getResult());
        assertEquals(DocWriteResponse.Result.DELETED, deleteDoc("a1").getResult());
        refresh();

        assertEquals(0, forceMergeToOne().getFailedShards());

        assertFalse(exists("a0"));
        assertFalse(exists("a1"));
        assertTrue(exists("b0"));
        assertTrue(exists("b1"));
        assertEquals("only generation B rows remain physically", 2L, parquetRows());
        assertParquetFileRowCountsMatchCatalog();
        assertCrossFormatRowAligned(2);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 5 — sorted index: delete then merge preserves sort AND alignment
    // ══════════════════════════════════════════════════════════════════════

    /**
     * On a sorted index, deleting a subset then merging must keep merged parquet sorted by value DESC,
     * physically drop the deletes, and keep parquet↔lucene row-aligned (RowIdMapping correct through
     * a reordering merge with deletes).
     */
    public void testSortedIndexDeleteThenMergePreservesSortAndAlignment() throws Exception {
        createIndex(true);

        int total = 40;
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            String id = "d" + i;
            // distinct values so sort order is unambiguous
            assertEquals(RestStatus.CREATED, indexDoc(id, i).status());
            ids.add(id);
            if (i % 10 == 9) {
                refresh();
            }
        }
        refresh();

        Set<String> deleted = new HashSet<>();
        for (int i = 0; i < total; i += 5) {
            assertEquals(DocWriteResponse.Result.DELETED, deleteDoc("d" + i).getResult());
            deleted.add("d" + i);
        }
        refresh();

        assertEquals(0, forceMergeToOne().getFailedShards());

        int survivors = total - deleted.size();
        assertEquals(survivors, (int) parquetRows());
        assertParquetFileRowCountsMatchCatalog();
        for (String id : ids) {
            assertEquals("resolution after sorted merge: " + id, deleted.contains(id) == false, exists(id));
        }
        assertMergedParquetSortedByValueDesc();
        assertCrossFormatRowAligned(survivors);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 6 — deleted doc that survives one merge is physically dropped on the NEXT merge
    // ══════════════════════════════════════════════════════════════════════

    /**
     * A doc deleted after a merge (so it is dead-but-present at the point of that merge's snapshot in
     * a subsequent generation) is physically removed by a following merge: once its {@code .liv} is in
     * the next frozen snapshot, the merge drops the row. Proves deletes are eventually reclaimed, not
     * retained forever.
     */
    public void testDeletedDocDroppedOnNextMerge() throws Exception {
        createIndex();

        for (int i = 0; i < 6; i++) {
            assertEquals(RestStatus.CREATED, indexDoc("d" + i, i).status());
        }
        refresh();
        for (int i = 6; i < 12; i++) {
            assertEquals(RestStatus.CREATED, indexDoc("d" + i, i).status());
        }
        refresh();

        // First merge — no deletes yet; all 12 rows present.
        assertEquals(0, forceMergeToOne().getFailedShards());
        assertEquals(12L, parquetRows());

        // Delete two docs and add a fresh segment so a second force-merge has something to merge.
        assertEquals(DocWriteResponse.Result.DELETED, deleteDoc("d3").getResult());
        assertEquals(DocWriteResponse.Result.DELETED, deleteDoc("d9").getResult());
        indexDoc("d100", 100);
        refresh();

        // Second merge — the two deletes are now in the frozen snapshot and must be physically dropped.
        assertEquals(0, forceMergeToOne().getFailedShards());
        assertEquals("deletes must be physically reclaimed by the next merge", 11L, parquetRows());
        assertParquetFileRowCountsMatchCatalog();
        assertFalse(exists("d3"));
        assertFalse(exists("d9"));
        assertTrue(exists("d100"));
        assertCrossFormatRowAligned(11);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Scenario 7 — concurrent multi-shard ingestion + deletion under merges
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Concurrency in two phases: multiple threads first ingest disjoint id ranges in parallel; after a
     * refresh commits those rows, a disjoint subset of each thread's docs is deleted while a force-merge
     * runs concurrently. Correctness is verified by get-by-id: every survivor must resolve and every
     * deleted id must not — no lost or resurrected docs under concurrent ingest + delete + merge.
     */
    public void testConcurrentIngestAndDeleteUnderMerge() throws Exception {
        createConcurrentIndex(1);

        int threads = 4;
        int docsPerThread = 50;
        int deletesPerThread = 10;         // delete the first 10 of each thread's 50

        List<String> survivorIds = java.util.Collections.synchronizedList(new ArrayList<>());
        List<String> deletedIds = java.util.Collections.synchronizedList(new ArrayList<>());
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);

        // Phase 1: all threads ingest their disjoint id ranges concurrently, then a barrier + refresh
        // commits every row before any delete is issued.
        List<java.util.concurrent.Future<?>> ingest = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            ingest.add(pool.submit(() -> {
                int base = threadId * docsPerThread;
                for (int i = 0; i < docsPerThread; i++) {
                    indexDoc("t" + threadId + "_d" + (base + i), base + i);
                }
            }));
        }
        for (java.util.concurrent.Future<?> f : ingest) {
            f.get(60, java.util.concurrent.TimeUnit.SECONDS);
        }
        refresh();

        // Phase 2: delete a disjoint subset of each thread's (now-committed) docs, asserting each delete
        // takes effect, while a force-merge is fired concurrently to race the deletes against a merge.
        java.util.concurrent.Future<?> merger = pool.submit(() -> {
            try {
                forceMergeToOne();
            } catch (Exception ignored) {
                // a merge racing with in-flight deletes may no-op; the final merge below is authoritative
            }
        });
        for (int t = 0; t < threads; t++) {
            int base = t * docsPerThread;
            for (int i = 0; i < deletesPerThread; i++) {
                String id = "t" + t + "_d" + (base + i);
                assertEquals(DocWriteResponse.Result.DELETED, deleteDoc(id).getResult());
                deletedIds.add(id);
            }
            for (int i = deletesPerThread; i < docsPerThread; i++) {
                survivorIds.add("t" + t + "_d" + (base + i));
            }
        }
        merger.get(60, java.util.concurrent.TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS));

        // Authoritative final state: refresh, then a final force-merge so all deletes are reconciled.
        refresh();
        assertEquals(0, forceMergeToOne().getFailedShards());
        refresh();

        int expectedSurvivors = threads * (docsPerThread - deletesPerThread);
        assertEquals("survivor bookkeeping sanity", expectedSurvivors, survivorIds.size());

        // Correctness is verified by get-by-id: every concurrently-deleted id must be unresolvable and every
        // survivor must resolve — no lost or resurrected docs under concurrent ingest + delete + merge. The
        // physical parquet row count is not asserted here: a force-merge racing the deletes can collapse the
        // segments before the deletes commit, leaving them logically applied but not yet physically reclaimed
        // until a later multi-segment merge. Physical reclamation is covered by the sequential scenarios.
        for (String id : deletedIds) {
            assertFalse("concurrently-deleted doc must not resolve: " + id, exists(id));
        }
        for (String id : survivorIds) {
            assertTrue("survivor must resolve after concurrent churn + merge: " + id, exists(id));
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Verification helpers
    // ══════════════════════════════════════════════════════════════════════

    private IndexShard primary() {
        return getPrimaryShard(INDEX);
    }

    private Path parquetDir() {
        return primary().shardPath().getDataPath().resolve("parquet");
    }

    private Path luceneIndexDir() {
        return primary().shardPath().resolveIndex();
    }

    /** Every catalog parquet WriterFileSet's numRows must equal the actual on-disk file metadata. */
    private void assertParquetFileRowCountsMatchCatalog() throws IOException {
        Path dir = parquetDir();
        for (Segment segment : acquireAndGetSnapshot(INDEX).getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs == null) {
                continue;
            }
            long fileRows = 0;
            for (String file : wfs.files()) {
                Path fp = dir.resolve(file);
                assertTrue("catalog parquet file must exist on disk: " + fp, Files.exists(fp));
                ParquetFileMetadata md = RustBridge.getFileMetadata(fp.toString());
                fileRows += md.numRows();
            }
            assertEquals("catalog numRows must match on-disk parquet metadata for gen " + segment.generation(), wfs.numRows(), fileRows);
        }
    }

    /** merged parquet rows == merged lucene docs (numDocs), and row_id sequential per leaf. */
    private void assertCrossFormatRowAligned(int expectedRows) throws IOException {
        long actualParquetRows = parquetRows();
        assertEquals("parquet rows must match expected survivors", expectedRows, (int) actualParquetRows);

        Path luceneDir = luceneIndexDir();
        assertTrue("merged lucene dir must exist", Files.exists(luceneDir));
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("lucene numDocs must equal parquet rows (row-aligned)", expectedRows, reader.numDocs());
            for (LeafReaderContext ctx : reader.leaves()) {
                SortedNumericDocValues rowId = ctx.reader().getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
                if (rowId == null) {
                    continue;
                }
                long expected = 0;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    if (rowId.advanceExact(doc)) {
                        long v = rowId.nextValue();
                        assertEquals("row_id must be sequential (RowIdMapping applied) at doc " + doc, expected, v);
                        expected++;
                    }
                }
            }
        }
    }

    /** Merged parquet rows must be sorted by value DESC. */
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private void assertMergedParquetSortedByValueDesc() throws IOException {
        Path dir = parquetDir();
        for (Segment segment : acquireAndGetSnapshot(INDEX).getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs == null) {
                continue;
            }
            for (String file : wfs.files()) {
                String json = RustBridge.readAsJson(dir.resolve(file).toString());
                List<Map<String, Object>> rows = parseRows(json);
                for (int i = 1; i < rows.size(); i++) {
                    int prev = ((Number) rows.get(i - 1).get("value")).intValue();
                    int curr = ((Number) rows.get(i).get("value")).intValue();
                    assertTrue("value must be DESC, found " + prev + " before " + curr, prev >= curr);
                }
            }
        }
    }

    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> parseRows(String json) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            for (Object o : parser.list()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> row = (Map<String, Object>) o;
                rows.add(row);
            }
        }
        return rows;
    }
}
