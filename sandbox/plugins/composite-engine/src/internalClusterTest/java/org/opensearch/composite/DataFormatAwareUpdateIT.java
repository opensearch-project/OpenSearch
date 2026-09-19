/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.RoutingMissingException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder;
import org.opensearch.index.reindex.ReindexModulePlugin;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequestBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertHiddenRowStillPresent;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertParquetLuceneAligned;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertReachableDocuments;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertRowsOnDisk;

/**
 * End-to-end update coverage for {@link org.opensearch.index.engine.DataFormatAwareEngine} on a
 * composite (parquet primary, lucene secondary) index. Pure-delete sequences live in
 * {@link DataFormatAwareDeleteIT}; what stays here is the update sequences, the update-API paths, the
 * sequences that interleave an update with a delete because the interleaving is the point, and the
 * append-only gating rows.
 *
 * <p>An update never rewrites a row. It appends a new one and hides the old one, and which mechanism
 * hides it depends on whether a refresh has published the superseded row yet: inside one refresh window
 * the position is queued in {@code LuceneWriter.positionalDeletes} and applied at that writer's flush,
 * while after a refresh the only handle left is a term delete on {@code _id} against the shared parent
 * {@code IndexWriter}. {@code index.refresh_interval} is -1 on every index here so that choice is
 * deterministic rather than a race with the refresh scheduler.
 *
 * <p>Reads are by id, never by search: {@code prepareSearch} is refused against a
 * {@code DataFormatAwareEngine} through the shard, so a realtime get (the version map) and a
 * non-realtime get (the published parquet row) are the two authoritative read paths.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DataFormatAwareUpdateIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "dfae_update";

    /**
     * The base list plus the reindex module, which owns the {@code delete_by_query} and
     * {@code update_by_query} transport actions.
     *
     * <p>Having those classes on the test compile classpath is not enough to call the APIs: a transport
     * action only exists on a node if the plugin that registers it is loaded, so without this override
     * {@link #testDeleteByQueryAndUpdateByQueryAreRefused} fails with "failed to find action" before the
     * request ever reaches the engine — which is the opposite of what it means to assert.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexModulePlugin.class);
        return plugins;
    }

    /**
     * Composite index (Lucene secondary for {@code _id} resolution) with auto-refresh disabled so the
     * "within refresh window" vs "across refresh" cases stay deterministic across the refresh boundary.
     */
    private void createManualRefreshIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
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

    /**
     * Sorted composite index ({@code index.sort.field=value} desc) — the SORTED flush path where a
     * same-generation create→update previously tripped {@code LuceneWriter.flush} ({@code maxDoc !=
     * docCount}). The two-phase flush retains the prior copy through the reorder and hides it via
     * liveDocs, keeping 1:1 alignment with parquet.
     */
    private void createManualRefreshSortedIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .putList("index.sort.field", "value")
            .putList("index.sort.order", "desc")
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX);
    }

    private IndexResponse indexDoc(String id, String name, int value) {
        return client().prepareIndex(INDEX).setId(id).setSource("name", name, "value", value).get();
    }

    private static String name(GetResponse r) {
        return (String) r.getSourceAsMap().get("name");
    }

    private static int value(GetResponse r) {
        return ((Number) r.getSourceAsMap().get("value")).intValue();
    }

    /**
     * Update the same {@code _id} before any refresh — the prior copy lives in the same
     * active writer. The realtime (version-map) GET must return the new doc immediately, and after a
     * refresh the parquet-row path must also resolve to the new doc.
     */
    public void testUpdateWithinRefreshWindow() throws IOException {
        createManualRefreshIndex();

        IndexResponse created = indexDoc("k1", "v_old", 1);
        assertEquals(RestStatus.CREATED, created.status());
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        assertEquals(1L, created.getVersion());

        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(RestStatus.OK, updated.status());
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        GetResponse realtime = client().prepareGet(INDEX, "k1").setRealtime(true).get();
        assertTrue("realtime get must see the update", realtime.isExists());
        assertEquals("v_new", name(realtime));
        assertEquals(2, value(realtime));
        assertEquals(2L, realtime.getVersion());

        refreshIndex(INDEX);
        GetResponse rows = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue("post-refresh row get must see the update", rows.isExists());
        assertEquals("v_new", name(rows));
        assertEquals(2, value(rows));
        assertEquals(2L, rows.getVersion());

        // Both copies of k1 landed in one generation, so the old
        // one can only have been hidden by position. Both rows are still on disk — parquet has no
        // tombstone — and exactly one of them is reachable.
        String context = "positional tombstone";
        expectedReachable.put("k1", updated.getSeqNo());
        assertAligned(context);
        assertOnDisk(context, 2);
        assertHiddenRowStillPresent(context, getPrimaryShard(INDEX), acquireAndGetSnapshot(INDEX), created.getSeqNo());
    }

    /**
     * Same-generation create→update on a sorted index used to
     * fail at {@code LuceneWriter.flush} ({@code maxDoc != docCount}). The two-phase flush retains
     * the prior copy through the reorder (hidden via liveDocs), so refresh must not throw and
     * get-by-id resolves the new doc.
     */
    public void testSortedUpdateWithinRefreshWindow() {
        createManualRefreshSortedIndex();

        IndexResponse created = indexDoc("k1", "v_old", 1);
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());

        // Update BEFORE refresh — prior copy in the same active writer, sorted index (the bug repro).
        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        GetResponse realtime = client().prepareGet(INDEX, "k1").setRealtime(true).get();
        assertTrue("realtime get must see the update", realtime.isExists());
        assertEquals("v_new", name(realtime));

        // Refresh triggers the sorted two-phase flush (reorder retaining both rows, then a
        // liveDocs-only delete of the prior copy). Pre-fix this threw at LuceneWriter.flush.
        refreshIndex(INDEX);
        GetResponse rows = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue("post-refresh row get must see the update on a sorted index", rows.isExists());
        assertEquals("v_new", name(rows));
        assertEquals(2, value(rows));
        assertEquals(2L, rows.getVersion());
    }

    /**
     * Update after the prior copy is committed to the parent. The old copy's buffered delete
     * drains to the parent before the next {@code addIndexes} folds the new segment, so the second
     * refresh returns only the new doc (a reversed drain order would leave a duplicate).
     */
    public void testUpdateAcrossRefresh() throws IOException {
        createManualRefreshIndex();

        IndexResponse created = indexDoc("k1", "v_old", 1);
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());

        IndexResponse anchor = indexDoc("k2", "anchor", 99);

        refreshIndex(INDEX);
        GetResponse beforeUpdate = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue(beforeUpdate.isExists());
        assertEquals("v_old", name(beforeUpdate));

        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        refreshIndex(INDEX);
        GetResponse rows = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue("post-refresh row get must see the update", rows.isExists());
        assertEquals("v_new", name(rows));
        assertEquals(2, value(rows));
        assertEquals(2L, rows.getVersion());

        // The old copy was published before the update arrived, so
        // its writer is gone and the only handle left on it is a term delete on _id. Both copies carry
        // the same _id, so a term delete issued against the wrong generation would hide the new copy
        // as well and k1 would disappear while the write reported success.
        String context = "term tombstone";
        expectedReachable.put("k1", updated.getSeqNo());
        expectedReachable.put("k2", anchor.getSeqNo());
        assertAligned(context);
        assertOnDisk(context, 3);
        assertHiddenRowStillPresent(context, getPrimaryShard(INDEX), acquireAndGetSnapshot(INDEX), created.getSeqNo());
    }

    /**
     * N successive updates of the same {@code _id}, with interleaved refreshes, must always resolve
     * to the latest content and monotonically increasing version.
     */
    public void testManyUpdatesResolveToLatest() {
        createManualRefreshIndex();

        indexDoc("anchor_0", "anchor", 0);

        IndexResponse created = indexDoc("k1", "v_0", 0);
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        assertEquals(1L, created.getVersion());

        int updates = 10;
        for (int i = 1; i <= updates; i++) {
            IndexResponse r = indexDoc("k1", "v_" + i, i);
            assertEquals("update " + i + " must be reported as an update", DocWriteResponse.Result.UPDATED, r.getResult());
            assertEquals(1L + i, r.getVersion());
            if (i % 3 == 0) {
                refreshIndex(INDEX);
                indexDoc("anchor_" + i, "anchor", i);
            }
        }
        refreshIndex(INDEX);

        GetResponse rows = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue(rows.isExists());
        assertEquals("v_" + updates, name(rows));
        assertEquals(updates, value(rows));
        assertEquals(1L + updates, rows.getVersion());

        for (int i = 0; i <= updates; i += 3) {
            GetResponse anchor = client().prepareGet(INDEX, "anchor_" + i).setRealtime(false).get();
            assertTrue("anchor_" + i + " must survive the update/drain stream", anchor.isExists());
        }
    }

    /**
     * A conditional update carrying a stale {@code if_seq_no} must be rejected with a
     * {@link VersionConflictEngineException}, and the document must retain the last good update.
     */
    public void testUpdateVersionConflict() {
        createManualRefreshIndex();

        IndexResponse created = indexDoc("k1", "v_old", 1);
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        long staleSeqNo = created.getSeqNo();
        long term = created.getPrimaryTerm();

        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());

        VersionConflictEngineException ex = expectThrows(
            VersionConflictEngineException.class,
            () -> client().prepareIndex(INDEX)
                .setId("k1")
                .setSource("name", "v_conflict", "value", 3)
                .setIfSeqNo(staleSeqNo)
                .setIfPrimaryTerm(term)
                .get()
        );
        assertTrue("expected a version-conflict message, got: " + ex.getMessage(), ex.getMessage().contains("version conflict"));

        GetResponse realtime = client().prepareGet(INDEX, "k1").setRealtime(true).get();
        assertTrue(realtime.isExists());
        assertEquals("v_new", name(realtime));
        assertEquals(2, value(realtime));
    }

    private DeleteResponse deleteDoc(String id) {
        return client().prepareDelete(INDEX, id).get();
    }

    private static final String NAME = "name";
    private static final String TITLE = "title";
    private static final String VALUE = "value";
    private static final String PRICE = "price";
    private static final String ACTIVE = "active";

    /** Updatable composite index with one column of each JSON-renderable, reconstructable type. */
    private void createManualRefreshMultiFieldIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
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
            .setMapping(NAME, "type=keyword", TITLE, "type=text", VALUE, "type=long", PRICE, "type=double", ACTIVE, "type=boolean")
            .get();
        ensureGreen(INDEX);
    }

    private IndexResponse indexMultiField(String id, String name, String title, long value, double price, boolean active) {
        return client().prepareIndex(INDEX).setId(id).setSource(NAME, name, TITLE, title, VALUE, value, PRICE, price, ACTIVE, active).get();
    }

    /** Asserts the five user columns of a parquet row or a GET source. */
    private static void assertFields(
        String label,
        Map<String, Object> row,
        String name,
        String title,
        long value,
        double price,
        boolean active
    ) {
        assertEquals(label + ": " + NAME, name, row.get(NAME));
        assertEquals(label + ": " + TITLE, title, row.get(TITLE));
        assertEquals(label + ": " + VALUE, value, ((Number) row.get(VALUE)).longValue());
        assertEquals(label + ": " + PRICE, price, ((Number) row.get(PRICE)).doubleValue(), 0.0);
        assertEquals(label + ": " + ACTIVE, active, row.get(ACTIVE));
    }

    private static long seqNo(Map<String, Object> row) {
        return ((Number) row.get("_seq_no")).longValue();
    }

    private static long version(Map<String, Object> row) {
        return ((Number) row.get("_version")).longValue();
    }

    /** Refreshes and flushes, then renders every published parquet file of the shard as rows. */
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> readParquetRows() throws IOException {
        refreshIndex(INDEX);
        flushIndex(INDEX);
        Path parquetDir = getPrimaryShard(INDEX).shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Segment segment : acquireAndGetSnapshot(INDEX).getSegments()) {
            WriterFileSet parquetFiles = segment.dfGroupedSearchableFiles().get("parquet");
            if (parquetFiles == null) {
                continue;
            }
            for (String file : parquetFiles.files()) {
                String json = RustBridge.readAsJson(parquetDir.resolve(file).toString());
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
                        row.put("__generation__", segment.generation());
                        rows.add(row);
                    }
                }
            }
        }
        return rows;
    }

    /** The one parquet row written by the operation that was assigned {@code seqNo}. */
    private static Map<String, Object> rowAtSeqNo(List<Map<String, Object>> rows, long seqNo) {
        List<Map<String, Object>> matches = rows.stream().filter(r -> seqNo(r) == seqNo).collect(Collectors.toList());
        assertEquals("exactly one parquet row must carry _seq_no=" + seqNo + " in " + rows, 1, matches.size());
        return matches.get(0);
    }

    /** Optional: the superseded row, when a merge has not yet compacted it away. */
    private static Map<String, Object> rowAtSeqNoIfPresent(List<Map<String, Object>> rows, long seqNo) {
        return rows.stream().filter(r -> seqNo(r) == seqNo).findFirst().orElse(null);
    }

    /**
     * A full-document reindex of an existing id across a refresh appends a complete new parquet row
     * carrying every new value; the original row is never rewritten in place.
     */
    public void testFullUpdateWritesCompleteNewParquetRow() throws IOException {
        createManualRefreshMultiFieldIndex();
        IndexResponse created = indexMultiField("k1", "old", "old title", 1L, 1.5, true);
        refreshIndex(INDEX);

        IndexResponse updated = indexMultiField("k1", "new", "new title", 2L, 2.5, false);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        List<Map<String, Object>> rows = readParquetRows();
        Map<String, Object> newRow = rowAtSeqNo(rows, updated.getSeqNo());
        assertFields("parquet row after full update", newRow, "new", "new title", 2L, 2.5, false);
        assertEquals(2L, version(newRow));

        Map<String, Object> oldRow = rowAtSeqNoIfPresent(rows, created.getSeqNo());
        if (oldRow != null) {
            assertFields("superseded parquet row must be untouched", oldRow, "old", "old title", 1L, 1.5, true);
            assertEquals(1L, version(oldRow));
        }

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertFields("get after full update", get.getSourceAsMap(), "new", "new title", 2L, 2.5, false);
        assertEquals(2L, get.getVersion());
    }

    /** Same as above but without a refresh between create and update: both rows share one generation. */
    public void testFullUpdateWithinRefreshWindowLandsInOneGeneration() throws IOException {
        createManualRefreshMultiFieldIndex();
        IndexResponse created = indexMultiField("k1", "old", "old title", 1L, 1.5, true);
        IndexResponse updated = indexMultiField("k1", "new", "new title", 2L, 2.5, false);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());

        List<Map<String, Object>> rows = readParquetRows();
        Map<String, Object> newRow = rowAtSeqNo(rows, updated.getSeqNo());
        assertFields("parquet row after same-generation full update", newRow, "new", "new title", 2L, 2.5, false);
        assertEquals(2L, version(newRow));

        Map<String, Object> oldRow = rowAtSeqNoIfPresent(rows, created.getSeqNo());
        if (oldRow != null) {
            assertFields("superseded parquet row must be untouched", oldRow, "old", "old title", 1L, 1.5, true);
            assertEquals("both copies must sit in the same generation", oldRow.get("__generation__"), newRow.get("__generation__"));
        }

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertFields("get after same-generation full update", get.getSourceAsMap(), "new", "new title", 2L, 2.5, false);
    }

    /**
     * A partial update rewrites only the supplied field. The new parquet row must carry the updated
     * column and, for every column the request did not mention, the original value. The refresh before
     * the update forces its internal get onto the published parquet row rather than the translog, so
     * this exercises column reconstruction as the merge input.
     */
    public void testPartialUpdatePreservesUntouchedColumns() throws IOException {
        createManualRefreshMultiFieldIndex();
        indexMultiField("k1", "keep", "keep title", 1L, 1.5, true);
        refreshIndex(INDEX);

        UpdateResponse updated = client().prepareUpdate(INDEX, "k1").setDoc(VALUE, 42L).get();
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        List<Map<String, Object>> rows = readParquetRows();
        Map<String, Object> newRow = rowAtSeqNo(rows, updated.getSeqNo());
        assertFields("parquet row after partial update", newRow, "keep", "keep title", 42L, 1.5, true);
        assertEquals(2L, version(newRow));

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertFields("get after partial update", get.getSourceAsMap(), "keep", "keep title", 42L, 1.5, true);
        assertEquals(2L, get.getVersion());
    }

    /**
     * Two partial updates to different fields across a refresh. The second update's merge input is
     * the row written by the first, so each row must carry the cumulative state and nothing else.
     */
    public void testChainedPartialUpdatesAccumulateAcrossRefresh() throws IOException {
        createManualRefreshMultiFieldIndex();
        indexMultiField("k1", "keep", "keep title", 1L, 1.5, true);
        refreshIndex(INDEX);

        UpdateResponse first = client().prepareUpdate(INDEX, "k1").setDoc(VALUE, 10L).get();
        assertEquals(2L, first.getVersion());
        refreshIndex(INDEX);

        UpdateResponse second = client().prepareUpdate(INDEX, "k1").setDoc(ACTIVE, false, PRICE, 9.75).get();
        assertEquals(3L, second.getVersion());

        List<Map<String, Object>> rows = readParquetRows();
        Map<String, Object> afterFirst = rowAtSeqNoIfPresent(rows, first.getSeqNo());
        if (afterFirst != null) {
            assertFields("row after first partial update", afterFirst, "keep", "keep title", 10L, 1.5, true);
        }
        Map<String, Object> afterSecond = rowAtSeqNo(rows, second.getSeqNo());
        assertFields("row after second partial update", afterSecond, "keep", "keep title", 10L, 9.75, false);
        assertEquals(3L, version(afterSecond));

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertFields("get after chained partial updates", get.getSourceAsMap(), "keep", "keep title", 10L, 9.75, false);
        assertEquals(3L, get.getVersion());
    }

    /**
     * Id to the {@code _seq_no} of the version that must currently be reachable.
     *
     * <p>Keyed by id rather than by sequence number because an update replaces the entry, which is
     * exactly the property being asserted: one reachable row per id, carrying the newest version.
     */
    private final Map<String, Long> expectedReachable = new LinkedHashMap<>();

    private IndexResponse trackedIndex(String id, String name, int value) {
        IndexResponse response = indexDoc(id, name, value);
        expectedReachable.put(id, response.getSeqNo());
        return response;
    }

    private DeleteResponse trackedDelete(String id) {
        DeleteResponse response = deleteDoc(id);
        if (response.getResult() == DocWriteResponse.Result.DELETED) {
            expectedReachable.remove(id);
        }
        return response;
    }

    /**
     * Publishes the shard, then asserts that positions still line up between the two formats and that
     * exactly the tracked documents are reachable.
     */
    private void assertAligned(String context) throws IOException {
        refreshIndex(INDEX);
        flushIndex(INDEX);
        IndexShard shard = getPrimaryShard(INDEX);
        assertParquetLuceneAligned(context, shard, acquireAndGetSnapshot(INDEX));
        assertReachableDocuments(context, shard, acquireAndGetSnapshot(INDEX), expectedReachable);
    }

    /** Rows physically present across every published generation, reachable or hidden. */
    private void assertOnDisk(String context, int rows) throws IOException {
        assertRowsOnDisk(context, getPrimaryShard(INDEX), acquireAndGetSnapshot(INDEX), rows);
    }

    /**
     * Force merges to one segment and asserts the merge actually ran.
     *
     */
    private void forceMergeToOneSegment() throws IOException {
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        assertEquals(
            "the force merge must have combined the generations, otherwise nothing downstream is tested",
            1,
            acquireAndGetSnapshot(INDEX).getSegments().size()
        );
    }

    private boolean existsOnDisk(String id) {
        return client().prepareGet(INDEX, id).setRealtime(false).get().isExists();
    }

    /**
     * Bytes reported by {@code _stats} for the primary's store. Recorded in the log, never asserted on.
     */
    private long storeSizeInBytes() {
        return client().admin()
            .indices()
            .prepareStats(INDEX)
            .clear()
            .setStore(true)
            .get()
            .getIndex(INDEX)
            .getPrimaries()
            .getStore()
            .getSizeInBytes();
    }

    /**
     * Drops the index and recreates it empty, clearing the reachability bookkeeping with it.
     *
     */
    private void recreateIndex() {
        client().admin().indices().prepareDelete(INDEX).get();
        expectedReachable.clear();
        createManualRefreshIndex();
    }

    /**
     * An update must not be half-visible: in the window between
     * the write and the refresh the new content is reachable through the realtime path, and after the
     * refresh the published rows agree with it.
     *
     */
    public void testUpdateVisibilityBeforeAndAfterRefresh() throws IOException {
        createManualRefreshIndex();

        IndexResponse created = trackedIndex("k1", "v_old", 1);
        trackedIndex("k2", "keeper", 99);
        refreshIndex(INDEX);

        GetResponse published = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertEquals("the published row is the old value before the update", 1, value(published));

        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());

        GetResponse beforeRefresh = client().prepareGet(INDEX, "k1").setRealtime(true).get();
        assertTrue("the update must be reachable before any refresh", beforeRefresh.isExists());
        assertEquals("v_new", name(beforeRefresh));
        assertEquals(2, value(beforeRefresh));
        assertEquals(2L, beforeRefresh.getVersion());

        refreshIndex(INDEX);

        GetResponse afterRefresh = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue(afterRefresh.isExists());
        assertEquals("v_new", name(afterRefresh));
        assertEquals(2, value(afterRefresh));
        assertEquals(2L, afterRefresh.getVersion());

        String context = "visibility contract";
        expectedReachable.put("k1", updated.getSeqNo());
        assertAligned(context);
        assertOnDisk(context, 3);
        assertHiddenRowStillPresent(context, getPrimaryShard(INDEX), acquireAndGetSnapshot(INDEX), created.getSeqNo());
    }

    /**
     * update then delete, all inside one refresh window. Two rows exist for k1 and both
     * must end up hidden. An implementation that treats the delete as cancelling the update, or that
     * hides only the row its bookkeeping happened to point at, leaves one copy reachable and k1 comes
     * back from the dead.
     */
    public void testUpdateThenDeleteInOneWindowHidesBothCopies() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "v_old", 1);
        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());

        DeleteResponse deleted = trackedDelete("k1");
        assertEquals(DocWriteResponse.Result.DELETED, deleted.getResult());

        refreshIndex(INDEX);
        assertFalse("neither copy of k1 may be reachable", existsOnDisk("k1"));

        String context = "update then delete";
        assertAligned(context);
        assertOnDisk(context, 0);
    }

    /**
     * One id updated repeatedly with no refresh in between, so a single generation accumulates N rows
     * for k1 of which exactly one may be reachable. Run at N of 2, 10 and 1000 so the row count crosses
     * more than one parquet batch. What the merge then does with the superseded rows is
     * {@link #testRepeatedUpdatesInOneWindowAreReclaimedByAForceMerge}.
     */
    public void testRepeatedUpdatesInOneWindowResolveToLatest() throws IOException {
        for (int n : new int[] { 2, 10, 1000 }) {
            String context = "repeated updates in one window, n=" + n;
            createManualRefreshIndex();
            expectedReachable.clear();

            trackedIndex("k1", "v_1", 1);
            IndexResponse last = null;
            for (int v = 2; v <= n; v++) {
                last = indexDoc("k1", "v_" + v, v);
                assertEquals(context + ": update to " + v, DocWriteResponse.Result.UPDATED, last.getResult());
                assertEquals(context + ": version after update to " + v, (long) v, last.getVersion());
            }
            expectedReachable.put("k1", last.getSeqNo());

            refreshIndex(INDEX);
            GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
            assertTrue(context + ": k1 must be reachable", get.isExists());
            assertEquals(context + ": latest value", n, value(get));
            assertEquals(context + ": version", (long) n, get.getVersion());

            assertAligned(context);
            assertOnDisk(context, n);

            client().admin().indices().prepareDelete(INDEX).get();
        }
    }

    /**
     * The reclaim half of the same sequence — after the force merge the nine superseded copies of k1
     * must be gone from disk. {@link #testRepeatedUpdatesInOneWindowResolveToLatest} above establishes
     * that only the newest copy is ever reachable; this one establishes that the rest are physically
     * removed rather than merely hidden, which is a different failure and so is a separate test.
     *
     */
    public void testRepeatedUpdatesInOneWindowAreReclaimedByAForceMerge() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "v_1", 1);
        IndexResponse last = null;
        for (int v = 2; v <= 10; v++) {
            last = indexDoc("k1", "v_" + v, v);
        }
        expectedReachable.put("k1", last.getSeqNo());
        refreshIndex(INDEX);

        trackedIndex("k2", "second_generation", 99);
        refreshIndex(INDEX);

        // Ten rows for k1 in the first generation, of which nine are hidden, plus the bystander's row.
        assertOnDisk("before the force merge", 11);

        forceMergeToOneSegment();
        refreshIndex(INDEX);

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue("k1 must survive the merge", get.isExists());
        assertEquals("k1 must carry the last value written for it", 10, value(get));
        assertTrue("the bystander must survive the merge", existsOnDisk("k2"));

        String context = "repeated updates reclaimed by a force merge";
        assertAligned(context);
        // One row for k1 and one for the bystander. The nine superseded copies were dropped by the merge.
        assertOnDisk(context, 2);
    }

    /**
     * The {@code result} field, in single requests and per bulk item. This was hardcoded to
     * {@code created} before {@code #22406} and has no end-to-end guard, so every outcome is checked
     * in the one position that can produce it.
     */
    public void testResultFieldIsCorrectForEveryWriteOutcome() throws IOException {
        createManualRefreshIndex();

        assertEquals(DocWriteResponse.Result.CREATED, indexDoc("k1", "v_1", 1).getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, indexDoc("k1", "v_2", 2).getResult());
        assertEquals(DocWriteResponse.Result.DELETED, deleteDoc("k1").getResult());
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteDoc("k1").getResult());

        refreshIndex(INDEX);

        // One bulk request covering the same four outcomes plus a no-op, so per-item results are read
        // from the path that resolves ordering within a single request rather than across requests.
        indexDoc("k2", "noop_base", 5);
        refreshIndex(INDEX);

        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex(INDEX).setId("k3").setSource("name", "created", "value", 1))
            .add(client().prepareIndex(INDEX).setId("k3").setSource("name", "updated", "value", 2))
            .add(client().prepareDelete(INDEX, "k3"))
            .add(client().prepareDelete(INDEX, "k_absent"))
            .add(client().prepareUpdate(INDEX, "k2").setDoc("name", "noop_base", "value", 5))
            .get();

        BulkItemResponse[] items = bulk.getItems();
        assertEquals(5, items.length);
        for (BulkItemResponse item : items) {
            assertFalse("bulk item " + item.getItemId() + " failed: " + item.getFailureMessage(), item.isFailed());
        }
        assertEquals(DocWriteResponse.Result.CREATED, items[0].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, items[1].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.DELETED, items[2].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.NOT_FOUND, items[3].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.NOOP, items[4].getResponse().getResult());
    }

    /**
     * Repeated ids inside one bulk request. The ordering matrix above interleaves refreshes between
     * separate requests; here every write of an id arrives in the same request, so the ordering is
     * resolved inside the shard's bulk execution loop rather than across requests. That loop hides each
     * superseded copy by position, because no refresh can intervene between two items of one request.
     */
    public void testRepeatedIdsWithinOneBulkRequest() throws IOException {
        createManualRefreshIndex();

        // (a) three writes of the same new id. The first creates, the other two supersede.
        BulkResponse repeatedIndex = client().prepareBulk()
            .add(client().prepareIndex(INDEX).setId("k1").setSource(NAME, "v_1", VALUE, 1))
            .add(client().prepareIndex(INDEX).setId("k1").setSource(NAME, "v_2", VALUE, 2))
            .add(client().prepareIndex(INDEX).setId("k1").setSource(NAME, "v_3", VALUE, 3))
            .get();
        assertNoBulkFailures(repeatedIndex);
        assertEquals(DocWriteResponse.Result.CREATED, repeatedIndex.getItems()[0].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, repeatedIndex.getItems()[1].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, repeatedIndex.getItems()[2].getResponse().getResult());
        expectedReachable.put("k1", repeatedIndex.getItems()[2].getResponse().getSeqNo());
        refreshIndex(INDEX);

        GetResponse afterRepeatedIndex = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertEquals(3, value(afterRepeatedIndex));

        // (b) create, delete and recreate the same id in one request. The delete lands on a copy that is
        // still in the writer's buffer, and the recreate must not be caught by it even though both
        // copies carry the same _id.
        BulkResponse deleteThenRecreate = client().prepareBulk()
            .add(client().prepareIndex(INDEX).setId("k2").setSource(NAME, "v_1", VALUE, 1))
            .add(client().prepareDelete(INDEX, "k2"))
            .add(client().prepareIndex(INDEX).setId("k2").setSource(NAME, "v_3", VALUE, 3))
            .get();
        assertNoBulkFailures(deleteThenRecreate);
        assertEquals(DocWriteResponse.Result.CREATED, deleteThenRecreate.getItems()[0].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.DELETED, deleteThenRecreate.getItems()[1].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.CREATED, deleteThenRecreate.getItems()[2].getResponse().getResult());
        expectedReachable.put("k2", deleteThenRecreate.getItems()[2].getResponse().getSeqNo());
        refreshIndex(INDEX);

        GetResponse afterRecreate = client().prepareGet(INDEX, "k2").setRealtime(false).get();
        assertTrue("the recreate must survive the delete that preceded it in the same request", afterRecreate.isExists());
        assertEquals(3, value(afterRecreate));

        // (c) delete an id that has never existed, then create it, in that order in one request. The
        // not-found delete must leave nothing behind that could hide the create following it.
        BulkResponse deleteAbsentThenCreate = client().prepareBulk()
            .add(client().prepareDelete(INDEX, "k3"))
            .add(client().prepareIndex(INDEX).setId("k3").setSource(NAME, "v_1", VALUE, 1))
            .get();
        assertNoBulkFailures(deleteAbsentThenCreate);
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteAbsentThenCreate.getItems()[0].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.CREATED, deleteAbsentThenCreate.getItems()[1].getResponse().getResult());
        expectedReachable.put("k3", deleteAbsentThenCreate.getItems()[1].getResponse().getSeqNo());
        refreshIndex(INDEX);

        GetResponse afterCreate = client().prepareGet(INDEX, "k3").setRealtime(false).get();
        assertTrue("a create must not be hidden by a not-found delete of the same id", afterCreate.isExists());
        assertEquals(1, value(afterCreate));

        String context = "repeated ids within one bulk request";
        assertAligned(context);
        // Three rows for k1 with two hidden, two for k2 with one hidden, one for k3.
        assertOnDisk(context, 6);
    }

    private static void assertNoBulkFailures(BulkResponse bulk) {
        for (BulkItemResponse item : bulk.getItems()) {
            assertFalse("bulk item " + item.getItemId() + " failed: " + item.getFailureMessage(), item.isFailed());
        }
    }

    /**
     * Updates whose document is identical to what is already stored. The point is not the reported
     * {@code result} — it is whether the operation reaches the engine at all.
     *
     */
    public void testUpdatesThatChangeNothing() throws IOException {
        createManualRefreshIndex();

        IndexResponse created = trackedIndex("k1", "unchanging", 1);
        assertEquals(1L, created.getVersion());
        refreshIndex(INDEX);
        flushIndex(INDEX);
        long rowsBefore = 1;
        long storeBefore = storeSizeInBytes();
        assertOnDisk("before any no-op update", (int) rowsBefore);

        UpdateResponse firstNoop = client().prepareUpdate(INDEX, "k1").setDoc(NAME, "unchanging", VALUE, 1).get();
        assertEquals(DocWriteResponse.Result.NOOP, firstNoop.getResult());
        assertEquals("a no-op must not consume a version", 1L, firstNoop.getVersion());

        for (int i = 0; i < 1000; i++) {
            UpdateResponse noop = client().prepareUpdate(INDEX, "k1").setDoc(NAME, "unchanging", VALUE, 1).get();
            assertEquals("no-op " + i + " must report noop", DocWriteResponse.Result.NOOP, noop.getResult());
            assertEquals("no-op " + i + " must not consume a version", 1L, noop.getVersion());
        }

        String detected = "1000 detected no-ops";
        assertAligned(detected);
        assertOnDisk(detected, (int) rowsBefore);
        logger.info("detected no-ops: store size before [{}] after [{}]", storeBefore, storeSizeInBytes());

        GetResponse unchanged = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertEquals("unchanging", name(unchanged));
        assertEquals(1, value(unchanged));
        assertEquals(1L, unchanged.getVersion());

        // The contrast. Same thousand identical documents, detection switched off, so each one is a real
        // write: a new row appended and the previous one hidden.
        recreateIndex();
        trackedIndex("k1", "unchanging", 1);
        refreshIndex(INDEX);
        flushIndex(INDEX);
        long storeBeforeUndetected = storeSizeInBytes();

        long lastSeqNo = -1;
        for (int i = 0; i < 1000; i++) {
            UpdateResponse write = client().prepareUpdate(INDEX, "k1").setDoc(NAME, "unchanging", VALUE, 1).setDetectNoop(false).get();
            assertEquals("write " + i + " must be applied when detection is off", DocWriteResponse.Result.UPDATED, write.getResult());
            assertEquals("every applied write must consume a version", i + 2L, write.getVersion());
            lastSeqNo = write.getSeqNo();
        }
        expectedReachable.put("k1", lastSeqNo);

        String undetected = "1000 undetected no-ops";
        assertAligned(undetected);
        // The original row's generation was emptied by the first of the thousand and dropped, so what is
        // left on disk is the thousand rows the updates wrote, of which exactly one is reachable.
        assertOnDisk(undetected, 1000);
        logger.info("undetected no-ops: store size before [{}] after [{}]", storeBeforeUndetected, storeSizeInBytes());
    }

    /**
     * Mutations on an index whose mapping makes routing required. Every write and read of a routed
     * document must carry the routing value, and the ones that omit it must be rejected before they
     * reach the engine.
     *
     */
    @AwaitsFix(bugUrl = "Bug Identified in Custom Routing")
    public void testRoutedAndRequiredRoutingMutations() throws IOException {
        createRequiredRoutingIndex();
        String route = "route_a";

        IndexResponse created = client().prepareIndex(INDEX).setId("r1").setRouting(route).setSource(NAME, "routed", VALUE, 1).get();
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        expectedReachable.put("r1", created.getSeqNo());

        GetResponse routedGet = client().prepareGet(INDEX, "r1").setRouting(route).get();
        assertTrue(routedGet.isExists());
        assertEquals(route, routedGet.getField("_routing").getValue());

        UpdateResponse updated = client().prepareUpdate(INDEX, "r1").setRouting(route).setDoc(NAME, "routed", VALUE, 2).get();
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        expectedReachable.put("r1", updated.getSeqNo());
        refreshIndex(INDEX);
        assertEquals(2, value(client().prepareGet(INDEX, "r1").setRouting(route).setRealtime(false).get()));

        // Omitting the required routing value must be rejected by the coordinating node.
        expectThrows(RoutingMissingException.class, () -> client().prepareUpdate(INDEX, "r1").setDoc(NAME, "routed", VALUE, 3).get());
        expectThrows(RoutingMissingException.class, () -> client().prepareDelete(INDEX, "r1").get());
        assertEquals("a rejected mutation must not have been applied", 2, value(client().prepareGet(INDEX, "r1").setRouting(route).get()));

        // Routing carried on the individual actions of a bulk request.
        IndexResponse second = client().prepareIndex(INDEX).setId("r2").setRouting(route).setSource(NAME, "routed", VALUE, 10).get();
        expectedReachable.put("r2", second.getSeqNo());
        refreshIndex(INDEX);

        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareUpdate(INDEX, "r1").setRouting(route).setDoc(NAME, "routed", VALUE, 4))
            .add(client().prepareDelete(INDEX, "r2").setRouting(route))
            .get();
        assertNoBulkFailures(bulk);
        assertEquals(DocWriteResponse.Result.UPDATED, bulk.getItems()[0].getResponse().getResult());
        assertEquals(DocWriteResponse.Result.DELETED, bulk.getItems()[1].getResponse().getResult());
        expectedReachable.put("r1", bulk.getItems()[0].getResponse().getSeqNo());
        expectedReachable.remove("r2");

        // A routing value that was never used for this document. On one shard it resolves to the same
        // shard and therefore finds the document, so the write succeeds — recorded, not asserted away.
        UpdateResponse wrongRouting = client().prepareUpdate(INDEX, "r1").setRouting("route_b").setDoc(NAME, "routed", VALUE, 5).get();
        assertEquals(DocWriteResponse.Result.UPDATED, wrongRouting.getResult());
        expectedReachable.put("r1", wrongRouting.getSeqNo());

        String context = "required routing";
        assertAligned(context);
        // r1: one create plus three applied updates, three of them hidden. r2: one row, hidden by its
        // delete but kept on disk because r1's rows keep every generation alive.
        assertOnDisk(context, 5);
    }

    /**
     * Composite index whose mapping requires a routing value on every operation, with auto-refresh
     * disabled like the others so the refresh boundary stays under the test's control.
     */
    private void createRequiredRoutingIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", -1)
            .build();
        String mapping = "{\"_routing\":{\"required\":true},"
            + "\"properties\":{\"name\":{\"type\":\"keyword\"},\"value\":{\"type\":\"integer\"}}}";
        client().admin().indices().prepareCreate(INDEX).setSettings(settings).setMapping(mapping).get();
        ensureGreen(INDEX);
    }

    /**
     * The upsert paths of the update API. Each one decides between creating and updating by reading the
     * current document first, so on this engine each one depends on the realtime lookup resolving an
     * {@code _id} that may only exist in an unflushed writer.
     */
    public void testUpsertPaths() throws IOException {
        createManualRefreshIndex();

        // An explicit upsert document, used only when the id is missing. The doc differs from it, so the
        // two outcomes are distinguishable by content and not only by the reported result.
        UpdateResponse upsertCreated = client().prepareUpdate(INDEX, "u1")
            .setDoc(NAME, "doc_applied", VALUE, 2)
            .setUpsert(NAME, "upserted", VALUE, 1)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, upsertCreated.getResult());
        assertEquals(1L, upsertCreated.getVersion());
        expectedReachable.put("u1", upsertCreated.getSeqNo());
        GetResponse afterUpsert = client().prepareGet(INDEX, "u1").get();
        assertEquals("a create through upsert must store the upsert document, not the doc", "upserted", name(afterUpsert));
        assertEquals(1, value(afterUpsert));

        UpdateResponse upsertUpdated = client().prepareUpdate(INDEX, "u1")
            .setDoc(NAME, "doc_applied", VALUE, 2)
            .setUpsert(NAME, "upserted", VALUE, 1)
            .get();
        assertEquals(DocWriteResponse.Result.UPDATED, upsertUpdated.getResult());
        assertEquals(2L, upsertUpdated.getVersion());
        expectedReachable.put("u1", upsertUpdated.getSeqNo());
        GetResponse afterUpsertUpdate = client().prepareGet(INDEX, "u1").get();
        assertEquals("an update through upsert must apply the doc, not the upsert document", "doc_applied", name(afterUpsertUpdate));
        assertEquals(2, value(afterUpsertUpdate));

        // doc_as_upsert: the same document serves as both the partial update and the thing to create.
        UpdateResponse docAsUpsertCreated = client().prepareUpdate(INDEX, "u2")
            .setDoc(NAME, "from_doc", VALUE, 5)
            .setDocAsUpsert(true)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, docAsUpsertCreated.getResult());
        expectedReachable.put("u2", docAsUpsertCreated.getSeqNo());
        assertEquals("from_doc", name(client().prepareGet(INDEX, "u2").get()));

        UpdateResponse docAsUpsertUpdated = client().prepareUpdate(INDEX, "u2")
            .setDoc(NAME, "from_doc", VALUE, 6)
            .setDocAsUpsert(true)
            .get();
        assertEquals(DocWriteResponse.Result.UPDATED, docAsUpsertUpdated.getResult());
        expectedReachable.put("u2", docAsUpsertUpdated.getSeqNo());
        assertEquals(6, value(client().prepareGet(INDEX, "u2").get()));

        // No upsert document and doc_as_upsert left at its default, against an id that does not exist.
        DocumentMissingException missing = expectThrows(
            DocumentMissingException.class,
            () -> client().prepareUpdate(INDEX, "u3").setDoc(NAME, "never_created", VALUE, 7).get()
        );
        assertEquals(RestStatus.NOT_FOUND, missing.status());
        assertFalse("a rejected update must not have created the document", client().prepareGet(INDEX, "u3").get().isExists());

        // An upsert asking for the stored source back in the response.
        UpdateResponse withSource = client().prepareUpdate(INDEX, "u4")
            .setDoc(NAME, "ignored", VALUE, 0)
            .setUpsert(NAME, "fetched", VALUE, 9)
            .setFetchSource(true)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, withSource.getResult());
        expectedReachable.put("u4", withSource.getSeqNo());
        assertNotNull("fetch_source must return the document that was written", withSource.getGetResult());
        assertEquals("fetched", withSource.getGetResult().sourceAsMap().get(NAME));
        assertEquals(9, ((Number) withSource.getGetResult().sourceAsMap().get(VALUE)).intValue());

        String context = "upsert paths";
        assertAligned(context);
        // u1 and u2 each hold a create plus an update, one hidden apiece; u4 holds one row; u3 none.
        assertOnDisk(context, 5);

        // An upsert naming an index that does not exist. Recorded rather than asserted one way: index
        // creation here goes through auto-creation, which applies templates and cluster defaults, so
        // there is nothing that would make the new index composite unless a template says so.
        String absent = "dfae_update_absent";
        UpdateResponse autoCreated = client().prepareUpdate(absent, "a1")
            .setDoc(NAME, "x", VALUE, 1)
            .setUpsert(NAME, "auto", VALUE, 1)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, autoCreated.getResult());
        String dataFormatEnabled = client().admin()
            .indices()
            .prepareGetSettings(absent)
            .get()
            .getSetting(absent, "index.pluggable.dataformat.enabled");
        logger.info("auto-created index [{}] has index.pluggable.dataformat.enabled=[{}]", absent, dataFormatEnabled);
        assertEquals(
            "auto-creation applies templates and cluster defaults only, so the new index is a vanilla one",
            "false",
            dataFormatEnabled
        );
    }

    /**
     * A composite index that says nothing about append-only. The setting is deliberately absent so that
     * the derived default applies.
     *
     */
    private void createAppendOnlyIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.refresh_interval", -1)
            .build();
        client().admin().indices().prepareCreate(INDEX).setSettings(settings).setMapping(NAME, "type=keyword", VALUE, "type=integer").get();
        ensureGreen(INDEX);
    }

    /** The value {@code _settings} reports for a key, or null when the key is absent from the response. */
    private String reportedSetting(String key) {
        return client().admin().indices().prepareGetSettings(INDEX).get().getSetting(INDEX, key);
    }

    private static String describe(Exception failure) {
        return failure.getMessage() + " | cause: " + failure.getCause();
    }

    /**
     * a composite index left on the append-only default must refuse to mutate.
     *
     */
    public void testAppendOnlyDefaultGatesUpdatesAndDeletes() throws IOException {
        createAppendOnlyIndex();

        // The setting is absent from the index's settings entirely. Nothing writes it there: the flag is
        // derived inside IndexMetadata.Builder.build from index.pluggable.dataformat.enabled and never
        // stored back. So anyone reading _settings to find out whether an index is append-only learns
        // nothing — the key is missing while the index is append-only, and it is missing on a plain
        // index that is not. The behaviour below is the only reliable signal.
        assertNull(
            "the derived append-only flag is not reflected back into the index settings",
            reportedSetting(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey())
        );

        // An id-less create is the only allowed write.
        IndexResponse created = client().prepareIndex(INDEX).setSource(NAME, "appended", VALUE, 1).get();
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        String generatedId = created.getId();
        assertNotNull("an id-less create must report the id it generated", generatedId);

        String settingKey = IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey();

        Exception withCustomId = expectThrows(
            Exception.class,
            () -> client().prepareIndex(INDEX).setId("k1").setSource(NAME, "custom", VALUE, 1).get()
        );
        assertTrue(
            "indexing with a custom id must be refused by name: " + describe(withCustomId),
            describe(withCustomId).contains(settingKey)
        );
        assertTrue("the refusal must quote the rejected id: " + describe(withCustomId), describe(withCustomId).contains("k1"));

        Exception deleted = expectThrows(Exception.class, () -> client().prepareDelete(INDEX, generatedId).get());
        assertTrue("a delete must be refused by name: " + describe(deleted), describe(deleted).contains(settingKey));
        assertTrue("the refusal must name the operation: " + describe(deleted), describe(deleted).contains("DELETE"));

        // One bulk request holding every shape at once. Only the id-less create may survive, and the
        // three refusals must be reported per item rather than failing the whole request.
        BulkResponse mixed = client().prepareBulk()
            .add(client().prepareIndex(INDEX).setSource(NAME, "appended", VALUE, 2))
            .add(client().prepareIndex(INDEX).setId("k2").setSource(NAME, "custom", VALUE, 2))
            .add(client().prepareUpdate(INDEX, generatedId).setDoc(VALUE, 99))
            .add(client().prepareDelete(INDEX, generatedId))
            .get();
        assertFalse("the id-less create in the bulk must succeed", mixed.getItems()[0].isFailed());
        String secondGeneratedId = mixed.getItems()[0].getResponse().getId();
        for (int item = 1; item <= 3; item++) {
            BulkItemResponse refused = mixed.getItems()[item];
            assertTrue("bulk item " + item + " must be refused on an append-only index", refused.isFailed());
            assertTrue(
                "bulk item " + item + " must be refused by name: " + refused.getFailureMessage(),
                refused.getFailureMessage().contains(settingKey)
            );
        }

        // The single-document update API bypasses the bulk gate, and is still refused — but by the
        // custom-id rule rather than the update rule. TransportUpdateAction resolves the document, builds
        // an IndexRequest carrying the id it just resolved, and sends that through the bulk path, where
        // the gate sees an INDEX request with an id set. So the operation named in the refusal is INDEX
        // and the id quoted is the generated one, not anything the caller wrote.
        Exception updated = expectThrows(Exception.class, () -> client().prepareUpdate(INDEX, generatedId).setDoc(VALUE, 99).get());
        assertTrue("the update API must be refused by name: " + describe(updated), describe(updated).contains(settingKey));
        assertTrue(
            "the refusal arrives through the custom-id branch, quoting the resolved id: " + describe(updated),
            describe(updated).contains("custom document id " + generatedId)
        );

        // The shard has to still accept the write it is supposed to accept.
        IndexResponse afterwards = client().prepareIndex(INDEX).setSource(NAME, "appended", VALUE, 3).get();
        assertEquals(
            "an append-only index must still append after refusing a mutation",
            DocWriteResponse.Result.CREATED,
            afterwards.getResult()
        );
        assertNotNull("the id-less create inside the bulk must also report an id", secondGeneratedId);

        // Three appends landed and nothing was ever hidden, because every mutation was refused before it
        // reached a writer. Only the row count is checked. The alignment and reachability invariants both
        // read _seq_no doc values out of the lucene segment to join it to parquet by position, and an
        // append-only index does not write them: LuceneIndexingExecutionEngine sets
        // metadataDocValuesEnabled to isAppendOnlyIndex() == false and hands that to every
        // LuceneDocumentInput it builds. Nothing on such an index ever has to find a row by position, so
        // the doc values are left out and the invariants have nothing to key on.
        String context = "append-only default gates mutations";
        refreshIndex(INDEX);
        flushIndex(INDEX);
        assertOnDisk(context, 3);
    }

    /**
     * {@code delete_by_query} and {@code update_by_query} must fail rather than half
     * apply.
     *
     */
    public void testDeleteByQueryAndUpdateByQueryAreRefused() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "first", 1);
        trackedIndex("k2", "second", 2);
        trackedIndex("k3", "third", 3);
        refreshIndex(INDEX);

        String deleteOutcome = runByQuery(
            () -> new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE).source(INDEX)
                .filter(QueryBuilders.matchAllQuery())
                .refresh(true)
                .get()
        );
        logger.info("delete_by_query on a composite index: {}", deleteOutcome);
        assertByQueryFailedInTheSearchPhase("delete_by_query", deleteOutcome);

        String updateOutcome = runByQuery(
            () -> new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE).source(INDEX)
                .filter(QueryBuilders.matchAllQuery())
                .refresh(true)
                .get()
        );
        logger.info("update_by_query on a composite index: {}", updateOutcome);
        assertByQueryFailedInTheSearchPhase("update_by_query", updateOutcome);

        // Nothing may have moved. All three documents still readable, still at version 1, still holding
        // the values they were created with.
        for (Map.Entry<String, Integer> expected : Map.of("k1", 1, "k2", 2, "k3", 3).entrySet()) {
            GetResponse row = client().prepareGet(INDEX, expected.getKey()).setRealtime(false).get();
            assertTrue(expected.getKey() + " must have survived the refused by-query requests", row.isExists());
            assertEquals("no value may have been rewritten", expected.getValue().intValue(), value(row));
            assertEquals("no version may have been consumed", 1L, row.getVersion());
        }

        // And the index still works, so the refusal did not take the shard with it.
        assertEquals(DocWriteResponse.Result.UPDATED, trackedIndex("k1", "first", 11).getResult());
        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k2").getResult());

        String context = "by-query APIs refused";
        assertAligned(context);
        // Four rows: the three creates plus the update's new copy of k1. Two of them are hidden — the
        // original k1 by its update and k2 by its delete — and the generation survives because k3 and the
        // new k1 are still reachable in it.
        assertOnDisk(context, 4);
    }

    /**
     * Asserts the request died in its search phase rather than anywhere later.
     *
     */
    private static void assertByQueryFailedInTheSearchPhase(String api, String outcome) {
        assertTrue(api + " must have thrown rather than returned a partial result: " + outcome, outcome.startsWith("threw "));
        assertTrue(api + " must have failed on every shard, not some: " + outcome, outcome.contains("all shards failed"));
        assertTrue(
            api + " must have failed because the engine cannot be searched, before any write: " + outcome,
            outcome.contains("DataFormatAwareEngine")
        );
    }

    /**
     * Runs a by-query request and returns a description of how it ended, failing the test if it ended by
     * claiming to have changed something.
     *
     * <p>A {@code BulkByScrollResponse} does not throw on partial failure — it carries the failures
     * alongside the counts — so both endings have to be handled, and in both the counts are what decides
     * whether the guarantee held.
     */
    private String runByQuery(Supplier<BulkByScrollResponse> request) {
        BulkByScrollResponse response;
        try {
            response = request.get();
        } catch (Exception e) {
            return "threw " + describe(e);
        }
        assertEquals("a by-query request that cannot run must not report deletions", 0L, response.getDeleted());
        assertEquals("a by-query request that cannot run must not report updates", 0L, response.getUpdated());
        assertTrue(
            "a by-query request that changed nothing must say why, in a search or bulk failure",
            response.getBulkFailures().isEmpty() == false || response.getSearchFailures().isEmpty() == false
        );
        return "returned with search failures "
            + response.getSearchFailures()
            + " and bulk failures "
            + response.getBulkFailures()
            + " having deleted "
            + response.getDeleted()
            + " and updated "
            + response.getUpdated();
    }

    /**
     *  append-only is fixed at creation, in both directions.
     *
     */
    public void testAppendOnlySettingCannotBeChangedAfterCreation() throws IOException {
        String settingKey = IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey();

        createAppendOnlyIndex();
        Exception openingUp = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareUpdateSettings(INDEX).setSettings(Settings.builder().put(settingKey, false)).get()
        );
        assertTrue("the refusal must name the setting: " + describe(openingUp), describe(openingUp).contains(settingKey));
        assertTrue(
            "on an open index the non-dynamic check answers first: " + describe(openingUp),
            describe(openingUp).contains("non dynamic")
        );

        client().admin().indices().prepareClose(INDEX).get();
        Exception whileClosed = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareUpdateSettings(INDEX).setSettings(Settings.builder().put(settingKey, false)).get()
        );
        assertTrue(
            "closing the index must not make the setting updateable: " + describe(whileClosed),
            describe(whileClosed).contains(settingKey)
        );
        // Only here does the refusal say why: "final <index> setting [<key>], not updateable".
        assertTrue(
            "on a closed index the final check is the one that answers: " + describe(whileClosed),
            describe(whileClosed).contains("final") && describe(whileClosed).contains("not updateable")
        );
        client().admin().indices().prepareOpen(INDEX).get();
        ensureGreen(INDEX);

        // Still append-only, so a delete is still refused.
        IndexResponse appended = client().prepareIndex(INDEX).setSource(NAME, "appended", VALUE, 1).get();
        Exception stillRefused = expectThrows(Exception.class, () -> client().prepareDelete(INDEX, appended.getId()).get());
        assertTrue(
            "the failed settings update must not have relaxed anything: " + describe(stillRefused),
            describe(stillRefused).contains(settingKey)
        );

        // The other direction, on an index that was created mutable.
        client().admin().indices().prepareDelete(INDEX).get();
        expectedReachable.clear();
        createManualRefreshIndex();
        Exception sealing = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareUpdateSettings(INDEX).setSettings(Settings.builder().put(settingKey, true)).get()
        );
        assertTrue("a mutable index must not be sealable either: " + describe(sealing), describe(sealing).contains(settingKey));

        // And it is still mutable, so the rejected request changed nothing there either.
        assertEquals(DocWriteResponse.Result.CREATED, trackedIndex("k1", "mutable", 1).getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, trackedIndex("k1", "mutable", 2).getResult());
        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k1").getResult());

        String context = "append-only setting is final";
        assertAligned(context);
        // Nothing is left on disk. Both copies of k1 landed in one generation — the update hid the first
        // positionally and the delete hid the second — and a generation with every row hidden is dropped
        // from the catalog along with its file, so the whole generation went with them. The zero is the
        // point: the settings update was refused, and the index behaved exactly as a mutable one, not as
        // a half-sealed hybrid holding rows it cannot reclaim.
        assertOnDisk(context, 0);
    }
}
