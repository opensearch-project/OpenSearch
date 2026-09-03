/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

/**
 * End-to-end update/delete coverage for {@link org.opensearch.index.engine.DataFormatAwareEngine}
 * on a composite (parquet primary + lucene secondary) index: same-generation update (Case 3),
 * update across refresh (Case 1, parent drain), N successive updates, version conflict, and
 * standalone delete on both sorted and unsorted indexes.
 *
 * <p>Assertions use get-by-id, not search: the DFA engine rejects the Lucene search path and
 * {@code getNumDocs()} is still a stub, so get-by-id (realtime version map + post-refresh parquet
 * row) is the authoritative read path.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DataFormatAwareUpdateIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "dfae_update";

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
    public void testUpdateWithinRefreshWindow() {
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
    public void testUpdateAcrossRefresh() {
        createManualRefreshIndex();

        IndexResponse created = indexDoc("k1", "v_old", 1);
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());

        indexDoc("k2", "anchor", 99);

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

    /**
     * A conditional delete carrying a stale {@code if_seq_no} must be rejected with a
     * {@link VersionConflictEngineException} (exercising delete()'s pre-flight early-result path),
     * and the document must survive.
     */
    public void testDeleteVersionConflict() {
        createManualRefreshIndex();

        IndexResponse created = indexDoc("k1", "v_old", 1);
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        long staleSeqNo = created.getSeqNo();
        long term = created.getPrimaryTerm();

        IndexResponse updated = indexDoc("k1", "v_new", 2);
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());

        VersionConflictEngineException ex = expectThrows(
            VersionConflictEngineException.class,
            () -> client().prepareDelete(INDEX, "k1").setIfSeqNo(staleSeqNo).setIfPrimaryTerm(term).get()
        );
        assertTrue("expected a version-conflict message, got: " + ex.getMessage(), ex.getMessage().contains("version conflict"));

        GetResponse realtime = client().prepareGet(INDEX, "k1").setRealtime(true).get();
        assertTrue("doc must survive a conflicting delete", realtime.isExists());
        assertEquals("v_new", name(realtime));
    }

    private DeleteResponse deleteDoc(String id) {
        return client().prepareDelete(INDEX, id).get();
    }

    /** Verifies delete visibility before and after refresh while a bystander keeps the segment live. */
    public void testPureDeleteVisibleAfterRefresh() {
        createManualRefreshIndex();
        indexDoc("k1", "v1", 1);
        indexDoc("k2", "keeper", 2);
        refreshIndex(INDEX);

        DeleteResponse del = deleteDoc("k1");
        assertEquals(DocWriteResponse.Result.DELETED, del.getResult());

        assertFalse("realtime get must not see the deleted doc", client().prepareGet(INDEX, "k1").setRealtime(true).get().isExists());

        refreshIndex(INDEX);
        assertFalse("post-refresh get must not see the deleted doc", client().prepareGet(INDEX, "k1").setRealtime(false).get().isExists());
        assertTrue("bystander must survive", client().prepareGet(INDEX, "k2").setRealtime(false).get().isExists());
    }

    /** Verifies that a partial delete hides the document without dropping its segment. */
    public void testPartialDeleteHidesDocButRetainsSegment() throws IOException {
        createManualRefreshIndex();
        indexDoc("x", "xx", 1);
        indexDoc("y", "yy", 2);
        refreshIndex(INDEX);

        deleteDoc("x");
        refreshIndex(INDEX);

        assertFalse("deleted doc hidden", client().prepareGet(INDEX, "x").setRealtime(false).get().isExists());
        assertTrue("sibling survives", client().prepareGet(INDEX, "y").setRealtime(false).get().isExists());
        assertEquals("partial delete must not drop the segment", 4L, getTotalRowCount(acquireAndGetSnapshot(INDEX)));
    }

    /** Verifies that deleting a segment's only document drops that generation from the catalog. */
    public void testDeleteFullyEmptiesSegmentDropsGeneration() throws IOException {
        createManualRefreshIndex();
        indexDoc("solo", "only", 1);
        refreshIndex(INDEX);

        assertEquals(2L, getTotalRowCount(acquireAndGetSnapshot(INDEX)));

        DeleteResponse del = deleteDoc("solo");
        assertEquals(DocWriteResponse.Result.DELETED, del.getResult());

        refreshIndex(INDEX);

        assertFalse("deleted doc must be gone", client().prepareGet(INDEX, "solo").setRealtime(false).get().isExists());
        assertEquals("fully-deleted generation must be dropped from the catalog", 0L, getTotalRowCount(acquireAndGetSnapshot(INDEX)));
    }

    /** Verifies that a fully deleted generation is dropped while other generations remain. */
    public void testDeleteAcrossGenerationsDropsOnlyEmptied() throws IOException {
        createManualRefreshIndex();
        indexDoc("a", "aa", 1);
        refreshIndex(INDEX);
        indexDoc("b", "bb", 2);
        refreshIndex(INDEX);

        deleteDoc("a");
        refreshIndex(INDEX);

        assertFalse("deleted doc gone", client().prepareGet(INDEX, "a").setRealtime(false).get().isExists());
        assertTrue("other-generation doc must survive", client().prepareGet(INDEX, "b").setRealtime(false).get().isExists());

        assertEquals("only the emptied generation is dropped", 2L, getTotalRowCount(acquireAndGetSnapshot(INDEX)));
    }
}
