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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertHiddenRowStillPresent;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertParquetLuceneAligned;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertReachableDocuments;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertRowsOnDisk;

/**
 * Delete coverage for {@link org.opensearch.index.engine.DataFormatAwareEngine} on a composite
 * (parquet primary, lucene secondary) index.
 *
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DataFormatAwareDeleteIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "dfae_delete";

    /**
     * Composite index (lucene secondary for {@code _id} resolution) with auto-refresh disabled so the
     * "within refresh window" and "after a refresh" cases stay deterministic across the boundary.
     *
     * <p>{@code index.append_only.enabled} has to be set false explicitly. Left unset it takes its value
     * from {@code index.pluggable.dataformat.enabled}, which would make every index here append-only and
     * every delete below a validation failure.
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

    private IndexResponse indexDoc(String id, String name, int value) {
        return client().prepareIndex(INDEX).setId(id).setSource("name", name, "value", value).get();
    }

    private DeleteResponse deleteDoc(String id) {
        return client().prepareDelete(INDEX, id).get();
    }

    private static String name(GetResponse r) {
        return (String) r.getSourceAsMap().get("name");
    }

    private static int value(GetResponse r) {
        return ((Number) r.getSourceAsMap().get("value")).intValue();
    }

    /**
     * Id to the {@code _seq_no} of the version that must currently be reachable.
     *
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
     *
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

    /**
     * Delete then re-index with a refresh after every step. The re-indexed document is a
     * genuinely new one that happens to reuse a dead id, and the tombstone from the delete must not
     * reach forward and hide it. If delete state is keyed by id and never cleared, the new row is born
     * hidden: the write reports success and the document can never be read back.
     */
    public void testDeleteThenReindexWithRefreshBetweenEachStep() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "v_1", 1);
        trackedIndex("k2", "keeper", 99);
        refreshIndex(INDEX);

        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k1").getResult());
        refreshIndex(INDEX);
        assertFalse("k1 must be gone before it is re-indexed", existsOnDisk("k1"));

        IndexResponse reindexed = trackedIndex("k1", "v_3", 3);
        refreshIndex(INDEX);

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue("the re-indexed document must be reachable", get.isExists());
        assertEquals("v_3", name(get));
        assertEquals(3, value(get));
        assertEquals("the original value must never be observable again", reindexed.getSeqNo(), get.getSeqNo());

        // The bystander keeps generation 1 alive, so k1's original row is still on disk alongside it.
        String context = "delete then re-index, refresh between";
        assertAligned(context);
        assertOnDisk(context, 3);
    }

    /**
     * Delete then re-index with no refresh in between, so the write, the delete and the
     * re-index all compete inside one writer. This is where a positional delete sealed at the wrong
     * moment shows up: the queued position must resolve to the original row and not to the row the
     * re-index appended after it.
     */
    public void testDeleteThenReindexInsideOneRefreshWindow() throws IOException {
        createManualRefreshIndex();

        IndexResponse original = trackedIndex("k1", "v_1", 1);
        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k1").getResult());
        IndexResponse reindexed = trackedIndex("k1", "v_3", 3);

        refreshIndex(INDEX);

        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue("the re-indexed document must be reachable", get.isExists());
        assertEquals("v_3", name(get));
        assertEquals(3, value(get));

        String context = "delete then re-index, no refresh between";
        assertAligned(context);
        assertOnDisk(context, 2);
        assertHiddenRowStillPresent(context, getPrimaryShard(INDEX), acquireAndGetSnapshot(INDEX), original.getSeqNo());
    }

    /**
     * A delete must survive every boundary. Being recorded in memory is not enough: the tombstone has to
     * survive being written into a commit point and then having its segment rewritten by a merge. If it
     * is lost at any of those points the document silently comes back.
     *
     */
    public void testDeleteSurvivesRefreshAndFlush() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "v_1", 1);
        trackedIndex("k2", "keeper", 99);
        refreshIndex(INDEX);

        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k1").getResult());
        refreshIndex(INDEX);
        assertFalse("k1 must be gone after the refresh", existsOnDisk("k1"));

        flushIndex(INDEX);
        assertFalse("k1 must still be gone after the flush", existsOnDisk("k1"));
        assertTrue("the bystander must survive", existsOnDisk("k2"));

        // The plan allows either outcome here, so long as the old content is not resurrected: a
        // re-index of a deleted id creates the document again rather than updating the dead copy.
        IndexResponse again = indexDoc("k1", "v_reborn", 7);
        assertEquals("re-indexing a deleted id must create, not update", DocWriteResponse.Result.CREATED, again.getResult());
        expectedReachable.put("k1", again.getSeqNo());

        refreshIndex(INDEX);
        GetResponse get = client().prepareGet(INDEX, "k1").setRealtime(false).get();
        assertTrue(get.isExists());
        assertEquals("v_reborn", name(get));
        assertEquals("the pre-delete value must never reappear", 7, value(get));

        assertAligned("delete survives refresh and flush");
    }

    /**
     * The merge boundary — the same delete carried through a force merge, which rewrites the segment
     * holding the tombstone. The pre-merge half of the sequence is
     * {@link #testDeleteSurvivesRefreshAndFlush}.
     *
     */
    public void testDeleteSurvivesAForceMerge() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "v_1", 1);
        trackedIndex("k2", "keeper", 99);
        refreshIndex(INDEX);

        trackedIndex("k4", "second_generation", 4);
        refreshIndex(INDEX);

        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k1").getResult());
        refreshIndex(INDEX);
        flushIndex(INDEX);

        forceMergeToOneSegment();
        refreshIndex(INDEX);

        assertFalse("k1 must still be gone after the force merge", existsOnDisk("k1"));
        assertTrue("the bystander must survive the merge", existsOnDisk("k2"));
        assertTrue("the second generation must survive the merge", existsOnDisk("k4"));

        String context = "delete survives a force merge";
        assertAligned(context);
        assertOnDisk("the merge must have reclaimed the hidden row", 2);
    }

    /**
     * Deleting a document whose row a merge has already reclaimed. After the merge there is no longer
     * any row on disk to hide, so the second delete must report not-found and record nothing. A
     * tombstone written for a row that no longer exists is how a later document reusing that id ends up
     * hidden from birth.
     *
     */
    public void testDeletingADocumentWhoseRowWasAlreadyReclaimed() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "v_1", 1);
        trackedIndex("k2", "keeper", 99);
        refreshIndex(INDEX);

        trackedIndex("k4", "second_generation", 4);
        refreshIndex(INDEX);

        assertEquals(DocWriteResponse.Result.DELETED, trackedDelete("k1").getResult());
        refreshIndex(INDEX);

        forceMergeToOneSegment();
        refreshIndex(INDEX);

        DeleteResponse again = deleteDoc("k1");
        assertEquals("deleting an already-reclaimed row must report not-found", DocWriteResponse.Result.NOT_FOUND, again.getResult());
        assertEquals(RestStatus.NOT_FOUND, again.status());

        assertEquals("the shard must still be writable", DocWriteResponse.Result.CREATED, trackedIndex("k3", "after", 5).getResult());

        String context = "delete a merged-away document";
        assertAligned(context);
        // The bystander and the second generation's document survived the merge, k1's row was reclaimed
        // by it, and the not-found delete recorded nothing. The third row is k3, published by the flush
        // inside assertAligned.
        assertOnDisk(context, 3);
    }

    /**
     * deleting an {@code _id} that was never indexed. The cheapest case in the matrix
     * and the one most likely to be wrong: it must report not-found and leave nothing behind. If it
     * records a tombstone anyway, the first document ever written with that id is invisible from
     * birth, which is what the write and read after it check.
     */
    public void testDeleteOfAnIdThatNeverExisted() throws IOException {
        createManualRefreshIndex();

        trackedIndex("k1", "keeper", 1);
        refreshIndex(INDEX);

        DeleteResponse missing = deleteDoc("never_indexed");
        assertEquals(DocWriteResponse.Result.NOT_FOUND, missing.getResult());
        assertEquals(RestStatus.NOT_FOUND, missing.status());

        IndexResponse afterwards = trackedIndex("never_indexed", "born_after_its_tombstone", 4);
        assertEquals(DocWriteResponse.Result.CREATED, afterwards.getResult());
        refreshIndex(INDEX);

        GetResponse get = client().prepareGet(INDEX, "never_indexed").setRealtime(false).get();
        assertTrue("a document written after a not-found delete of its id must be reachable", get.isExists());
        assertEquals(4, value(get));

        String context = "delete an id that never existed";
        assertAligned(context);
        assertOnDisk(context, 2);
    }
}
