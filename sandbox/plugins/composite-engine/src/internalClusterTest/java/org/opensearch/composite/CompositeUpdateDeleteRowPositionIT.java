/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.composite.CompositeUpdateDeleteInvariants.ParquetRow;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertHiddenRowStillPresent;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertParquetLuceneAligned;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertReachableDocuments;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.assertRowsOnDisk;
import static org.opensearch.composite.CompositeUpdateDeleteInvariants.liveRows;

/**
 * Checks that a parquet row and the lucene doc at the same position always describe the same
 * document, through every operation that can move a row: an update, a delete, a merge, and a flush
 * that physically reorders rows because the index is sorted.
 *
 * <p>{@link #indexDoc} and {@link #deleteDoc} keep {@link #expectedReachable} up to date as they go,
 * so a test states what it does and {@link #assertInvariants} checks the consequence without the test
 * restating the expected state by hand.
 *
 * @see CompositeUpdateDeleteInvariants for what each assertion checks and how
 */
public class CompositeUpdateDeleteRowPositionIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "row_position_test";

    /**
     * Id to the {@code _seq_no} of the version that should currently be reachable.
     *
     * <p>Keyed by id and not by sequence number because an update replaces the entry, which is exactly
     * the semantics being asserted: one reachable row per id, at the newest version.
     */
    private final Map<String, Long> expectedReachable = new LinkedHashMap<>();

    public void testPositionsStayAlignedWhenUpdateHitsTheSameGeneration() throws Exception {
        createMutableIndex();

        for (int i = 1; i <= 5; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        long supersededSeqNo = expectedReachable.get("k3");
        // No refresh in between, so k3's original row is still in the writer's buffer.
        indexDoc("k3", "v3_updated", 999);

        publish();
        assertInvariants("update inside the refresh window");
        assertRowsOnDisk("update inside the refresh window", shard(), snapshot(), 6);
        assertHiddenRowStillPresent("update inside the refresh window", shard(), snapshot(), supersededSeqNo);
        assertLiveValue("update inside the refresh window", "k3", "v3_updated", 999);
    }

    public void testPositionsStayAlignedWhenUpdateCrossesAGeneration() throws Exception {
        createMutableIndex();

        for (int i = 1; i <= 5; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        publish();
        long supersededSeqNo = expectedReachable.get("k3");

        indexDoc("k3", "v3_updated", 999);
        publish();

        assertInvariants("update across a generation");
        assertRowsOnDisk("update across a generation", shard(), snapshot(), 6);
        assertHiddenRowStillPresent("update across a generation", shard(), snapshot(), supersededSeqNo);
        assertLiveValue("update across a generation", "k3", "v3_updated", 999);
    }

    /**
     * Updates and a delete aimed at the first, last, and a middle position of a generation.
     *
     */
    public void testPositionsStayAlignedAtTheFirstLastAndMiddlePositionOfAGeneration() throws Exception {
        createMutableIndex();

        for (int i = 1; i <= 7; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        publish();

        indexDoc("k1", "first_updated", 101);   // position 0 of the generation
        indexDoc("k4", "middle_updated", 104);  // a middle position
        indexDoc("k7", "last_updated", 107);    // the final position
        deleteDoc("k2");
        publish();

        String context = "first, last and middle position";
        assertInvariants(context);
        // 7 original rows plus 3 update results; the delete of k2 adds nothing.
        assertRowsOnDisk(context, shard(), snapshot(), 10);
        assertLiveValue(context, "k1", "first_updated", 101);
        assertLiveValue(context, "k4", "middle_updated", 104);
        assertLiveValue(context, "k7", "last_updated", 107);
    }

    /**
     * A force merge renumbers positions from scratch, and does it twice over: the rust parquet merge
     * decides the new row order, and {@code RowIdRemappingOneMerge} decides the new lucene doc order.
     * The two are separate pieces of code that have to agree, and hidden rows are physically dropped
     * on the way, so every position in the result differs from what it was before.
     *
     */
    public void testPositionsStayAlignedAcrossAForceMerge() throws Exception {
        createMutableIndex();

        // Three generations, so the merge has something to combine.
        for (int i = 1; i <= 4; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        publish();
        indexDoc("k2", "v2_updated", 202);
        indexDoc("k5", "v5", 5);
        publish();
        deleteDoc("k1");
        indexDoc("k3", "v3_updated", 203);
        publish();

        assertInvariants("before the merge");

        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        publish();

        assertInvariants("after the merge");
        assertLiveValue("after the merge", "k2", "v2_updated", 202);
        assertLiveValue("after the merge", "k3", "v3_updated", 203);
    }

    /**
     * On a sorted index the rows are physically reordered when the generation flushes, so the
     * position a delete was recorded against during indexing is not the position the row ends up at.
     * Something has to translate between the two, and if it does not, the wrong row is hidden — the
     * document that was updated stays reachable in its old form and an untouched document disappears.
     *
     * <p>The values are chosen so that sorting on {@code value} descending is the exact reverse of
     * insertion order, which puts every row at a different position from the one it was written at.
     */
    public void testPositionsStayAlignedOnASortedIndexThatReordersRowsAtFlush() throws Exception {
        createMutableSortedIndex();

        // Inserted ascending, stored descending: every position moves.
        for (int i = 1; i <= 6; i++) {
            indexDoc("k" + i, "v" + i, i);
        }
        // Still inside the refresh window, so these become positional deletes against pre-sort
        // positions that the flush is about to move.
        indexDoc("k2", "v2_updated", 500);
        deleteDoc("k5");

        publish();
        String context = "sorted index reordering rows at flush";
        assertInvariants(context);
        assertLiveValue(context, "k2", "v2_updated", 500);
    }

    /**
     * A writer's positional deletes must not survive into the next writer. If the queue is shared
     * rather than per generation, then a delete recorded at position 1 of one generation also hides
     * position 1 of the next, and a document nobody touched silently disappears.
     *
     * <p>The first generation has its position 1 hidden by an update; the second generation is only
     * written to, never updated or deleted from, so all of its positions must stay reachable.
     */
    public void testDeletesFromOneGenerationDoNotHideRowsInTheNext() throws Exception {
        createMutableIndex();

        indexDoc("k1", "v1", 1);
        indexDoc("k2", "v2", 2);  // position 1 of the first generation
        indexDoc("k3", "v3", 3);
        indexDoc("k2", "v2_updated", 202);
        publish();

        // A second generation with no updates and no deletes at all.
        indexDoc("k4", "v4", 4);
        indexDoc("k5", "v5", 5);  // position 1 of the second generation
        indexDoc("k6", "v6", 6);
        publish();

        String context = "delete state must not leak between generations";
        assertInvariants(context);
        assertLiveValue(context, "k5", "v5", 5);
    }

    /**
     * A randomized mix of writes, updates and deletes across several generations, checked after every
     * generation.
     *
     */
    public void testPositionsStayAlignedUnderARandomMixOfUpdatesAndDeletes() throws Exception {
        createMutableIndex();

        int nextId = 1;
        int totalWrites = 0;
        int generations = randomIntBetween(3, 5);

        for (int generation = 1; generation <= generations; generation++) {
            int operations = randomIntBetween(3, 8);
            for (int op = 0; op < operations; op++) {
                // Only a create is possible while nothing is reachable yet.
                int choice = expectedReachable.isEmpty() ? 0 : randomIntBetween(0, 2);
                if (choice == 0) {
                    indexDoc("k" + nextId++, "created", generation);
                } else if (choice == 1) {
                    indexDoc(randomFrom(new ArrayList<>(expectedReachable.keySet())), "updated_g" + generation, 1000 + generation);
                } else {
                    deleteDoc(randomFrom(new ArrayList<>(expectedReachable.keySet())));
                }
                totalWrites++;
            }
            publish();
            assertInvariants("random mix, after generation " + generation);
        }

        assertTrue("the random mix performed no operations, so it checked nothing", totalWrites > 0);
    }

    // ── Index recipes ──

    /**
     * A composite index that accepts updates and deletes.
     *
     */
    private void createMutableIndex() {
        createMutableIndex(Settings.builder());
    }

    /** The same index with a sort, so rows are physically reordered when a generation flushes. */
    private void createMutableSortedIndex() {
        createMutableIndex(Settings.builder().putList("index.sort.field", "value").putList("index.sort.order", "desc"));
    }

    private void createMutableIndex(Settings.Builder extra) {
        Settings settings = extra.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
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
        expectedReachable.clear();
    }

    // ── Operations ──

    /**
     * Indexes or overwrites one document, and records that this id should now be reachable at the
     * {@code _seq_no} the operation was given.
     */
    private void indexDoc(String id, String name, int value) {
        IndexResponse response = client().prepareIndex().setIndex(INDEX).setId(id).setSource("name", name, "value", value).get();
        expectedReachable.put(id, response.getSeqNo());
    }

    /** Deletes one document, and records that no row for this id should be reachable any more. */
    private void deleteDoc(String id) {
        client().prepareDelete().setIndex(INDEX).setId(id).get();
        expectedReachable.remove(id);
    }

    /**
     * Refreshes then flushes, so both formats are readable at the same commit.
     *
     * <p>The invariants read the parquet files named by the catalog and the lucene directory at its
     * last commit, so a refresh alone would leave the two sides describing different points in time.
     */
    private void publish() {
        refreshIndex(INDEX);
        flushIndex(INDEX);
    }

    // ── Assertions ──

    /**
     * The two checks every test in this class makes: positions still line up, and exactly the expected
     * documents are reachable at exactly the expected versions.
     */
    private void assertInvariants(String context) throws IOException {
        IndexShard shard = shard();
        assertParquetLuceneAligned(context, shard, snapshot());
        assertReachableDocuments(context, shard, snapshot(), expectedReachable);
    }

    /**
     * Asserts the reachable row for an id carries the given column values.
     *
     */
    private void assertLiveValue(String context, String id, String name, int value) throws IOException {
        Long expectedSeqNo = expectedReachable.get(id);
        assertNotNull(context + ": the test does not expect [" + id + "] to be reachable at all", expectedSeqNo);

        ParquetRow found = null;
        for (ParquetRow row : liveRows(shard(), snapshot())) {
            if (row.seqNo() == expectedSeqNo) {
                found = row;
                break;
            }
        }
        assertNotNull(context + ": id [" + id + "] has no reachable row at _seq_no=" + expectedSeqNo, found);
        assertEquals(context + ": name column of the reachable row for [" + id + "]", name, found.columns().get("name"));
        assertEquals(
            context + ": value column of the reachable row for [" + id + "]",
            value,
            ((Number) found.columns().get("value")).intValue()
        );
    }

    private IndexShard shard() {
        return getPrimaryShard(INDEX);
    }

    private CatalogSnapshot snapshot() throws IOException {
        return acquireAndGetSnapshot(INDEX);
    }
}
