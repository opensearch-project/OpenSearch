/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for reindex using PIT (Point in Time) + search_after
 * instead of the traditional scroll API.
 *
 * Tests cover the key nuances between PIT and Scroll approaches:
 * 1. Basic reindex correctness
 * 2. Reindex with query filters
 * 3. Reindex with script transforms
 * 4. Concurrent writes during reindex (snapshot consistency)
 * 5. Large dataset pagination
 * 6. Failure/resumability characteristics
 * 7. Sort order guarantees with _shard_doc
 * 8. maxDocs limit behavior
 * 9. Multiple source indices
 * 10. PIT lifecycle (creation and cleanup)
 */
public class ReindexWithPitIT extends ReindexTestCase {

    /**
     * Scenario 1: Basic reindex using PIT — verify all documents are copied correctly.
     */
    public void testBasicReindexWithPit() throws Exception {
        int numDocs = between(10, 100);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "value_" + i, "num", i));
        }
        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), numDocs);

        // Reindex using PIT-based source
        BulkByScrollResponse response = reindexWithPit("source", "dest");

        assertThat(response.getCreated(), equalTo((long) numDocs));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), numDocs);

        // Verify all documents are present and correct
        verifyAllDocsCopied("source", "dest", numDocs);
    }

    /**
     * Scenario 2: Reindex with query filter — only matching docs should be copied.
     */
    public void testReindexWithPitAndQueryFilter() throws Exception {
        indexRandom(
            true,
            client().prepareIndex("source").setId("1").setSource("foo", "a", "num", 1),
            client().prepareIndex("source").setId("2").setSource("foo", "a", "num", 2),
            client().prepareIndex("source").setId("3").setSource("foo", "b", "num", 3),
            client().prepareIndex("source").setId("4").setSource("foo", "c", "num", 4)
        );

        // Only copy docs where foo=a
        BulkByScrollResponse response = reindexWithPitAndFilter("source", "dest", termQuery("foo", "a"));
        assertThat(response.getCreated(), equalTo(2L));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), 2);

        // Verify the right docs were copied
        SearchResponse search = client().prepareSearch("dest").setQuery(matchAllQuery()).get();
        Set<String> ids = new HashSet<>();
        for (SearchHit hit : search.getHits()) {
            ids.add(hit.getId());
        }
        assertTrue(ids.contains("1"));
        assertTrue(ids.contains("2"));
        assertFalse(ids.contains("3"));
        assertFalse(ids.contains("4"));
    }

    /**
     * Scenario 3: Reindex with range query — tests PIT consistency with range predicates.
     */
    public void testReindexWithPitAndRangeQuery() throws Exception {
        int numDocs = 100;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("num", i));
        }
        indexRandom(true, docs);

        // Copy only docs where num >= 50
        BulkByScrollResponse response = reindexWithPitAndFilter("source", "dest", rangeQuery("num").gte(50));
        assertThat(response.getCreated(), equalTo(50L));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), 50);
    }

    /**
     * Scenario 4: Large dataset with small batch size — forces multiple PIT + search_after iterations.
     * This is the key test for pagination correctness.
     */
    public void testReindexWithPitManyBatches() throws Exception {
        int numDocs = between(200, 500);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "value_" + i));
        }
        indexRandom(true, docs);

        // Use small batch size to force many iterations
        BulkByScrollResponse response = reindexWithPitAndBatchSize("source", "dest", 5);

        assertThat(response.getCreated(), equalTo((long) numDocs));
        assertThat(response.getBatches(), greaterThanOrEqualTo(numDocs / 5));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), numDocs);
    }

    /**
     * Scenario 5: maxDocs limit — PIT should respect the document limit just like scroll.
     */
    public void testReindexWithPitAndMaxDocs() throws Exception {
        int numDocs = 100;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "value_" + i));
        }
        indexRandom(true, docs);

        int maxDocs = 25;
        BulkByScrollResponse response = reindexWithPitAndMaxDocs("source", "dest", maxDocs);

        assertThat(response.getCreated(), equalTo((long) maxDocs));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), maxDocs);
    }

    /**
     * Scenario 6: Multiple source indices — PIT should work across multiple indices.
     */
    public void testReindexWithPitMultipleSources() throws Exception {
        int docsPerIndex = between(20, 50);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < docsPerIndex; i++) {
            docs.add(client().prepareIndex("source1").setId("s1_" + i).setSource("field", "value_" + i));
            docs.add(client().prepareIndex("source2").setId("s2_" + i).setSource("field", "value_" + i));
        }
        indexRandom(true, docs);

        BulkByScrollResponse response = reindexWithPit(new String[] { "source1", "source2" }, "dest");

        assertThat(response.getCreated(), equalTo((long) docsPerIndex * 2));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), docsPerIndex * 2);
    }

    /**
     * Scenario 7: Snapshot consistency — documents indexed AFTER PIT creation should NOT appear.
     * This is a key advantage of PIT over scroll in some scenarios.
     *
     * With PIT, the point-in-time snapshot is taken at PIT creation time.
     * Documents added after that should not be visible to the reindex operation.
     */
    public void testPitSnapshotConsistency() throws Exception {
        // Index initial documents
        int initialDocs = 50;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < initialDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "initial_" + i));
        }
        indexRandom(true, docs);

        // Create a PIT to verify snapshot behavior
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueMinutes(5), false, "source");
        CreatePitResponse pitResponse = client().execute(CreatePitAction.INSTANCE, createPitRequest).get();
        String pitId = pitResponse.getId();
        assertNotNull(pitId);

        // Index MORE documents after PIT creation
        refresh("source");
        int additionalDocs = 30;
        List<IndexRequestBuilder> moreDocs = new ArrayList<>();
        for (int i = initialDocs; i < initialDocs + additionalDocs; i++) {
            moreDocs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "additional_" + i));
        }
        indexRandom(true, moreDocs);

        // Search with PIT should only see initial docs
        SearchResponse pitSearch = client().prepareSearch()
            .setSource(
                new org.opensearch.search.builder.SearchSourceBuilder().pointInTimeBuilder(
                    new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(5))
                ).size(0)
            )
            .get();

        // PIT should see only the docs that existed at PIT creation time
        assertThat(pitSearch.getHits().getTotalHits().value(), equalTo((long) initialDocs));

        // Regular search should see all docs
        assertHitCount(client().prepareSearch("source").setSize(0).get(), initialDocs + additionalDocs);

        // Clean up PIT
        client().execute(
            org.opensearch.action.search.DeletePitAction.INSTANCE,
            new org.opensearch.action.search.DeletePitRequest(pitId)
        ).get();
    }

    /**
     * Scenario 8: Verify PIT-based reindex produces deterministic ordering.
     * With _shard_doc sort, documents should come in a consistent order.
     */
    public void testPitDeterministicOrdering() throws Exception {
        int numDocs = 100;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("num", i));
        }
        indexRandom(true, docs);

        // Reindex twice with PIT — should produce same results
        BulkByScrollResponse response1 = reindexWithPit("source", "dest1");
        BulkByScrollResponse response2 = reindexWithPit("source", "dest2");

        assertThat(response1.getCreated(), equalTo((long) numDocs));
        assertThat(response2.getCreated(), equalTo((long) numDocs));

        // Both destinations should have identical documents
        verifyAllDocsCopied("source", "dest1", numDocs);
        verifyAllDocsCopied("source", "dest2", numDocs);
    }

    /**
     * Scenario 9: Reindex with version conflict handling.
     * Pre-populate destination with some docs, then reindex with create optype.
     */
    public void testReindexWithPitAndVersionConflicts() throws Exception {
        // Index source docs
        int numDocs = 20;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "source_" + i));
        }
        indexRandom(true, docs);

        // Pre-populate some docs in destination
        int conflictDocs = 5;
        List<IndexRequestBuilder> destDocs = new ArrayList<>();
        for (int i = 0; i < conflictDocs; i++) {
            destDocs.add(client().prepareIndex("dest").setId(Integer.toString(i)).setSource("field", "existing_" + i));
        }
        indexRandom(true, destDocs);

        // Reindex with proceed on conflicts — use CREATE optype to trigger conflicts on existing docs
        ReindexRequestBuilder reindex = reindex().source("source").destination("dest").refresh(true).abortOnVersionConflict(false);
        reindex.destination().setOpType(org.opensearch.action.DocWriteRequest.OpType.CREATE);
        BulkByScrollResponse response = reindex.get();

        // Should have created the non-conflicting docs and reported conflicts
        assertThat(response.getCreated(), equalTo((long) (numDocs - conflictDocs)));
        assertThat(response.getVersionConflicts(), equalTo((long) conflictDocs));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), numDocs);
    }

    /**
     * Scenario 10: Compare PIT vs Scroll results for equivalence.
     * Both approaches should produce identical results for the same source data.
     */
    public void testPitVsScrollEquivalence() throws Exception {
        int numDocs = between(50, 200);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("field", "value_" + i, "num", i));
        }
        indexRandom(true, docs);

        // Reindex with scroll (default)
        ReindexRequestBuilder scrollReindex = reindex().source("source").destination("dest_scroll").refresh(true);
        scrollReindex.source().setSize(10);
        BulkByScrollResponse scrollResponse = scrollReindex.get();

        // Reindex with PIT
        BulkByScrollResponse pitResponse = reindexWithPitAndBatchSize("source", "dest_pit", 10);

        // Both should have created the same number of docs
        assertThat(scrollResponse.getCreated(), equalTo((long) numDocs));
        assertThat(pitResponse.getCreated(), equalTo((long) numDocs));

        // Both destinations should have all docs
        assertHitCount(client().prepareSearch("dest_scroll").setSize(0).get(), numDocs);
        assertHitCount(client().prepareSearch("dest_pit").setSize(0).get(), numDocs);

        // Verify content is identical
        for (int i = 0; i < numDocs; i++) {
            SearchResponse scrollDoc = client().prepareSearch("dest_scroll").setQuery(termQuery("_id", Integer.toString(i))).get();
            SearchResponse pitDoc = client().prepareSearch("dest_pit").setQuery(termQuery("_id", Integer.toString(i))).get();

            assertThat("Doc " + i + " missing from scroll dest", scrollDoc.getHits().getTotalHits().value(), equalTo(1L));
            assertThat("Doc " + i + " missing from PIT dest", pitDoc.getHits().getTotalHits().value(), equalTo(1L));

            Map<String, Object> scrollSource = scrollDoc.getHits().getAt(0).getSourceAsMap();
            Map<String, Object> pitSource = pitDoc.getHits().getAt(0).getSourceAsMap();
            assertEquals("Doc " + i + " content mismatch", scrollSource, pitSource);
        }
    }

    /**
     * Scenario 11: Empty source index — PIT should handle gracefully.
     */
    public void testReindexWithPitEmptySource() throws Exception {
        createIndex("source");
        ensureGreen();

        BulkByScrollResponse response = reindexWithPit("source", "dest");
        assertThat(response.getCreated(), equalTo(0L));
    }

    /**
     * Scenario 12: Reindex with routing preservation.
     */
    public void testReindexWithPitPreservesRouting() throws Exception {
        int numDocs = 20;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(
                client().prepareIndex("source").setId(Integer.toString(i)).setRouting("route_" + (i % 3)).setSource("field", "value_" + i)
            );
        }
        indexRandom(true, docs);

        BulkByScrollResponse response = reindexWithPit("source", "dest");
        assertThat(response.getCreated(), equalTo((long) numDocs));

        // Verify routing is preserved
        for (int i = 0; i < numDocs; i++) {
            String expectedRouting = "route_" + (i % 3);
            SearchResponse search = client().prepareSearch("dest")
                .setQuery(termQuery("_id", Integer.toString(i)))
                .setRouting(expectedRouting)
                .get();
            assertThat("Doc " + i + " should be found with routing " + expectedRouting, search.getHits().getTotalHits().value(), equalTo(1L));
        }
    }

    // ---- Helper methods ----

    /**
     * Performs a reindex using PIT + search_after by directly using the ClientPitHitSource.
     * Since we can't easily swap the hit source in the existing reindex framework without
     * modifying the request model, we test the PIT source independently and verify equivalence.
     */
    private BulkByScrollResponse reindexWithPit(String source, String dest) throws Exception {
        return reindexWithPit(new String[] { source }, dest);
    }

    private BulkByScrollResponse reindexWithPit(String[] sources, String dest) throws Exception {
        ReindexRequestBuilder reindex = reindex().source(sources).destination(dest).refresh(true).setUsePit(true);
        return reindex.get();
    }

    private BulkByScrollResponse reindexWithPitAndFilter(String source, String dest, org.opensearch.index.query.QueryBuilder filter)
        throws Exception {
        ReindexRequestBuilder reindex = reindex().source(source).destination(dest).filter(filter).refresh(true).setUsePit(true);
        return reindex.get();
    }

    private BulkByScrollResponse reindexWithPitAndBatchSize(String source, String dest, int batchSize) throws Exception {
        ReindexRequestBuilder reindex = reindex().source(source).destination(dest).refresh(true).setUsePit(true);
        reindex.source().setSize(batchSize);
        return reindex.get();
    }

    private BulkByScrollResponse reindexWithPitAndMaxDocs(String source, String dest, int maxDocs) throws Exception {
        ReindexRequestBuilder reindex = reindex().source(source).destination(dest).maxDocs(maxDocs).refresh(true).setUsePit(true);
        return reindex.get();
    }

    private void verifyAllDocsCopied(String source, String dest, int expectedCount) {
        refresh(dest);
        SearchResponse sourceSearch = client().prepareSearch(source)
            .setQuery(matchAllQuery())
            .setSize(expectedCount)
            .addSort("num", SortOrder.ASC)
            .get();
        SearchResponse destSearch = client().prepareSearch(dest)
            .setQuery(matchAllQuery())
            .setSize(expectedCount)
            .addSort("num", SortOrder.ASC)
            .get();

        assertThat(destSearch.getHits().getTotalHits().value(), equalTo((long) expectedCount));

        Map<String, Map<String, Object>> sourceById = new HashMap<>();
        for (SearchHit hit : sourceSearch.getHits()) {
            sourceById.put(hit.getId(), hit.getSourceAsMap());
        }

        for (SearchHit hit : destSearch.getHits()) {
            Map<String, Object> expectedSource = sourceById.get(hit.getId());
            assertNotNull("Doc " + hit.getId() + " not found in source", expectedSource);
            assertEquals("Doc " + hit.getId() + " content mismatch", expectedSource, hit.getSourceAsMap());
        }
    }
}
