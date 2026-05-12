/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration test for the Query-Then-Fetch (QTF) feature.
 *
 * <p>4 documents across 2 segments (2 per segment). Known data:
 * <pre>
 *   Segment 1 (row_base=0):
 *     row 0: name=alpha,  value=10, category=A
 *     row 1: name=beta,   value=20, category=B
 *   Segment 2 (row_base=2):
 *     row 0: name=gamma,  value=30, category=A  → global_row_id=2
 *     row 1: name=delta,  value=40, category=B  → global_row_id=3
 * </pre>
 *
 * <p>Expected global row IDs (shard-wide): alpha=0, beta=1, gamma=2, delta=3
 */
public class QueryThenFetchIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "qtf_row_id_e2e";

    private static boolean indexCreated = false;

    private void ensureIndexReady() throws IOException {
        if (indexCreated) {
            return;
        }
        createIndex();
        ingestSegment1();
        ingestSegment2();
        indexCreated = true;
    }

    // ── Test: full scan, all 4 docs sorted by value ─────────────────────────────

    /**
     * Full scan with __row_id__ + sort by value.
     * Expected order: alpha(0,10), beta(1,20), gamma(2,30), delta(3,40)
     * Row IDs must be unique and non-null.
     */
    public void testFullScanSortByValue() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME + " | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("Full scan should return 4 rows", 4, rows.size());

        // Verify sort order and names
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "beta", 20);
        assertRow(rows.get(2), "gamma", 30);
        assertRow(rows.get(3), "delta", 40);

        // Verify row IDs are unique and non-null
        assertRowIdsNonNullAndUnique(rows, 0);

        // Verify row IDs are sequential 0..3 (since data is ingested in order)
        List<Long> ids = extractRowIds(rows, 0);
        assertTrue("Row IDs should contain 0", ids.contains(0L));
        assertTrue("Row IDs should contain 1", ids.contains(1L));
        assertTrue("Row IDs should contain 2", ids.contains(2L));
        assertTrue("Row IDs should contain 3", ids.contains(3L));
    }

    // ── Test: filter category=A, rows from different segments ────────────────────

    /**
     * Filter category='A' hits alpha (segment 1, row_id=0) and gamma (segment 2, row_id=2).
     * Keyword equality filter triggers the indexed path (Lucene collector).
     */
    public void testFilterCategoryA() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where category = 'A' | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("Category A filter should return 2 rows", 2, rows.size());

        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "gamma", 30);

        assertRowIdsNonNullAndUnique(rows, 0);

        List<Long> ids = extractRowIds(rows, 0);
        assertTrue("alpha row_id should be 0", ids.get(0) == 0L);
        assertTrue("gamma row_id should be 2", ids.get(1) == 2L);
    }

    // ── Test: filter category=B, rows from different segments ────────────────────

    /**
     * Filter category='B' hits beta (segment 1, row_id=1) and delta (segment 2, row_id=3).
     * Keyword equality filter triggers the indexed path (Lucene collector).
     */
    public void testFilterCategoryB() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where category = 'B' | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("Category B filter should return 2 rows", 2, rows.size());

        assertRow(rows.get(0), "beta", 20);
        assertRow(rows.get(1), "delta", 40);

        assertRowIdsNonNullAndUnique(rows, 0);

        List<Long> ids = extractRowIds(rows, 0);
        assertTrue("beta row_id should be 1", ids.get(0) == 1L);
        assertTrue("delta row_id should be 3", ids.get(1) == 3L);
    }

    // ── Test: filter by name (keyword exact match) ───────────────────────────────

    /**
     * Exact keyword match on name field. Tests Lucene term query path.
     * name='gamma' is in segment 2 only → row_id=2.
     */
    public void testKeywordExactMatch() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where name = 'gamma' | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals(1, rows.size());
        assertEquals("gamma", rows.get(0).get(1));
        assertCellNumericEquals("value", 30, rows.get(0).get(2));
        assertEquals("gamma row_id should be 2", Long.valueOf(2L), toLong(rows.get(0).get(0)));
    }

    // ── Test: filter by name with OR (multiple keyword terms) ────────────────────

    /**
     * OR across keyword terms spanning segments.
     * name='alpha' (seg1, id=0) OR name='delta' (seg2, id=3).
     */
    public void testKeywordOrFilter() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where name = 'alpha' or name = 'delta' | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "delta", 40);

        List<Long> ids = extractRowIds(rows, 0);
        assertEquals("alpha row_id", Long.valueOf(0L), ids.get(0));
        assertEquals("delta row_id", Long.valueOf(3L), ids.get(1));
    }

    // ── Test: combined keyword + numeric filter ──────────────────────────────────

    /**
     * Combined filter: category='B' AND value > 25.
     * Only delta(40) matches (segment 2, row_id=3).
     */
    public void testCombinedKeywordAndNumericFilter() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where category = 'B' and value > 25 | fields __row_id__, name, value, category";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals(1, rows.size());
        assertEquals("delta", rows.get(0).get(1));
        assertCellNumericEquals("value", 40, rows.get(0).get(2));
        assertEquals("B", rows.get(0).get(3));
        assertEquals("delta row_id should be 3", Long.valueOf(3L), toLong(rows.get(0).get(0)));
    }

    // ── Test: sort descending ────────────────────────────────────────────────────

    /**
     * Sort by value descending. Row IDs should still be globally correct.
     */
    public void testSortDescending() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME + " | sort - value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals(4, rows.size());

        // Descending order: delta(40), gamma(30), beta(20), alpha(10)
        assertRow(rows.get(0), "delta", 40);
        assertRow(rows.get(1), "gamma", 30);
        assertRow(rows.get(2), "beta", 20);
        assertRow(rows.get(3), "alpha", 10);

        assertRowIdsNonNullAndUnique(rows, 0);
    }

    // ── Test: equality filter on value ───────────────────────────────────────────

    /**
     * Exact match: value=30 should return only gamma (segment 2, row_id=2).
     */
    public void testEqualityFilter() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where value = 30 | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("value=30 should match 1 row", 1, rows.size());
        assertEquals("gamma", rows.get(0).get(1));
        assertCellNumericEquals("value", 30, rows.get(0).get(2));

        // gamma is row 0 in segment 2, global_row_id = 2
        Long rowId = toLong(rows.get(0).get(0));
        assertNotNull("Row ID must not be null", rowId);
        assertEquals("gamma global row_id should be 2", Long.valueOf(2L), rowId);
    }

    // ── Test: range filter (value > 15 AND value < 35) ──────────────────────────

    /**
     * Range filter crossing segment boundary:
     * beta(20) from segment 1, gamma(30) from segment 2.
     */
    public void testRangeFilter() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where value > 15 and value < 35 | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("Range filter should return 2 rows", 2, rows.size());

        assertRow(rows.get(0), "beta", 20);
        assertRow(rows.get(1), "gamma", 30);

        assertRowIdsNonNullAndUnique(rows, 0);

        List<Long> ids = extractRowIds(rows, 0);
        assertEquals("beta row_id should be 1", Long.valueOf(1L), ids.get(0));
        assertEquals("gamma row_id should be 2", Long.valueOf(2L), ids.get(1));
    }

    // ── Test: row_id with multiple projected columns ─────────────────────────────

    /**
     * Project __row_id__ alongside all data columns. Verifies that data columns
     * are not corrupted when row ID is computed.
     */
    public void testRowIdWithAllColumns() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | sort value | fields __row_id__, name, value, category";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals(4, rows.size());

        // Verify all columns are present and correct
        assertEquals("alpha", rows.get(0).get(1));
        assertCellNumericEquals("value", 10, rows.get(0).get(2));
        assertEquals("A", rows.get(0).get(3));

        assertEquals("delta", rows.get(3).get(1));
        assertCellNumericEquals("value", 40, rows.get(3).get(2));
        assertEquals("B", rows.get(3).get(3));

        assertRowIdsNonNullAndUnique(rows, 0);
    }

    // ── Test: limit ──────────────────────────────────────────────────────────────

    /**
     * LIMIT 2 with sort. Should return first 2 by value: alpha, beta.
     */
    public void testSortWithLimit() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | sort value | fields __row_id__, name, value | head 2";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("LIMIT 2 should return 2 rows", 2, rows.size());

        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "beta", 20);

        assertRowIdsNonNullAndUnique(rows, 0);
    }

    // ── Test: no filter, just __row_id__ ─────────────────────────────────────────

    /**
     * Project only __row_id__. All 4 values must be unique and cover 0..3.
     */
    public void testRowIdOnly() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME + " | sort __row_id__ | fields __row_id__";
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals(4, rows.size());

        List<Long> ids = extractRowIds(rows, 0);
        assertEquals("Should have 4 unique IDs", 4, new HashSet<>(ids).size());

        // Sorted: should be 0, 1, 2, 3
        assertEquals(Long.valueOf(0L), ids.get(0));
        assertEquals(Long.valueOf(1L), ids.get(1));
        assertEquals(Long.valueOf(2L), ids.get(2));
        assertEquals(Long.valueOf(3L), ids.get(3));
    }

    // ── Index setup ─────────────────────────────────────────────────────────────

    private void createIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"value\": { \"type\": \"integer\" },"
            + "    \"category\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(
            client().performRequest(createIndex), "Create index"
        );
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void ingestSegment1() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"alpha\", \"value\": 10, \"category\": \"A\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"beta\", \"value\": 20, \"category\": \"B\"}\n");

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

    private void ingestSegment2() throws IOException {
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"gamma\", \"value\": 30, \"category\": \"A\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"delta\", \"value\": 40, \"category\": \"B\"}\n");

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

    // ══════════════════════════════════════════════════════════════════════════════
    // Strategy tests: dynamically switch fetch_strategy and verify correctness.
    // ══════════════════════════════════════════════════════════════════════════════

    /**
     * ListingTable strategy (1): ShardTableProvider + ProjectRowIdOptimizer.
     * Full scan, no filter.
     * TODO: Requires logical-level analyzer (ProjectRowIdAnalyzer) integration.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0")
    public void testStrategyListingTable_NoFilter() throws IOException {
        ensureIndexReady();
        setFetchStrategy("listing_table");
        try {
            String ppl = "source = " + INDEX_NAME + " | sort value | fields __row_id__, name, value";
            List<List<Object>> rows = executePplRows(ppl);
            assertEquals(4, rows.size());
            assertRowIdsNonNullAndUnique(rows, 0);
        } finally {
            setFetchStrategy("indexed");
        }
    }

    /**
     * ListingTable strategy (1) with keyword filter.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0")
    public void testStrategyListingTable_WithFilter() throws IOException {
        ensureIndexReady();
        setFetchStrategy("listing_table");
        try {
            String ppl = "source = " + INDEX_NAME
                + " | where category = 'A' | sort value | fields __row_id__, name, value";
            List<List<Object>> rows = executePplRows(ppl);
            assertEquals(2, rows.size());
            assertRow(rows.get(0), "alpha", 10);
            assertRow(rows.get(1), "gamma", 30);
            assertRowIdsNonNullAndUnique(rows, 0);
        } finally {
            setFetchStrategy("indexed");
        }
    }

    /**
     * IndexedPredicateOnly strategy (2): FilterClass::None (no filter).
     */
    public void testStrategyIndexed_FilterClassNone() throws IOException {
        ensureIndexReady();
        setFetchStrategy("indexed");
        String ppl = "source = " + INDEX_NAME + " | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);
        assertEquals(4, rows.size());
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    /**
     * IndexedPredicateOnly strategy (2): predicate-only (value > 15).
     */
    public void testStrategyIndexed_PredicateOnly() throws IOException {
        ensureIndexReady();
        setFetchStrategy("indexed");
        String ppl = "source = " + INDEX_NAME
            + " | where value > 15 | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);
        assertEquals(3, rows.size());
        assertRow(rows.get(0), "beta", 20);
        assertRow(rows.get(1), "gamma", 30);
        assertRow(rows.get(2), "delta", 40);
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    /**
     * IndexedPredicateOnly strategy (2): SingleCollector (category='A').
     */
    public void testStrategyIndexed_SingleCollector() throws IOException {
        ensureIndexReady();
        setFetchStrategy("indexed");
        String ppl = "source = " + INDEX_NAME
            + " | where category = 'A' | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "gamma", 30);
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    /**
     * IndexedPredicateOnly strategy (2): Tree (OR of two keyword terms).
     */
    public void testStrategyIndexed_TreeFilter() throws IOException {
        ensureIndexReady();
        setFetchStrategy("indexed");
        String ppl = "source = " + INDEX_NAME
            + " | where name = 'alpha' or name = 'delta' | sort value | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "delta", 40);
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    /**
     * IndexedPredicateOnly strategy (2): SingleCollector + residual predicate.
     */
    public void testStrategyIndexed_SingleCollectorWithResidual() throws IOException {
        ensureIndexReady();
        setFetchStrategy("indexed");
        String ppl = "source = " + INDEX_NAME
            + " | where category = 'B' and value > 25 | fields __row_id__, name, value";
        List<List<Object>> rows = executePplRows(ppl);
        assertEquals(1, rows.size());
        assertEquals("delta", rows.get(0).get(1));
        assertCellNumericEquals("value", 40, rows.get(0).get(2));
        assertNotNull(toLong(rows.get(0).get(0)));
    }

    /**
     * Both strategies must produce identical row IDs for the same query.
     * TODO: Depends on ListingTable no-filter path (requires ProjectRowIdAnalyzer).
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0")
    public void testBothStrategiesProduceSameRowIds() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME + " | sort value | fields __row_id__, name, value";

        setFetchStrategy("listing_table");
        List<Long> listingIds;
        try {
            listingIds = extractRowIds(executePplRows(ppl), 0);
        } finally {
            setFetchStrategy("indexed");
        }

        List<Long> indexedIds = extractRowIds(executePplRows(ppl), 0);

        assertEquals("Both strategies should return same count", listingIds.size(), indexedIds.size());
        assertEquals("Row IDs must match between strategies", listingIds, indexedIds);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private void setFetchStrategy(String strategy) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"datafusion.indexed.fetch_strategy\": \"" + strategy + "\"}}");
        client().performRequest(request);
    }

    private List<List<Object>> executePplRows(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> parsed = assertOkAndParse(response, "PPL: " + ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) parsed.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        return rows;
    }

    private void assertRow(List<Object> row, String expectedName, int expectedValue) {
        assertEquals(expectedName, row.get(1));
        assertCellNumericEquals(expectedName + " value", expectedValue, row.get(2));
    }

    private void assertRowIdsNonNullAndUnique(List<List<Object>> rows, int colIdx) {
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            Long id = toLong(rows.get(i).get(colIdx));
            assertNotNull("__row_id__ must not be null at row " + i, id);
            assertTrue("Duplicate row_id " + id + " at row " + i, seen.add(id));
        }
    }

    private List<Long> extractRowIds(List<List<Object>> rows, int colIdx) {
        return rows.stream()
            .map(r -> toLong(r.get(colIdx)))
            .collect(Collectors.toList());
    }

    private static Long toLong(Object val) {
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    private static void assertCellNumericEquals(String message, Number expected, Object actual) {
        assertNotNull(message + " is null", actual);
        assertTrue(message + " not a Number: " + actual.getClass(), actual instanceof Number);
        assertEquals(message, expected.longValue(), ((Number) actual).longValue());
    }
}
