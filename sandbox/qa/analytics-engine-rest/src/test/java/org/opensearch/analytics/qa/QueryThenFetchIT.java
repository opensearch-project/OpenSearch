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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration test for the Query-Then-Fetch (QTF) feature.
 *
 * <p>Validates that PPL queries projecting {@code __row_id__} return unique, non-null
 * row identifiers across multiple segments. The indexed executor computes row IDs
 * positionally (global_base + rg.first_row + position_in_rg) so correctness across
 * segment boundaries is the key invariant under test.
 *
 * <p>Exercises the full path: PPL → planner → data node → indexed executor with
 * emit_row_ids=true → position-based row ID computation → results.
 */
public class QueryThenFetchIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "qtf_row_id_e2e";

    private static boolean indexCreated = false;

    /**
     * Lazily create the index and ingest documents. The index has two segments
     * (two flush operations) to verify cross-segment row ID uniqueness.
     */
    private void ensureIndexReady() throws IOException {
        if (indexCreated) {
            return;
        }
        createIndex();
        ingestSegment1();
        ingestSegment2();
        indexCreated = true;
    }

    /**
     * All rows returned with __row_id__ projection must have unique, non-null row IDs
     * and correct sort order by value ascending.
     */
    public void testRowIdFullScan() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME + " | sort value | fields __row_id__, name, value";
        Map<String, Object> response = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field", rows);
        assertEquals("Full scan should return 4 rows", 4, rows.size());

        // Verify sort order by value (ascending): 10, 20, 30, 40
        assertCellNumericEquals("Row 0 value", 10, rows.get(0).get(2));
        assertCellNumericEquals("Row 1 value", 20, rows.get(1).get(2));
        assertCellNumericEquals("Row 2 value", 30, rows.get(2).get(2));
        assertCellNumericEquals("Row 3 value", 40, rows.get(3).get(2));

        // Verify names match sorted order
        assertEquals("Row 0 name", "alpha", rows.get(0).get(1));
        assertEquals("Row 1 name", "beta", rows.get(1).get(1));
        assertEquals("Row 2 name", "gamma", rows.get(2).get(1));
        assertEquals("Row 3 name", "delta", rows.get(3).get(1));

        // Verify __row_id__ values are non-null and unique
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    /**
     * Filtered query returning rows from different segments must produce unique row IDs.
     * category='A' matches alpha (segment 1) and gamma (segment 2).
     */
    public void testRowIdWithFilter() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME
            + " | where category = 'A' | sort value | fields __row_id__, name, value";
        Map<String, Object> response = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field", rows);
        assertEquals("Filtered scan should return 2 rows (category A)", 2, rows.size());

        // Verify correct rows returned in sort order
        assertEquals("Row 0 name", "alpha", rows.get(0).get(1));
        assertCellNumericEquals("Row 0 value", 10, rows.get(0).get(2));
        assertEquals("Row 1 name", "gamma", rows.get(1).get(1));
        assertCellNumericEquals("Row 1 value", 30, rows.get(1).get(2));

        // Verify __row_id__ values are non-null and unique across segments
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    /**
     * Row IDs from the full scan must be globally unique — no overlap between
     * rows in segment 1 and segment 2.
     */
    public void testRowIdUniquenessAcrossSegments() throws IOException {
        ensureIndexReady();

        String ppl = "source = " + INDEX_NAME + " | fields __row_id__, name";
        Map<String, Object> response = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field", rows);
        assertEquals("Should return all 4 documents", 4, rows.size());

        // Collect all row IDs and verify uniqueness
        assertRowIdsNonNullAndUnique(rows, 0);
    }

    // ── Index setup helpers ─────────────────────────────────────────────────────

    private void createIndex() throws IOException {
        // Clean up if exists from a previous run
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
        assertEquals("Index creation should be acknowledged", true, response.get("acknowledged"));

        // Wait for green health
        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * Ingest first batch of documents and flush to create segment 1.
     */
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

        // Flush to create segment 1
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

    /**
     * Ingest second batch of documents and flush to create segment 2.
     */
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

        // Flush to create segment 2
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

    // ── Assertion helpers ───────────────────────────────────────────────────────

    /**
     * Assert that all row IDs at the given column index are non-null and unique.
     */
    private void assertRowIdsNonNullAndUnique(List<List<Object>> rows, int rowIdColumnIndex) {
        Set<Object> seenIds = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            Object rowId = rows.get(i).get(rowIdColumnIndex);
            assertNotNull("__row_id__ must not be null at row " + i, rowId);
            assertTrue(
                "__row_id__ must be unique — duplicate found at row " + i + ": " + rowId,
                seenIds.add(rowId)
            );
        }
        assertEquals(
            "Number of unique __row_id__ values must equal number of rows",
            rows.size(),
            seenIds.size()
        );
    }

    /**
     * Numeric-tolerant comparison (Jackson may parse integers as Integer or Long).
     */
    private static void assertCellNumericEquals(String message, Number expected, Object actual) {
        assertNotNull(message + " — actual value is null", actual);
        assertTrue(message + " — actual is not a Number: " + actual.getClass(), actual instanceof Number);
        assertEquals(message, expected.longValue(), ((Number) actual).longValue());
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
