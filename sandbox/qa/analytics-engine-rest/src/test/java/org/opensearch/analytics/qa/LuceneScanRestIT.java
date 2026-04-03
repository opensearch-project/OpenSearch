/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST-based integration tests for the Lucene-only scan path.
 * Exercises the full end-to-end flow: PPL query → analytics planner →
 * Substrait → Rust execution → JNI batch pull → result.
 */
public class LuceneScanRestIT extends OpenSearchRestTestCase {

    private static final Logger logger = LogManager.getLogger(LuceneScanRestIT.class);
    private static final String TEST_INDEX = "lucene_scan_test";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    // ── Task 8.1: Index creation ──────────────────────────────────────────────

    private void createTestIndex() throws IOException {
        // Delete if exists to ensure clean state (Req 9.4)
        try {
            client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
        } catch (Exception e) {
            // index doesn't exist, ignore
        }
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity(INDEX_MAPPING);
        client().performRequest(createIndex);
    }

    // ── Task 8.2: Test data insertion and cluster health ──────────────────────

    private void bulkInsertTestData() throws IOException {
        StringBuilder bulk = new StringBuilder();
        String action = "{\"index\":{\"_index\":\"" + TEST_INDEX + "\"}}\n";

        // Doc 1: active/electronics/US/1000/5/199.99
        bulk.append(action);
        bulk.append("{\"status\":\"active\",\"category\":\"electronics\",\"region\":\"US\",");
        bulk.append("\"amount\":1000,\"quantity\":5,\"price\":199.99}\n");

        // Doc 2: active/electronics/EU/2000/3/299.99
        bulk.append(action);
        bulk.append("{\"status\":\"active\",\"category\":\"electronics\",\"region\":\"EU\",");
        bulk.append("\"amount\":2000,\"quantity\":3,\"price\":299.99}\n");

        // Doc 3: inactive/clothing/US/500/10/49.99
        bulk.append(action);
        bulk.append("{\"status\":\"inactive\",\"category\":\"clothing\",\"region\":\"US\",");
        bulk.append("\"amount\":500,\"quantity\":10,\"price\":49.99}\n");

        // Doc 4: active/clothing/US/750/7/99.99
        bulk.append(action);
        bulk.append("{\"status\":\"active\",\"category\":\"clothing\",\"region\":\"US\",");
        bulk.append("\"amount\":750,\"quantity\":7,\"price\":99.99}\n");

        // Doc 5: inactive/electronics/EU/1500/2/399.99
        bulk.append(action);
        bulk.append("{\"status\":\"inactive\",\"category\":\"electronics\",\"region\":\"EU\",");
        bulk.append("\"amount\":1500,\"quantity\":2,\"price\":399.99}\n");

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder()
                .addHeader("Content-Type", "application/x-ndjson")
                .build()
        );
        Response bulkResponse = client().performRequest(bulkRequest);
        assertEquals("Bulk insert failed", 200, bulkResponse.getStatusLine().getStatusCode());
    }

    private void waitForClusterHealth() throws IOException {
        Request healthRequest = new Request("GET", "/_cluster/health/" + TEST_INDEX);
        healthRequest.addParameter("wait_for_status", "yellow");
        healthRequest.addParameter("timeout", "60s");
        client().performRequest(healthRequest);
    }

    // ── Task 8.3: PPL query execution and result verification ─────────────────

    public void testLuceneScanQueries() throws Exception {
        createTestIndex();
        bulkInsertTestData();
        client().performRequest(new Request("POST", "/" + TEST_INDEX + "/_refresh"));
        waitForClusterHealth();

        List<String> failures = new ArrayList<>();

        // q1: COUNT(*) no filter → 5
        try {
            runQuery("q1", "source=" + TEST_INDEX + " | stats count() as cnt", result -> {
                assertColumns(result, "q1", "cnt");
                assertSingleRow(result, "q1", "cnt", 5);
            });
        } catch (Exception e) {
            failures.add("q1: " + e.getMessage());
            logger.error("FAILED q1", e);
        }

        // q2: COUNT(*) with keyword filter → 3
        try {
            runQuery("q2", "source=" + TEST_INDEX + " | where status='active' | stats count() as cnt", result -> {
                assertColumns(result, "q2", "cnt");
                assertSingleRow(result, "q2", "cnt", 3);
            });
        } catch (Exception e) {
            failures.add("q2: " + e.getMessage());
            logger.error("FAILED q2", e);
        }

        // q3: SUM(amount) → 5750
        try {
            runQuery("q3", "source=" + TEST_INDEX + " | stats sum(amount) as total", result -> {
                assertColumns(result, "q3", "total");
                assertSingleRow(result, "q3", "total", 5750);
            });
        } catch (Exception e) {
            failures.add("q3: " + e.getMessage());
            logger.error("FAILED q3", e);
        }

        // q4: GROUP BY category with COUNT → electronics=3, clothing=2
        try {
            runQuery("q4", "source=" + TEST_INDEX + " | stats count() as cnt by category", result -> {
                assertNotNull("q4: response should contain 'columns'", result.get("columns"));
                assertNotNull("q4: response should contain 'rows'", result.get("rows"));

                @SuppressWarnings("unchecked")
                List<String> columns = (List<String>) result.get("columns");
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) result.get("rows");

                int cntIdx = columns.indexOf("cnt");
                int catIdx = columns.indexOf("category");
                assertTrue("q4: columns must contain 'cnt'", cntIdx >= 0);
                assertTrue("q4: columns must contain 'category'", catIdx >= 0);
                assertEquals("q4: expected 2 group rows", 2, rows.size());

                boolean foundElectronics = false;
                boolean foundClothing = false;
                for (List<Object> row : rows) {
                    String cat = String.valueOf(row.get(catIdx));
                    long cnt = ((Number) row.get(cntIdx)).longValue();
                    if ("electronics".equals(cat)) {
                        assertEquals("q4: electronics count", 3, cnt);
                        foundElectronics = true;
                    } else if ("clothing".equals(cat)) {
                        assertEquals("q4: clothing count", 2, cnt);
                        foundClothing = true;
                    }
                }
                assertTrue("q4: missing electronics row", foundElectronics);
                assertTrue("q4: missing clothing row", foundClothing);
            });
        } catch (Exception e) {
            failures.add("q4: " + e.getMessage());
            logger.error("FAILED q4", e);
        }

        // q5: projection → 5 rows, 2 columns (status, amount)
        try {
            runQuery("q5", "source=" + TEST_INDEX + " | fields status, amount", result -> {
                assertNotNull("q5: response should contain 'columns'", result.get("columns"));
                assertNotNull("q5: response should contain 'rows'", result.get("rows"));

                @SuppressWarnings("unchecked")
                List<String> columns = (List<String>) result.get("columns");
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) result.get("rows");

                assertEquals("q5: expected 2 columns", 2, columns.size());
                assertTrue("q5: columns must contain 'status'", columns.contains("status"));
                assertTrue("q5: columns must contain 'amount'", columns.contains("amount"));
                assertEquals("q5: expected 5 rows", 5, rows.size());
            });
        } catch (Exception e) {
            failures.add("q5: " + e.getMessage());
            logger.error("FAILED q5", e);
        }

        if (failures.isEmpty() == false) {
            fail(failures.size() + " Lucene-scan queries failed:\n  " + String.join("\n  ", failures));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void runQuery(String queryId, String ppl, QueryAssertion assertion) throws Exception {
        logger.info("=== LuceneScan {} ===\nPPL: {}", queryId, ppl);

        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);

        assertEquals("HTTP status for " + queryId, 200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        logger.info("RESULT {}: {}", queryId, responseMap);

        assertion.verify(responseMap);
        logger.info("SUCCESS {}", queryId);
    }

    @FunctionalInterface
    private interface QueryAssertion {
        void verify(Map<String, Object> result) throws Exception;
    }

    private void assertColumns(Map<String, Object> result, String queryId, String... expectedColumns) {
        assertNotNull(queryId + ": response should contain 'columns'", result.get("columns"));
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        for (String col : expectedColumns) {
            assertTrue(queryId + ": columns must contain '" + col + "'", columns.contains(col));
        }
    }

    private void assertSingleRow(Map<String, Object> result, String queryId, String column, long expected) {
        assertNotNull(queryId + ": response should contain 'rows'", result.get("rows"));
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertEquals(queryId + ": expected 1 row", 1, rows.size());
        int idx = columns.indexOf(column);
        assertTrue(queryId + ": column '" + column + "' not found", idx >= 0);
        long actual = ((Number) rows.get(0).get(idx)).longValue();
        assertEquals(queryId + ": " + column, expected, actual);
    }

    private static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ── Index mapping (Req 9.1, 9.2, 9.3) ────────────────────────────────────

    private static final String INDEX_MAPPING = "{\n"
        + "  \"settings\": {\n"
        + "    \"index.number_of_shards\": 1,\n"
        + "    \"index.number_of_replicas\": 0\n"
        + "  },\n"
        + "  \"mappings\": {\n"
        + "    \"properties\": {\n"
        + "      \"status\": {\"type\": \"keyword\"},\n"
        + "      \"category\": {\"type\": \"keyword\"},\n"
        + "      \"region\": {\"type\": \"keyword\"},\n"
        + "      \"amount\": {\"type\": \"long\"},\n"
        + "      \"quantity\": {\"type\": \"integer\"},\n"
        + "      \"price\": {\"type\": \"double\"}\n"
        + "    }\n"
        + "  }\n"
        + "}";
}
