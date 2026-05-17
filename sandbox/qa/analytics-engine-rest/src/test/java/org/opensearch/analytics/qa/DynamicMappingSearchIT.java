/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end integration test for dynamic mapping with search verification.
 * <p>
 * Validates that documents with dynamically added fields (not in the original mapping)
 * are correctly indexed into a composite parquet index AND are searchable via PPL
 * through both the vanilla scan path and the indexed executor (filter delegation) path.
 * <p>
 * Run with:
 * ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.DynamicMappingSearchIT" -Dsandbox.enabled=true
 */
public class DynamicMappingSearchIT extends AnalyticsRestTestCase {

    private static final String INDEX = "dynamic_mapping_search_e2e";

    // ── Field name constants ────────────────────────────────────────────────
    private static final String FIELD_NAME = "name";
    private static final String FIELD_AGE = "age";
    private static final String FIELD_CITY = "city";
    private static final String FIELD_POINTS = "points";
    private static final String FIELD_PRIORITY = "priority";

    /**
     * Full end-to-end test: 3-phase ingestion with progressive schema evolution,
     * verifying search works correctly at each stage via both vanilla and indexed paths.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21701 — performance "
        + "filter delegation needs handling for dynamically added fields. Lucene's per-segment "
        + "FieldInfos differ across segments, and a query against a field absent from older "
        + "segments returns empty bitsets that get incorrectly AND'd into DataFusion's candidates. "
        + "Either propagate per-segment field-presence as a 'skip' signal or disable perf "
        + "delegation for dynamically-mapped fields.")
    public void testSearchOnDynamicallyAddedFields() throws Exception {
        createIndex();

        // ── Phase 1: Initial schema (name, age) ─────────────────────────────
        bulkIndex(docs(
            doc("alice", 30),
            doc("bob", 25),
            doc("carol", 35),
            doc("dave", 28),
            doc("eve", 32)
        ));
        flush();

        // Dynamic fields should NOT be in mapping yet
        assertMappingContains(FIELD_NAME, FIELD_AGE);
        assertMappingNotContains(FIELD_CITY, FIELD_POINTS, FIELD_PRIORITY);

        assertCount("stats count() as cnt", 5);

        // ── Phase 2: Dynamic fields (city, points) ──────────────────────────
        bulkIndex(docs(
            doc("frank", 40, "seattle", 95),
            doc("grace", 22, "portland", 88),
            doc("hank", 45, "seattle", 72),
            doc("iris", 29, "portland", 91),
            doc("jack", 33, "seattle", 85)
        ));
        flush();

        // city and points now in mapping; priority still absent
        assertMappingContains(FIELD_CITY, FIELD_POINTS);
        assertMappingNotContains(FIELD_PRIORITY);

        assertCount("stats count() as cnt", 10);
        assertCount("where " + FIELD_CITY + " = 'seattle' | stats count() as cnt", 3);
        assertCount("where " + FIELD_POINTS + " >= 90 | stats count() as cnt", 2);
        assertValue("stats sum(" + FIELD_POINTS + ") as total", "total", 431.0);
        assertCount("where isnull(" + FIELD_CITY + ") | stats count() as cnt", 5);
        assertCount("where " + FIELD_AGE + " > 30 | stats count() as cnt", 5);

        // ── Phase 3: Another new field (priority) ───────────────────────────
        bulkIndex(docs(
            doc("kate", 27, "seattle", 90, 1),
            doc("leo", 38, "portland", 78, 2),
            doc("mia", 31, "seattle", 94, 1)
        ));
        flush();

        // All fields now present in mapping
        assertMappingContains(FIELD_NAME, FIELD_AGE, FIELD_CITY, FIELD_POINTS, FIELD_PRIORITY);

        assertCount("stats count() as cnt", 13);
        assertCount("where isnotnull(" + FIELD_PRIORITY + ") | stats count() as cnt", 3);
        assertCount("where " + FIELD_PRIORITY + " = 1 | stats count() as cnt", 2);
        assertCount("where " + FIELD_CITY + " = 'seattle' | stats count() as cnt", 5);
        assertValue("stats sum(" + FIELD_POINTS + ") as total", "total", 693.0);

        // ── Indexed executor path (filter delegation via match()) ────────────
        assertCount("where match(" + FIELD_NAME + ", 'kate') | stats count() as cnt", 1);
        assertCount("where match(" + FIELD_NAME + ", 'alice') | stats count() as cnt", 1);
        assertValue("where match(" + FIELD_NAME + ", 'hank') | stats sum(" + FIELD_POINTS + ") as total", "total", 72.0);
        assertCount("where match(" + FIELD_CITY + ", 'seattle') | stats count() as cnt", 5);
        assertValue("where match(" + FIELD_NAME + ", 'frank') | stats sum(" + FIELD_POINTS + ") as total", "total", 95.0);
        assertValue("where match(" + FIELD_NAME + ", 'mia') | stats sum(" + FIELD_PRIORITY + ") as total", "total", 1.0);
    }

    // ── Document builders ───────────────────────────────────────────────────

    /** Phase 1 doc: name + age only */
    private static String doc(String name, int age) {
        return "{\"" + FIELD_NAME + "\": \"" + name + "\", \"" + FIELD_AGE + "\": " + age + "}";
    }

    /** Phase 2 doc: name + age + city + points */
    private static String doc(String name, int age, String city, int points) {
        return "{\"" + FIELD_NAME + "\": \"" + name + "\", \"" + FIELD_AGE + "\": " + age
            + ", \"" + FIELD_CITY + "\": \"" + city + "\", \"" + FIELD_POINTS + "\": " + points + "}";
    }

    /** Phase 3 doc: name + age + city + points + priority */
    private static String doc(String name, int age, String city, int points, int priority) {
        return "{\"" + FIELD_NAME + "\": \"" + name + "\", \"" + FIELD_AGE + "\": " + age
            + ", \"" + FIELD_CITY + "\": \"" + city + "\", \"" + FIELD_POINTS + "\": " + points
            + ", \"" + FIELD_PRIORITY + "\": " + priority + "}";
    }

    /** Combine docs into bulk NDJSON */
    private static String docs(String... documents) {
        StringBuilder sb = new StringBuilder();
        for (String doc : documents) {
            sb.append("{\"index\": {}}\n").append(doc).append("\n");
        }
        return sb.toString();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
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
            + "    \"" + FIELD_NAME + "\": { \"type\": \"keyword\" },"
            + "    \"" + FIELD_AGE + "\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void bulkIndex(String ndjson) throws Exception {
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.setJsonEntity(ndjson);
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
    }

    private void flush() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    private Map<String, Object> executePPL(String ppl) throws IOException {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(req);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    private void assertCount(String pplSuffix, int expected) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        Map<String, Object> result = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("Expected 1 row for count query: " + ppl, 1, rows.size());
        long actual = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("Count mismatch for: " + ppl, expected, actual);
    }

    private void assertValue(String pplSuffix, String column, double expected) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        Map<String, Object> result = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals(1, rows.size());
        int idx = columns.indexOf(column);
        assertTrue("Column '" + column + "' not found in: " + columns, idx >= 0);
        double actual = ((Number) rows.get(0).get(idx)).doubleValue();
        assertEquals("Value mismatch for: " + ppl, expected, actual, 0.01);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMappingProperties() throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_mapping"));
        Map<String, Object> map = assertOkAndParse(response, "Get mapping");
        Map<String, Object> indexMap = (Map<String, Object>) map.get(INDEX);
        Map<String, Object> mappings = (Map<String, Object>) indexMap.get("mappings");
        return (Map<String, Object>) mappings.get("properties");
    }

    private void assertMappingContains(String... fields) throws IOException {
        Map<String, Object> properties = getMappingProperties();
        for (String field : fields) {
            assertTrue("Mapping should contain field '" + field + "'", properties.containsKey(field));
        }
    }

    private void assertMappingNotContains(String... fields) throws IOException {
        Map<String, Object> properties = getMappingProperties();
        for (String field : fields) {
            assertFalse("Mapping should NOT contain field '" + field + "' yet", properties.containsKey(field));
        }
    }
}
