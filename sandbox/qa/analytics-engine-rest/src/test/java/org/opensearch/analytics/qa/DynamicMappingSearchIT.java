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

    /**
     * Cross-shard schema drift: a sparse dynamically-mapped field that exists in only one doc
     * (hence in only one shard's parquet segments) must not break a filtered query that
     * materializes rows. The coordinator's substrait base_schema names the field (it's in the
     * merged index mapping), so the indexed-executor scan path must widen each shard's schema to
     * base_schema and null-fill the absent column — otherwise shards lacking it fail with
     * "No field named ...". Regression test for the indexed-path widening fix.
     */
    public void testCrossShardDriftFilteredQuery() throws Exception {
        String index = "cross_shard_drift_e2e";
        createTwoShardIndex(index, "{\"proto\": { \"type\": \"keyword\" }, \"common\": { \"type\": \"integer\" }}");

        // 10 docs across 2 shards (natural _id hashing); exactly one carries the sparse field, so
        // it lands in only one shard's parquet. The dynamic field enters the merged index mapping.
        StringBuilder bulk = new StringBuilder();
        for (int i = 1; i <= 10; i++) {
            bulk.append("{\"index\": {}}\n");
            if (i == 5) {
                bulk.append("{\"proto\": \"TCP\", \"common\": ").append(i).append(", \"sparse_field\": 999}\n");
            } else {
                bulk.append("{\"proto\": \"TCP\", \"common\": ").append(i).append("}\n");
            }
        }
        bulkIndexInto(index, bulk.toString());

        // Sanity: the sparse field is dynamically mapped and present on exactly one doc.
        assertCountIn(index, "where isnotnull(sparse_field) | stats count() as cnt", 1);

        // The drift trigger: a filtered query that materializes rows (indexed-executor path).
        // Before the fix this failed on the shard lacking sparse_field with "No field named".
        Map<String, Object> result = executePpl("source = " + index + " | where proto = 'TCP' | fields common | sort common");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("drift query returned no datarows", rows);
        assertEquals("filtered query over a drifted multi-shard index should return all matching rows", 10, rows.size());

        // The drifted column reads back as its real value where present, NULL where absent.
        assertValueIn(index, "where common = 5 | fields sparse_field", "sparse_field", 999.0);
        assertCountIn(index, "where isnull(sparse_field) | stats count() as cnt", 9);
    }

    /**
     * Filtered query against an index whose shards are all empty (zero docs / zero parquet files).
     * The empty-shard guard derives the scan schema from the plan rather than from segments, so a
     * row-materializing filtered query must return zero rows cleanly rather than erroring.
     */
    public void testFilteredQueryOnEmptyShards() throws Exception {
        String index = "empty_shard_filter_e2e";
        createTwoShardIndex(index, "{\"proto\": { \"type\": \"keyword\" }, \"common\": { \"type\": \"integer\" }}");

        Map<String, Object> result = executePpl("source = " + index + " | where proto = 'TCP' | fields common | head 2");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("empty-shard query returned no datarows", rows);
        assertEquals("filtered query on an empty index should return zero rows", 0, rows.size());

        assertCountIn(index, "stats count() as cnt", 0);
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

    /** Creates a 2-shard composite/parquet index (lucene secondary) with the given mapping properties. */
    private void createTwoShardIndex(String index, String propertiesJson) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": { \"properties\": " + propertiesJson + " }"
            + "}";
        Request req = new Request("PUT", "/" + index);
        req.setJsonEntity(body);
        assertEquals(true, assertOkAndParse(client().performRequest(req), "Create index " + index).get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /** Bulk-index NDJSON into the named index with refresh. */
    private void bulkIndexInto(String index, String ndjson) throws Exception {
        Request req = new Request("POST", "/" + index + "/_bulk");
        req.setJsonEntity(ndjson);
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk index " + index);
        assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
    }

    /** count-query assertion against an arbitrary index. */
    private void assertCountIn(String index, String pplSuffix, int expected) throws IOException {
        String ppl = "source = " + index + " | " + pplSuffix;
        Map<String, Object> result = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("Response missing 'datarows' for: " + ppl, rows);
        assertEquals("Expected 1 row for count query: " + ppl, 1, rows.size());
        assertEquals("Count mismatch for: " + ppl, expected, ((Number) rows.get(0).get(0)).longValue());
    }

    /** single-value assertion against an arbitrary index. */
    private void assertValueIn(String index, String pplSuffix, String column, double expected) throws IOException {
        String ppl = "source = " + index + " | " + pplSuffix;
        Map<String, Object> result = executePpl(ppl);
        List<String> columns = extractColumnNames(result);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("Response missing 'datarows' for: " + ppl, rows);
        assertEquals(1, rows.size());
        int idx = columns.indexOf(column);
        assertTrue("Column '" + column + "' not found in: " + columns, idx >= 0);
        assertEquals("Value mismatch for: " + ppl, expected, ((Number) rows.get(0).get(idx)).doubleValue(), 0.01);
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


    private void assertCount(String pplSuffix, int expected) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        Map<String, Object> result = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("Expected 1 row for count query: " + ppl, 1, rows.size());
        long actual = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("Count mismatch for: " + ppl, expected, actual);
    }

    private void assertValue(String pplSuffix, String column, double expected) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        Map<String, Object> result = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(result);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
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
