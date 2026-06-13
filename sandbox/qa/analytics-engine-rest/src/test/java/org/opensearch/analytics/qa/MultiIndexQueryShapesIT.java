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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end integration tests exercising multi-index queries (aliases, wildcard patterns)
 * across different query shapes: plain scan, filter, aggregation, and filter delegation.
 *
 * <p>Tests run against parquet-only indices AND parquet+lucene indices (for filter delegation).
 * Each test verifies correct fan-out, null-fill for differing field sets, and result correctness.
 */
public class MultiIndexQueryShapesIT extends AnalyticsRestTestCase {

    // ── Parquet-only indices (non-indexed path) ──
    private static final String PQONLY_A = "multi_pq_a";
    private static final String PQONLY_B = "multi_pq_b";
    private static final String PQONLY_ALIAS = "multi_pq_alias";

    // ── Parquet+Lucene indices (indexed/delegation path) ──
    private static final String PQLUC_A = "multi_pqluc_a";
    private static final String PQLUC_B = "multi_pqluc_b";
    private static final String PQLUC_ALIAS = "multi_pqluc_alias";

    private static boolean provisioned = false;

    private void ensureProvisioned() throws IOException {
        if (provisioned) return;

        // Parquet-only: index A has {status, message}, index B has {status, message, source}
        createParquetIndex(PQONLY_A, "{\"properties\":{\"status\":{\"type\":\"integer\"},\"message\":{\"type\":\"keyword\"}}}");
        createParquetIndex(PQONLY_B, "{\"properties\":{\"status\":{\"type\":\"integer\"},\"message\":{\"type\":\"keyword\"},\"source\":{\"type\":\"keyword\"}}}");
        bulk(PQONLY_A, "{\"status\":200,\"message\":\"ok\"}\n{\"status\":500,\"message\":\"error\"}\n{\"status\":200,\"message\":\"hello\"}\n");
        bulk(PQONLY_B, "{\"status\":200,\"message\":\"world\",\"source\":\"app1\"}\n{\"status\":404,\"message\":\"not found\",\"source\":\"app2\"}\n");
        putAlias(PQONLY_ALIAS, List.of(PQONLY_A, PQONLY_B));

        // Parquet+Lucene: same field layout, enables filter delegation
        createCompositeIndex(PQLUC_A, "{\"properties\":{\"status\":{\"type\":\"integer\"},\"message\":{\"type\":\"keyword\"}}}");
        createCompositeIndex(PQLUC_B, "{\"properties\":{\"status\":{\"type\":\"integer\"},\"message\":{\"type\":\"keyword\"},\"source\":{\"type\":\"keyword\"}}}");
        bulk(PQLUC_A, "{\"status\":200,\"message\":\"ok\"}\n{\"status\":500,\"message\":\"error\"}\n{\"status\":200,\"message\":\"hello\"}\n");
        bulk(PQLUC_B, "{\"status\":200,\"message\":\"world\",\"source\":\"app1\"}\n{\"status\":404,\"message\":\"not found\",\"source\":\"app2\"}\n");
        putAlias(PQLUC_ALIAS, List.of(PQLUC_A, PQLUC_B));

        provisioned = true;
    }

    // ── Parquet-only: alias scan ──

    public void testAliasScanFanOut() throws IOException {
        ensureProvisioned();
        long count = singleCount("source=" + PQONLY_ALIAS + " | stats count() as c");
        assertEquals("alias fan-out: 3 + 2 = 5", 5L, count);
    }

    public void testAliasFilter() throws IOException {
        ensureProvisioned();
        long count = singleCount("source=" + PQONLY_ALIAS + " | where status = 200 | stats count() as c");
        assertEquals("status=200: 2 from A + 1 from B = 3", 3L, count);
    }

    public void testAliasAggregation() throws IOException {
        ensureProvisioned();
        Map<String, Object> body = executePpl("source=" + PQONLY_ALIAS + " | stats sum(status) as total");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals(1, rows.size());
        long total = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("sum(200+500+200+200+404)", 1504L, total);
    }

    public void testAliasNullFillForDifferingFields() throws IOException {
        ensureProvisioned();
        Map<String, Object> body = executePpl("source=" + PQONLY_ALIAS + " | fields message, source");
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(body);
        assertTrue("must have source column", columns.contains("source"));
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("total rows: 3 + 2 = 5", 5, rows.size());
        int sourceCol = columns.indexOf("source");
        int nullCount = 0;
        for (List<Object> row : rows) {
            if (row.get(sourceCol) == null) nullCount++;
        }
        assertEquals("index A rows have null source", 3, nullCount);
    }

    // ── Parquet-only: wildcard scan ──

    public void testWildcardScanFanOut() throws IOException {
        ensureProvisioned();
        long count = singleCount("source=multi_pq_* | stats count() as c");
        assertEquals("wildcard fan-out: 3 + 2 = 5", 5L, count);
    }

    public void testWildcardFilterAggregation() throws IOException {
        ensureProvisioned();
        long count = singleCount("source=multi_pq_* | where status > 200 | stats count() as c");
        assertEquals("status > 200: 500 from A + 404 from B = 2", 2L, count);
    }

    // ── Parquet+Lucene: wildcard ──

    public void testDelegationWildcardFanOut() throws IOException {
        ensureProvisioned();
        long count = singleCount("source=multi_pqluc_* | stats count() as c");
        assertEquals("delegation wildcard fan-out: 3 + 2 = 5", 5L, count);
    }

    // ── Parquet+Lucene (delegation): alias scan ──

    public void testDelegationAliasScanFanOut() throws IOException {
        ensureProvisioned();
        long count = singleCount("source=" + PQLUC_ALIAS + " | stats count() as c");
        assertEquals("delegation alias fan-out: 3 + 2 = 5", 5L, count);
    }

    public void testDelegationAliasAggregation() throws IOException {
        ensureProvisioned();
        Map<String, Object> body = executePpl("source=" + PQLUC_ALIAS + " | stats sum(status) as total");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals(1, rows.size());
        long total = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("sum(200+500+200+200+404)", 1504L, total);
    }

    public void testDelegationAliasNullFill() throws IOException {
        ensureProvisioned();
        Map<String, Object> body = executePpl("source=" + PQLUC_ALIAS + " | fields message, source");
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(body);
        assertTrue("must have source column", columns.contains("source"));
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals(5, rows.size());
        int sourceCol = columns.indexOf("source");
        int nullCount = 0;
        for (List<Object> row : rows) {
            if (row.get(sourceCol) == null) nullCount++;
        }
        assertEquals("index A rows have null source", 3, nullCount);
    }


    // ── Multi-shard: verifies reduce handles null-filled batches from different shards ──

    private static final String MSHARD_A = "multi_mshard_a";
    private static final String MSHARD_B = "multi_mshard_b";
    private static final String MSHARD_ALIAS = "multi_mshard_alias";
    private static boolean mshardProvisioned = false;

    private void ensureMshardProvisioned() throws IOException {
        if (mshardProvisioned) return;
        createIndexWithSettings(MSHARD_A, "{\"properties\":{\"val\":{\"type\":\"long\"},\"tag\":{\"type\":\"keyword\"}}}", false, 2);
        createIndexWithSettings(MSHARD_B, "{\"properties\":{\"val\":{\"type\":\"long\"},\"extra\":{\"type\":\"double\"}}}", false, 2);
        // 10 docs per index spread across 2 shards each = 4 shards total feeding the reduce
        StringBuilder docsA = new StringBuilder();
        StringBuilder docsB = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            docsA.append("{\"val\":").append(i).append(",\"tag\":\"t").append(i).append("\"}\n");
            docsB.append("{\"val\":").append(i + 10).append(",\"extra\":").append(i * 1.5).append("}\n");
        }
        bulk(MSHARD_A, docsA.toString());
        bulk(MSHARD_B, docsB.toString());
        putAlias(MSHARD_ALIAS, List.of(MSHARD_A, MSHARD_B));
        mshardProvisioned = true;
    }

    public void testMultiShardReduceWithNullFill() throws IOException {
        ensureMshardProvisioned();
        long count = singleCount("source=" + MSHARD_ALIAS + " | stats count() as c");
        assertEquals("multi-shard: 10 + 10 = 20", 20L, count);
    }

    public void testMultiShardAggregationAcrossUnion() throws IOException {
        ensureMshardProvisioned();
        Map<String, Object> body = executePpl("source=" + MSHARD_ALIAS + " | stats sum(val) as total");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals(1, rows.size());
        long total = ((Number) rows.get(0).get(0)).longValue();
        // sum(0..9) + sum(10..19) = 45 + 145 = 190
        assertEquals("sum across multi-shard union", 190L, total);
    }

    public void testMultiShardNullFillFieldsPresent() throws IOException {
        ensureMshardProvisioned();
        Map<String, Object> body = executePpl("source=" + MSHARD_ALIAS + " | fields val, tag, extra");
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(body);
        assertTrue("must have tag", columns.contains("tag"));
        assertTrue("must have extra", columns.contains("extra"));
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("20 total rows", 20, rows.size());
        int tagCol = columns.indexOf("tag");
        int extraCol = columns.indexOf("extra");
        int nullTags = 0, nullExtras = 0;
        for (List<Object> row : rows) {
            if (row.get(tagCol) == null) nullTags++;
            if (row.get(extraCol) == null) nullExtras++;
        }
        assertEquals("index B rows have null tag", 10, nullTags);
        assertEquals("index A rows have null extra", 10, nullExtras);
    }

    // ── Type coverage: verifies the Substrait→Arrow round-trip for various field types ──

    private static final String TYPES_A = "multi_types_a";
    private static final String TYPES_B = "multi_types_b";
    private static final String TYPES_ALIAS = "multi_types_alias";
    private static boolean typesProvisioned = false;

    private void ensureTypesProvisioned() throws IOException {
        if (typesProvisioned) return;
        // Index A: integer, keyword, boolean
        // Index B: integer, double, long (keyword/boolean absent → null-filled)
        createParquetIndex(TYPES_A,
            "{\"properties\":{\"id\":{\"type\":\"integer\"},\"label\":{\"type\":\"keyword\"},\"active\":{\"type\":\"boolean\"}}}");
        createParquetIndex(TYPES_B,
            "{\"properties\":{\"id\":{\"type\":\"integer\"},\"score\":{\"type\":\"double\"},\"count\":{\"type\":\"long\"}}}");
        bulk(TYPES_A, "{\"id\":1,\"label\":\"foo\",\"active\":true}\n{\"id\":2,\"label\":\"bar\",\"active\":false}\n");
        bulk(TYPES_B, "{\"id\":3,\"score\":9.5,\"count\":100}\n{\"id\":4,\"score\":3.2,\"count\":200}\n");
        putAlias(TYPES_ALIAS, List.of(TYPES_A, TYPES_B));
        typesProvisioned = true;
    }

    public void testTypeCoverageUnionFanOut() throws IOException {
        ensureTypesProvisioned();
        long count = singleCount("source=" + TYPES_ALIAS + " | stats count() as c");
        assertEquals("type coverage: 2 + 2 = 4", 4L, count);
    }

    public void testTypeCoverageNullFillAcrossTypes() throws IOException {
        ensureTypesProvisioned();
        Map<String, Object> body = executePpl("source=" + TYPES_ALIAS + " | fields id, label, active, score, count");
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(body);
        assertTrue("must have all union columns", columns.containsAll(List.of("id", "label", "active", "score", "count")));
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("4 total rows", 4, rows.size());
        int labelCol = columns.indexOf("label");
        int scoreCol = columns.indexOf("score");
        int nullLabels = 0, nullScores = 0;
        for (List<Object> row : rows) {
            if (row.get(labelCol) == null) nullLabels++;
            if (row.get(scoreCol) == null) nullScores++;
        }
        assertEquals("index B rows have null label", 2, nullLabels);
        assertEquals("index A rows have null score", 2, nullScores);
    }

    public void testTypeCoverageAggregateOnSharedField() throws IOException {
        ensureTypesProvisioned();
        Map<String, Object> body = executePpl("source=" + TYPES_ALIAS + " | stats sum(id) as total");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals(1, rows.size());
        long total = ((Number) rows.get(0).get(0)).longValue();
        assertEquals("sum(1+2+3+4)", 10L, total);
    }

    // ── Dynamic mapping: fields added by indexing without explicit mapping ──

    public void testDynamicMappingUnionAcrossIndices() throws IOException {
        String dynA = "multi_dyn_a";
        String dynB = "multi_dyn_b";
        String dynAlias = "multi_dyn_alias";
        // Create indices with minimal explicit mapping; let dynamic mapping add fields from docs
        createParquetIndex(dynA, "{\"properties\":{\"id\":{\"type\":\"integer\"}}}");
        createParquetIndex(dynB, "{\"properties\":{\"id\":{\"type\":\"integer\"}}}");
        // Index docs with different dynamic fields — dynA gets "city", dynB gets "country"
        bulk(dynA, "{\"id\":1,\"city\":\"Seattle\"}\n{\"id\":2,\"city\":\"Portland\"}\n");
        bulk(dynB, "{\"id\":3,\"country\":\"US\"}\n{\"id\":4,\"country\":\"CA\"}\n");
        putAlias(dynAlias, List.of(dynA, dynB));

        long count = singleCount("source=" + dynAlias + " | stats count() as c");
        assertEquals("dynamic mapping union: 2 + 2 = 4", 4L, count);

        Map<String, Object> body = executePpl("source=" + dynAlias + " | fields id, city, country");
        @SuppressWarnings("unchecked")
        List<String> columns = extractColumnNames(body);
        assertTrue("must have city", columns.contains("city"));
        assertTrue("must have country", columns.contains("country"));
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals(4, rows.size());
        int cityCol = columns.indexOf("city");
        int countryCol = columns.indexOf("country");
        int nullCities = 0, nullCountries = 0;
        for (List<Object> row : rows) {
            if (row.get(cityCol) == null) nullCities++;
            if (row.get(countryCol) == null) nullCountries++;
        }
        assertEquals("dynB rows have null city", 2, nullCities);
        assertEquals("dynA rows have null country", 2, nullCountries);
    }

    // ── Failure tests: type mismatch rejection ──

    public void testWildcardTypeMismatchIsRejected() throws IOException {
        String mismatchA = "multi_mismatch_a";
        String mismatchB = "multi_mismatch_b";
        createParquetIndex(mismatchA, "{\"properties\":{\"val\":{\"type\":\"long\"}}}");
        createParquetIndex(mismatchB, "{\"properties\":{\"val\":{\"type\":\"keyword\"}}}");
        bulk(mismatchA, "{\"val\":1}\n");
        bulk(mismatchB, "{\"val\":\"hello\"}\n");

        String error = executePplExpectingFailure("source=multi_mismatch_* | stats count() as c");
        assertContains(error, "incompatible field types");
        assertContains(error, "val");
    }

    public void testAliasTypeMismatchIsRejected() throws IOException {
        String mismatchA = "multi_aliasmm_a";
        String mismatchB = "multi_aliasmm_b";
        String mismatchAlias = "multi_aliasmm";
        createParquetIndex(mismatchA, "{\"properties\":{\"score\":{\"type\":\"double\"}}}");
        createParquetIndex(mismatchB, "{\"properties\":{\"score\":{\"type\":\"keyword\"}}}");
        bulk(mismatchA, "{\"score\":1.5}\n");
        bulk(mismatchB, "{\"score\":\"high\"}\n");
        putAlias(mismatchAlias, List.of(mismatchA, mismatchB));

        String error = executePplExpectingFailure("source=" + mismatchAlias + " | stats count() as c");
        assertContains(error, "incompatible field types");
        assertContains(error, "score");
    }

    private String executePplExpectingFailure(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            fail("Expected failure but got: " + assertOkAndParse(response, ppl));
            return "";
        } catch (ResponseException re) {
            return entityAsString(re.getResponse());
        }
    }

    private static void assertContains(String haystack, String needle) {
        assertTrue("expected to contain [" + needle + "] but was: " + haystack, haystack.contains(needle));
    }

    // ── Helpers ──

    private long singleCount(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertNotNull("missing 'rows' for: " + ppl, rows);
        assertEquals("single count row expected: " + ppl, 1, rows.size());
        Object cell = rows.get(0).get(0);
        assertTrue("expected numeric count: " + cell, cell instanceof Number);
        return ((Number) cell).longValue();
    }


    private void createParquetIndex(String name, String mappingJson) throws IOException {
        createIndexWithSettings(name, mappingJson, false, 1);
    }

    private void createCompositeIndex(String name, String mappingJson) throws IOException {
        createIndexWithSettings(name, mappingJson, true, 1);
    }

    private void createIndexWithSettings(String name, String mappingJson, boolean withLuceneSecondary) throws IOException {
        createIndexWithSettings(name, mappingJson, withLuceneSecondary, 1);
    }

    private void createIndexWithSettings(String name, String mappingJson, boolean withLuceneSecondary, int shards) throws IOException {
        String secondaryFormats = ",\"index.composite.secondary_data_formats\":\"lucene\"";
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.number_of_shards\":" + shards + ",\"index.number_of_replicas\":0"
                + secondaryFormats
                + "},\"mappings\":" + mappingJson + "}"
        );
        try {
            client().performRequest(create);
        } catch (ResponseException re) {
            String body = entityAsString(re.getResponse());
            if (!body.contains("resource_already_exists_exception")) {
                throw re;
            }
        }
    }

    private void bulk(String index, String ndjsonDocs) throws IOException {
        StringBuilder bulkBody = new StringBuilder();
        for (String doc : ndjsonDocs.split("\n")) {
            if (doc.isBlank()) continue;
            bulkBody.append("{\"index\": {}}\n").append(doc).append("\n");
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(bulkBody.toString());
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "bulk " + index);
        assertEquals("bulk into " + index + " had errors", false, response.get("errors"));
    }

    private void putAlias(String alias, List<String> indices) throws IOException {
        StringBuilder actions = new StringBuilder("{\"actions\":[");
        for (int i = 0; i < indices.size(); i++) {
            if (i > 0) actions.append(",");
            actions.append("{\"add\":{\"index\":\"").append(indices.get(i)).append("\",\"alias\":\"").append(alias).append("\"}}");
        }
        actions.append("]}");
        Request put = new Request("POST", "/_aliases");
        put.setJsonEntity(actions.toString());
        client().performRequest(put);
    }

    private static String entityAsString(Response response) throws IOException {
        try (var is = response.getEntity().getContent()) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
