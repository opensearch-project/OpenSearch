/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for the normal Lucene doc-values to DataFusion query path.
 *
 * <p>The main fixture is a standard two-shard Lucene index. Its mapping also contains an
 * unsupported integer field, so successful long/keyword/date queries
 * verify that planning narrows the Arrow source instead of requiring every scan column. Three
 * forced flushes exercise multiple Lucene segments, and one document omits {@code metric} to
 * exercise null propagation and {@code COUNT(field)} semantics.
 */
public class LuceneArrowSourceIT extends AnalyticsRestTestCase {

    private static final String INDEX = "lucene_arrow_source_e2e";
    private static final String FALLBACK_INDEX = "lucene_arrow_source_fallback";
    private static final String MULTI_VALUE_INDEX = "lucene_arrow_source_multi_value";

    private static final List<Doc> DOCS = List.of(
        new Doc(1L, 10L, "alpha", "2024-01-01T00:00:00Z", 101),
        new Doc(2L, 20L, "beta", "2024-01-02T00:00:00Z", 102),
        new Doc(3L, null, "alpha", "2024-01-03T00:00:00Z", 103),
        new Doc(4L, 40L, "beta", "2024-01-04T00:00:00Z", 104),
        new Doc(5L, 50L, "gamma", "2024-01-05T00:00:00Z", 105),
        new Doc(6L, 60L, "alpha", "2024-01-06T00:00:00Z", 106)
    );

    private static boolean dataProvisioned;

    @Override
    protected void onBeforeQuery() throws IOException {
        synchronized (LuceneArrowSourceIT.class) {
            if (dataProvisioned) {
                return;
            }
            createLucenePrimaryIndex();
            ingestLuceneSegments();
            createFallbackIndex();
            createMultiValueIndex();
            dataProvisioned = true;
        }
    }

    public void testFilteredAggregateAcrossShardsAndSegments() throws Exception {
        String ppl = "source="
            + INDEX
            + " | where id >= 3 | stats sum(metric) as total, count(metric) as non_null, count() as rows";
        Map<String, Object> explain = executeExplain(ppl);
        assertStageChoseBackend(explain, "SHARD_FRAGMENT", "lucene");
        assertStageChoseBackend(explain, "COORDINATOR_REDUCE", "datafusion");

        Map<String, Object> response = executePpl(ppl);
        assertEquals(150L, numberCell(response, 0, "total").longValue());
        assertEquals(3L, numberCell(response, 0, "non_null").longValue());
        assertEquals(4L, numberCell(response, 0, "rows").longValue());
    }

    public void testAverageMinAndMaxAggregates() throws Exception {
        String ppl = "source=" + INDEX + " | stats avg(metric) as average, min(metric) as minimum, max(metric) as maximum";
        Map<String, Object> response = executePpl(ppl);

        assertEquals(36.0d, numberCell(response, 0, "average").doubleValue(), 0.0d);
        assertEquals(10L, numberCell(response, 0, "minimum").longValue());
        assertEquals(60L, numberCell(response, 0, "maximum").longValue());
        Map<String, Object> explain = executeExplain(ppl);
        assertStageChoseBackend(explain, "SHARD_FRAGMENT", "lucene");
        assertStageChoseBackend(explain, "COORDINATOR_REDUCE", "datafusion");
    }

    public void testGroupedKeywordAggregate() throws Exception {
        String ppl = "source=" + INDEX + " | stats sum(metric) as total, count(metric) as non_null by category | sort category";
        Map<String, Object> response = executePpl(ppl);
        Map<String, long[]> actual = new HashMap<>();
        List<String> columns = extractColumnNames(response);
        for (List<Object> row : dataRows(response)) {
            String category = row.get(columns.indexOf("category")).toString();
            long total = ((Number) row.get(columns.indexOf("total"))).longValue();
            long nonNull = ((Number) row.get(columns.indexOf("non_null"))).longValue();
            actual.put(category, new long[] { total, nonNull });
        }

        assertEquals(3, actual.size());
        assertArrayEquals(new long[] { 70L, 2L }, actual.get("alpha"));
        assertArrayEquals(new long[] { 60L, 2L }, actual.get("beta"));
        assertArrayEquals(new long[] { 50L, 1L }, actual.get("gamma"));
        assertStageChoseBackend(executeExplain(ppl), "SHARD_FRAGMENT", "lucene");
    }

    public void testProjectionFilterSortAndTimestamp() throws Exception {
        String ppl = "source=" + INDEX + " | where id >= 4 | fields id, category, event_time | sort id";
        Map<String, Object> response = executePpl(ppl);
        List<List<Object>> rows = dataRows(response);
        List<String> columns = extractColumnNames(response);

        assertEquals(3, rows.size());
        assertProjectedRow(rows.get(0), columns, 4L, "beta", "2024-01-04");
        assertProjectedRow(rows.get(1), columns, 5L, "gamma", "2024-01-05");
        assertProjectedRow(rows.get(2), columns, 6L, "alpha", "2024-01-06");
        assertStageChoseBackend(executeExplain(ppl), "SHARD_FRAGMENT", "lucene");
    }

    public void testCountFastPathAndNumericNullFilter() throws Exception {
        String countAll = "source=" + INDEX + " | stats count() as rows";
        String countNull = "source=" + INDEX + " | where isnull(metric) | stats count() as rows";

        assertEquals(6L, numberCell(executePpl(countAll), 0, "rows").longValue());
        assertEquals(1L, numberCell(executePpl(countNull), 0, "rows").longValue());
        assertStageChoseBackend(executeExplain(countAll), "SHARD_FRAGMENT", "lucene");
        assertStageChoseBackend(executeExplain(countNull), "SHARD_FRAGMENT", "lucene");
    }

    public void testMultiValuedScalarFieldFailsFast() {
        String ppl = "source=" + MULTI_VALUE_INDEX + " | stats sum(metric) as total";

        ResponseException failure = expectThrows(ResponseException.class, () -> executePpl(ppl));
        assertEquals(500, failure.getResponse().getStatusLine().getStatusCode());
    }

    public void testUnsupportedIntegerFallsBackToDataFusion() throws Exception {
        String ppl = "source=" + FALLBACK_INDEX + " | stats sum(value) as total";

        assertEquals(10L, numberCell(executePpl(ppl), 0, "total").longValue());
        assertStageChoseBackend(executeExplain(ppl), "SHARD_FRAGMENT", "datafusion");
    }

    private void createLucenePrimaryIndex() throws IOException {
        deleteIfExists(INDEX);
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.composite.primary_data_format\": \"lucene\""
            + "},"
            + "\"mappings\": {\"properties\": {"
            + "  \"id\": {\"type\": \"long\"},"
            + "  \"metric\": {\"type\": \"long\"},"
            + "  \"category\": {\"type\": \"keyword\"},"
            + "  \"event_time\": {\"type\": \"date\"},"
            + "  \"unsupported\": {\"type\": \"integer\"}"
            + "}}}";
        createIndex(INDEX, body);
    }

    private void ingestLuceneSegments() throws IOException {
        for (int from = 0; from < DOCS.size(); from += 2) {
            StringBuilder bulk = new StringBuilder();
            for (int i = from; i < Math.min(from + 2, DOCS.size()); i++) {
                bulk.append("{\"index\": {}}\n");
                bulk.append(DOCS.get(i).toJson()).append('\n');
            }
            bulkIndex(INDEX, bulk.toString());
            client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
        }
    }

    private void createMultiValueIndex() throws IOException {
        deleteIfExists(MULTI_VALUE_INDEX);
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.composite.primary_data_format\": \"lucene\""
            + "},"
            + "\"mappings\": {\"properties\": {\"metric\": {\"type\": \"long\"}}}"
            + "}";
        createIndex(MULTI_VALUE_INDEX, body);
        bulkIndex(MULTI_VALUE_INDEX, "{\"index\": {}}\n{\"metric\": [10, 20]}\n");
    }

    private void createFallbackIndex() throws IOException {
        deleteIfExists(FALLBACK_INDEX);
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {\"properties\": {\"value\": {\"type\": \"integer\"}}}"
            + "}";
        createIndex(FALLBACK_INDEX, body);
        bulkIndex(
            FALLBACK_INDEX,
            "{\"index\": {}}\n{\"value\": 2}\n"
                + "{\"index\": {}}\n{\"value\": 3}\n"
                + "{\"index\": {}}\n{\"value\": 5}\n"
        );
    }

    private void createIndex(String index, String body) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create " + index);
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void bulkIndex(String index, String ndjson) throws IOException {
        Request bulk = new Request("POST", "/" + index + "/_bulk");
        bulk.setJsonEntity(ndjson);
        bulk.addParameter("refresh", "true");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(bulk), "Bulk index " + index);
        assertEquals("Bulk indexing should have no errors: " + response, false, response.get("errors"));
    }

    private void deleteIfExists(String index) throws IOException {
        Request delete = new Request("DELETE", "/" + index);
        delete.addParameter("ignore_unavailable", "true");
        client().performRequest(delete);
    }

    private Map<String, Object> executeExplain(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "EXPLAIN: " + ppl);
    }

    @SuppressWarnings("unchecked")
    private static void assertStageChoseBackend(Map<String, Object> explain, String executionType, String expectedBackend) {
        Map<String, Object> profile = (Map<String, Object>) explain.get("profile");
        assertNotNull("profile present", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present", stages);
        for (Map<String, Object> stage : stages) {
            if (executionType.equals(stage.get("execution_type"))) {
                assertEquals(executionType + " stage: " + stage, expectedBackend, stage.get("chosen_backend"));
                return;
            }
        }
        fail("No " + executionType + " stage in profile: " + stages);
    }

    private static Number numberCell(Map<String, Object> response, int rowIndex, String column) {
        List<String> columns = extractColumnNames(response);
        int columnIndex = columns.indexOf(column);
        assertTrue("Missing column " + column + " in " + columns, columnIndex >= 0);
        return (Number) dataRows(response).get(rowIndex).get(columnIndex);
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> dataRows(Map<String, Object> response) {
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing datarows: " + response, rows);
        return rows;
    }

    private static void assertProjectedRow(
        List<Object> row,
        List<String> columns,
        long expectedId,
        String expectedCategory,
        String expectedDate
    ) {
        assertEquals(expectedId, ((Number) row.get(columns.indexOf("id"))).longValue());
        assertEquals(expectedCategory, row.get(columns.indexOf("category")));
        Object timestamp = row.get(columns.indexOf("event_time"));
        assertNotNull(timestamp);
        assertTrue("Unexpected timestamp: " + timestamp, timestamp.toString().contains(expectedDate));
    }

    private record Doc(long id, Long metric, String category, String eventTime, int unsupported) {
        String toJson() {
            String metricJson = metric == null ? "" : ", \"metric\": " + metric;
            return "{\"id\": "
                + id
                + metricJson
                + ", \"category\": \""
                + category
                + "\", \"event_time\": \""
                + eventTime
                + "\", \"unsupported\": "
                + unsupported
                + "}";
        }
    }
}
