/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.apis;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Package-private utility class providing REST wrappers and response navigation
 * helpers for composite engine API integration tests.
 *
 * @opensearch.experimental
 */
public class DataFormatApiTestUtils {

    private DataFormatApiTestUtils() {}

    // --- REST wrappers ---

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getDataFormatStats(RestClient client, String index, Map<String, String> params) throws IOException {
        String path = index != null ? "/_plugins/dataformat_stats/" + index : "/_plugins/dataformat_stats";
        String query = buildQuery(params);
        if (!query.isEmpty()) {
            path += "?" + query;
        }
        Response response = client.performRequest(new Request("GET", path));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return parseJson(response);
    }

    public static Map<String, Object> getDataFormatStatsExpectError(RestClient client, String path, int expectedStatus) throws IOException {
        try {
            client.performRequest(new Request("GET", path));
            throw new AssertionError("Expected ResponseException with status " + expectedStatus);
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(expectedStatus));
            return parseJson(e.getResponse());
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getCatalogSnapshot(RestClient client, String index, Map<String, String> params) throws IOException {
        String path = "/_plugins/composite/" + index + "/_catalog_snapshot";
        String query = buildQuery(params);
        if (!query.isEmpty()) {
            path += "?" + query;
        }
        Response response = client.performRequest(new Request("GET", path));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return parseJson(response);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getParquetAnalyze(RestClient client, String index, Map<String, String> params) throws IOException {
        String path = "/_plugins/parquet/" + index + "/_analyze";
        String query = buildQuery(params);
        if (!query.isEmpty()) {
            path += "?" + query;
        }
        Response response = client.performRequest(new Request("GET", path));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return parseJson(response);
    }

    // --- Path navigation helpers ---

    @SuppressWarnings("unchecked")
    public static Map<String, Object> mapAt(Map<String, Object> root, String... path) {
        Map<String, Object> current = root;
        for (String key : path) {
            if (current == null) return null;
            Object val = current.get(key);
            if (val == null) return null;
            current = (Map<String, Object>) val;
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> listAt(Map<String, Object> root, String... path) {
        if (path.length == 0) return null;
        Map<String, Object> parent = path.length == 1 ? root : mapAt(root, java.util.Arrays.copyOf(path, path.length - 1));
        if (parent == null) return null;
        Object val = parent.get(path[path.length - 1]);
        return val instanceof List ? (List<Object>) val : null;
    }

    public static long longAt(Map<String, Object> root, String... path) {
        if (path.length == 0) return 0L;
        Map<String, Object> parent = path.length == 1 ? root : mapAt(root, java.util.Arrays.copyOf(path, path.length - 1));
        assertNotNull("Parent map is null for path", parent);
        Object val = parent.get(path[path.length - 1]);
        assertNotNull("Value is null at terminal key: " + path[path.length - 1], val);
        return ((Number) val).longValue();
    }

    public static String stringAt(Map<String, Object> root, String... path) {
        if (path.length == 0) return null;
        Map<String, Object> parent = path.length == 1 ? root : mapAt(root, java.util.Arrays.copyOf(path, path.length - 1));
        if (parent == null) return null;
        Object val = parent.get(path[path.length - 1]);
        return val != null ? val.toString() : null;
    }

    // --- Stats-specific extractors ---

    public static long statsDocsIndexed(Map<String, Object> stats, String index) {
        return longAt(stats, "indices", index, "composite", "indexing", "docs_indexed_total");
    }

    public static long statsFlushTotal(Map<String, Object> stats, String index) {
        return longAt(stats, "indices", index, "composite", "flush", "flush_total");
    }

    public static long statsRefreshTotal(Map<String, Object> stats, String index) {
        return longAt(stats, "indices", index, "composite", "refresh", "refresh_total");
    }

    public static long statsMergeTotal(Map<String, Object> stats, String index) {
        return longAt(stats, "indices", index, "composite", "merge", "merges_total");
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> statsPerFormat(Map<String, Object> stats, String index, String format) {
        return mapAt(stats, "indices", index, "composite", "per_format", format);
    }

    // --- Cross-API consistency assertion ---

    @SuppressWarnings("unchecked")
    public static void assertCrossApiConsistency(
        Map<String, Object> stats,
        Map<String, Object> catalog,
        Map<String, Object> analyze,
        String index,
        long expectedDocs
    ) {
        long docsIndexed = statsDocsIndexed(stats, index);
        assertThat("Stats docs_indexed_total should match expected", docsIndexed, equalTo(expectedDocs));

        Map<String, Object> summary = mapAt(catalog, "summary", "by_format", "parquet");
        assertThat("Catalog should have parquet summary", summary, notNullValue());
        long catalogRows = ((Number) summary.get("total_rows")).longValue();
        assertThat("Catalog total_rows should match expected", catalogRows, equalTo(expectedDocs));

        long analyzeRows = ((Number) analyze.get("total_rows")).longValue();
        assertThat("Analyze total_rows should match expected", analyzeRows, equalTo(expectedDocs));
    }

    // --- Utility ---

    public static String buildQuery(Map<String, String> params) {
        if (params == null || params.isEmpty()) return "";
        return params.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));
    }

    public static Map<String, Object> parseJson(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
    }

    // --- Format-specific stats REST wrappers ---

    public static Map<String, Object> getParquetStats(RestClient client, String index, Map<String, String> params) throws IOException {
        String query = buildQuery(params);
        String path = "/_plugins/parquet/" + index + "/_stats" + (query.isEmpty() ? "" : "?" + query);
        return parseJson(client.performRequest(new Request("GET", path)));
    }

    public static Map<String, Object> getLuceneStats(RestClient client, String index, Map<String, String> params) throws IOException {
        String query = buildQuery(params);
        String path = "/_plugins/lucene/" + index + "/_stats" + (query.isEmpty() ? "" : "?" + query);
        return parseJson(client.performRequest(new Request("GET", path)));
    }

    public static Map<String, Object> getParquetNodeStats(RestClient client, String nodeId, Map<String, String> params) throws IOException {
        String query = buildQuery(params);
        String path = "/_plugins/parquet/_nodes" + (nodeId != null ? "/" + nodeId : "") + "/_stats" + (query.isEmpty() ? "" : "?" + query);
        return parseJson(client.performRequest(new Request("GET", path)));
    }

    public static Map<String, Object> getLuceneNodeStats(RestClient client, String nodeId, Map<String, String> params) throws IOException {
        String query = buildQuery(params);
        String path = "/_plugins/lucene/_nodes" + (nodeId != null ? "/" + nodeId : "") + "/_stats" + (query.isEmpty() ? "" : "?" + query);
        return parseJson(client.performRequest(new Request("GET", path)));
    }
}
