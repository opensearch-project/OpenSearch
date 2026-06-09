/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;

/**
 * E2E integration test for bare comparison predicates delegated to Lucene on a composite
 * parquet+lucene index. Keyword fields use lexicographic ordering.
 */
public class ComparisonDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "comparison_delegation_e2e";

    public void testComparisonOperatorsOnKeywordFields() throws Exception {
        createIndex();
        indexDocs();

        // All comparisons are lexicographic on keyword fields.
        // price values: "10","20","30","40","50","60","70","80","90","100"
        // Lexicographic order: "10" < "100" < "20" < "30" < "40" < "50" < "60" < "70" < "80" < "90"

        // price > '50' → "60","70","80","90" (4 docs — "100" < "50" lexicographically)
        String gtPpl = "source = " + INDEX_NAME + " | where price > '50' | stats count() as c";
        Map<String, Object> gtResult = executePplViaShim(gtPpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> gtRows = (List<List<Object>>) gtResult.get("rows");
        assertNotNull("rows must not be null", gtRows);
        assertEquals("scalar agg must return exactly 1 row", 1, gtRows.size());
        assertEquals("price > '50' should match 4 docs (lexicographic)", 4L, ((Number) gtRows.get(0).get(0)).longValue());

        // price >= '50' → "50","60","70","80","90" (5 docs)
        String gtePpl = "source = " + INDEX_NAME + " | where price >= '50' | stats count() as c";
        Map<String, Object> gteResult = executePplViaShim(gtePpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> gteRows = (List<List<Object>>) gteResult.get("rows");
        assertNotNull("rows must not be null", gteRows);
        assertEquals("scalar agg must return exactly 1 row", 1, gteRows.size());
        assertEquals("price >= '50' should match 5 docs (lexicographic)", 5L, ((Number) gteRows.get(0).get(0)).longValue());

        // price < '30' → "10","100","20" (3 docs — lexicographic)
        String ltPpl = "source = " + INDEX_NAME + " | where price < '30' | stats count() as c";
        Map<String, Object> ltResult = executePplViaShim(ltPpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> ltRows = (List<List<Object>>) ltResult.get("rows");
        assertNotNull("rows must not be null", ltRows);
        assertEquals("scalar agg must return exactly 1 row", 1, ltRows.size());
        assertEquals("price < '30' should match 3 docs (lexicographic)", 3L, ((Number) ltRows.get(0).get(0)).longValue());

        // price <= '30' → "10","100","20","30" (4 docs — lexicographic)
        String ltePpl = "source = " + INDEX_NAME + " | where price <= '30' | stats count() as c";
        Map<String, Object> lteResult = executePplViaShim(ltePpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> lteRows = (List<List<Object>>) lteResult.get("rows");
        assertNotNull("rows must not be null", lteRows);
        assertEquals("scalar agg must return exactly 1 row", 1, lteRows.size());
        assertEquals("price <= '30' should match 4 docs (lexicographic)", 4L, ((Number) lteRows.get(0).get(0)).longValue());
    }

    private void createIndex() throws Exception {
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
            + "    \"price\": { \"type\": \"keyword\" },"
            + "    \"qty\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexDocs() throws Exception {
        String[] prices = {"10", "20", "30", "40", "50", "60", "70", "80", "90", "100"};
        String[] qtys = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"price\": \"").append(prices[i]).append("\", \"qty\": \"").append(qtys[i]).append("\"}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Flush to ensure parquet files are written
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }
}
