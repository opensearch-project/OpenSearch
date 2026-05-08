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

import java.util.List;
import java.util.Map;

/**
 * E2E integration test for filter delegation: a MATCH predicate is delegated to Lucene
 * while DataFusion drives the scan + aggregation.
 *
 * <p>Exercises the full path: PPL → planner → ShardScanWithDelegationInstructionNode →
 * data node dispatch → Lucene FilterDelegationHandle → Rust indexed executor → results.
 */
public class FilterDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "filter_delegation_e2e";

    public void testMatchFilterDelegationWithAggregate() throws Exception {
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') | stats sum(value) as total";
        Map<String, Object> result = executePPL(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        // 10 docs with "hello world" and value=5 → total = 50
        Number total = (Number) rows.get(0).get(0);
        assertEquals("SUM(value) for MATCH(message, 'hello') docs", 50L, total.longValue());
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
            + "    \"message\": { \"type\": \"text\" },"
            + "    \"value\": { \"type\": \"integer\" }"
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
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"hello world\", \"value\": 5}\n");
        }
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"goodbye world\", \"value\": 3}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Flush to ensure parquet files are written
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}
