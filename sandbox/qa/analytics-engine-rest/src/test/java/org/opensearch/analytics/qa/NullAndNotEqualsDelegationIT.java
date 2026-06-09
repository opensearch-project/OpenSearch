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
 * Integration test for IS_NULL, IS_NOT_NULL, and NOT_EQUALS predicate delegation to Lucene
 * on a composite parquet+lucene index.
 *
 * <p>Exercises the full path: PPL → planner → delegation → data node dispatch → results.
 * The dataset has a nullable keyword column ({@code str1}) that is present on some docs and
 * missing on others, plus a non-nullable keyword column ({@code str0}) used for NOT_EQUALS.
 */
public class NullAndNotEqualsDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "null_and_not_equals_delegation_e2e";

    public void testNullAndNotEqualsDelegation() throws Exception {
        createIndex();
        indexDocs();

        // NOT_EQUALS: str0 != 'apple' → 7 docs (banana×3 + cherry×2 + date×2)
        String notEqualsPpl = "source = " + INDEX_NAME + " | where str0 != 'apple' | stats count() as c";
        Map<String, Object> notEqualsResult = executePplViaShim(notEqualsPpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> notEqualsRows = (List<List<Object>>) notEqualsResult.get("rows");
        assertNotNull("rows must not be null", notEqualsRows);
        assertEquals("scalar agg must return exactly 1 row", 1, notEqualsRows.size());
        assertEquals("str0 != 'apple' should return 7", 7L, ((Number) notEqualsRows.get(0).get(0)).longValue());

        // IS_NOT_NULL: str1 IS NOT NULL → 6 docs
        String isNotNullPpl = "source = " + INDEX_NAME + " | where isnotnull(str1) | stats count() as c";
        Map<String, Object> isNotNullResult = executePplViaShim(isNotNullPpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> isNotNullRows = (List<List<Object>>) isNotNullResult.get("rows");
        assertNotNull("rows must not be null", isNotNullRows);
        assertEquals("scalar agg must return exactly 1 row", 1, isNotNullRows.size());
        assertEquals("str1 IS NOT NULL should return 6", 6L, ((Number) isNotNullRows.get(0).get(0)).longValue());

        // IS_NULL: str1 IS NULL → 4 docs
        String isNullPpl = "source = " + INDEX_NAME + " | where isnull(str1) | stats count() as c";
        Map<String, Object> isNullResult = executePplViaShim(isNullPpl);
        @SuppressWarnings("unchecked")
        List<List<Object>> isNullRows = (List<List<Object>>) isNullResult.get("rows");
        assertNotNull("rows must not be null", isNullRows);
        assertEquals("scalar agg must return exactly 1 row", 1, isNullRows.size());
        assertEquals("str1 IS NULL should return 4", 4L, ((Number) isNullRows.get(0).get(0)).longValue());
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
            + "    \"str0\": { \"type\": \"keyword\" },"
            + "    \"str1\": { \"type\": \"keyword\" }"
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
        // 10 docs total:
        //   str0: "apple"×3, "banana"×3, "cherry"×2, "date"×2
        //   str1: present ("x") for 6 docs, missing/null for 4 docs
        StringBuilder bulk = new StringBuilder();

        // apple×3: 2 with str1, 1 without
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"apple\", \"str1\": \"x\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"apple\", \"str1\": \"x\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"apple\"}\n");

        // banana×3: 2 with str1, 1 without
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"banana\", \"str1\": \"x\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"banana\", \"str1\": \"x\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"banana\"}\n");

        // cherry×2: 1 with str1, 1 without
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"cherry\", \"str1\": \"x\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"cherry\"}\n");

        // date×2: 1 with str1, 1 without
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"date\", \"str1\": \"x\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"str0\": \"date\"}\n");

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Flush to ensure parquet files are written
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

}
