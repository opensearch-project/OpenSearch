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

/**
 * Integration test verifying that deeply nested PPL expressions return a 400 error
 * with a meaningful message instead of a 500 internal server error.
 *
 * <p>Reproduces: https://github.com/opensearch-project/OpenSearch/issues/XXXXX
 * Root cause: prost protobuf decoder recursion limit (default 100) is exceeded when
 * Substrait plan has 31+ nested ScalarFunction calls (each adds ~3 protobuf nesting levels).
 */
public class NestedQueryRecursionLimitIT extends AnalyticsRestTestCase {

    private static final String INDEX = "nested_recursion_test";

    public void testDeeplyNestedQueryReturns400() throws Exception {
        createIndex();
        indexData();

        // 50 nested calls should fail with 400 (not 500)
        String deep = buildNestedExpr(50);
        Request badRequest = new Request("POST", "/_analytics/ppl");
        badRequest.setJsonEntity("{\"query\": \"source=" + INDEX + " | eval result = " + deep + "\"}");
        try {
            client().performRequest(badRequest);
            fail("Expected 400 error for deeply nested query");
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            assertTrue(
                "Expected 400 but got " + status + ": " + e.getMessage(),
                status == 400
            );
            String body = new String(e.getResponse().getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(
                "Error should mention nesting depth, got: " + body,
                body.contains("too deeply nested") || body.contains("nesting depth")
            );
        }
    }

    private String buildNestedExpr(int depth) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append(i % 2 == 0 ? "ceil(" : "floor(");
        }
        sb.append("value");
        for (int i = 0; i < depth; i++) {
            sb.append(")");
        }
        return sb.toString();
    }

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) throw e;
        } catch (Exception ignored) {}

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{"
            + "\"settings\": {"
            + "  \"index.number_of_shards\": 1,"
            + "  \"index.number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"value\": {\"type\": \"double\"}"
            + "  }"
            + "}"
            + "}");
        client().performRequest(create);
    }

    private void indexData() throws Exception {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"value\":3.7}\n"
                + "{\"index\":{}}\n{\"value\":8.2}\n"
                + "{\"index\":{}}\n{\"value\":1.5}\n"
        );
        client().performRequest(bulk);

        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }
}
