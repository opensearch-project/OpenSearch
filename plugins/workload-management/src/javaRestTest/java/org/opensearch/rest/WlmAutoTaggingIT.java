/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

public class WlmAutoTaggingIT extends OpenSearchRestTestCase {

    public void testCreateAutoTaggingRule() throws Exception {
        String json = "{\n"
            + "  \"id\": \"wlm_test_rule\",\n"
            + "  \"description\": \"auto tagging test\",\n"
            + "  \"attributes\": {\n"
            + "    \"index_pattern\": [\"logs-*\"]\n"
            + "  },\n"
            + "  \"feature_type\": \"workload_group\",\n"
            + "  \"tag\": \"group_A\"\n"
            + "}";

        Response response = performOperation("PUT", "/_rules/workload_group", json);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    // public void testCreateAutoTaggingRule() throws Exception {
    // String ruleId = "wlm_autotag_test_rule";
    //
    // // Create rule
    // String json = "{\n" +
    // " \"feature_type\": \"workload_group\",\n" +
    // " \"id\": \"" + ruleId + "\",\n" +
    // " \"description\": \"Integration test rule\",\n" +
    // " \"attributes\": {\n" +
    // " \"index_pattern\": [\"logs-*\"]\n" +
    // " },\n" +
    // " \"value\": \"group_A\"\n" +
    // "}";
    //
    // Response response = performOperation("PUT", "/_rules/workload_group", json);
    // assertEquals(200, response.getStatusLine().getStatusCode());
    //
    // String responseBody = EntityUtils.toString(response.getEntity());
    // assertTrue(responseBody.contains("\"id\":\"" + ruleId + "\""));
    //
    // // Get rule
    // Response getResponse = performOperation("GET", "/_rules/workload_group/" + ruleId, null);
    // assertEquals(200, getResponse.getStatusLine().getStatusCode());
    //
    // String getResponseBody = EntityUtils.toString(getResponse.getEntity());
    // assertTrue(getResponseBody.contains(ruleId));
    // assertTrue(getResponseBody.contains("logs-*"));
    //
    // // Clean up
    // Response deleteResponse = performOperation("DELETE", "/_rules/workload_group/" + ruleId, null);
    // assertEquals(200, deleteResponse.getStatusLine().getStatusCode());
    // }

    public void testCreateInvalidAutoTaggingRule() throws Exception {
        String invalidJson = "{\n"
            + "  \"feature_type\": \"workload_group\",\n"
            + "  \"id\": \"bad_rule\",\n"
            + "  \"description\": \"missing attributes\",\n"
            + "  \"value\": \"group_B\"\n"
            + "}";

        assertThrows(ResponseException.class, () -> performOperation("PUT", "/_rules/workload_group", invalidJson));
    }

    Response performOperation(String method, String uriPath, String json) throws IOException {
        Request request = new Request(method, uriPath);
        if (json != null) {
            request.setJsonEntity(json);
        }
        return client().performRequest(request);
    }
}
