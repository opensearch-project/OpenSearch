/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

public class WorkloadManagementRestIT extends OpenSearchRestTestCase {

    public void testCreate() throws Exception {
        Response response = performOperation("PUT", "_wlm/query_group", getCreateJson("analytics", "enforced", 0.4, 0.2));
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        performOperation("DELETE", "_wlm/query_group/analytics", null);
    }

    public void testMultipleCreate() throws Exception {
        Response response = performOperation("PUT", "_wlm/query_group", getCreateJson("analytics2", "enforced", 0.4, 0.2));
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Response response2 = performOperation("PUT", "_wlm/query_group", getCreateJson("users", "soft", 0.2, 0.1));
        assertEquals(response2.getStatusLine().getStatusCode(), 200);

        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/query_group", getCreateJson("users2", "soft", 0.41, 0.71))
        );
        performOperation("DELETE", "_wlm/query_group/analytics2", null);
        performOperation("DELETE", "_wlm/query_group/users", null);
    }

    public void testGet() throws Exception {
        Response response = performOperation("PUT", "_wlm/query_group", getCreateJson("analytics3", "enforced", 0.4, 0.2));
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Response response2 = performOperation("GET", "_wlm/query_group/analytics3", null);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics3\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"enforced\""));
        assertTrue(responseBody2.contains("\"cpu\":0.4"));
        assertTrue(responseBody2.contains("\"memory\":0.2"));

        assertThrows(ResponseException.class, () -> performOperation("GET", "_wlm/query_group/analytics97", null));
        performOperation("DELETE", "_wlm/query_group/analytics3", null);
    }

    public void testDelete() throws Exception {
        Response response = performOperation("PUT", "_wlm/query_group", getCreateJson("analytics4", "enforced", 0.4, 0.2));
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Response response2 = performOperation("DELETE", "_wlm/query_group/analytics4", null);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        assertTrue(EntityUtils.toString(response2.getEntity()).contains("\"acknowledged\":true"));

        assertThrows(ResponseException.class, () -> performOperation("DELETE", "_wlm/query_group/analytics99", null));
    }

    public void testUpdate() throws Exception {
        Response response = performOperation("PUT", "_wlm/query_group", getCreateJson("analytics5", "enforced", 0.4, 0.2));
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Response response2 = performOperation("PUT", "_wlm/query_group/analytics5", getUpdateJson("monitor", 0.41, 0.21));
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics5\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"monitor\""));
        assertTrue(responseBody2.contains("\"cpu\":0.41"));
        assertTrue(responseBody2.contains("\"memory\":0.21"));

        String json = "{\n"
            + "    \"resource_limits\": {\n"
            + "        \"cpu\" : 1.1,\n"
            + "        \"memory\" : -0.1\n"
            + "    }\n"
            + "}'";
        assertThrows(ResponseException.class, () -> performOperation("PUT", "_wlm/query_group/analytics5", json));
        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/query_group/analytics98", getUpdateJson("monitor", 0.43, 0.23))
        );
        performOperation("DELETE", "_wlm/query_group/analytics5", null);
    }

    public void testCRUD() throws Exception {
        Response response = performOperation("PUT", "_wlm/query_group", getCreateJson("analytics6", "enforced", 0.4, 0.2));
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Response response1 = performOperation("PUT", "_wlm/query_group/analytics6", getUpdateJson("monitor", 0.41, 0.21));
        assertEquals(response1.getStatusLine().getStatusCode(), 200);

        Response response2 = performOperation("GET", "_wlm/query_group/analytics6", null);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics6\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"monitor\""));
        assertTrue(responseBody2.contains("\"cpu\":0.41"));
        assertTrue(responseBody2.contains("\"memory\":0.21"));

        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/query_group", getCreateJson("users3", "monitor", 0.6, 0.8))
        );

        Response response4 = performOperation("PUT", "_wlm/query_group", getCreateJson("users3", "monitor", 0.59, 0.79));
        assertEquals(response4.getStatusLine().getStatusCode(), 200);

        Response response5 = performOperation("DELETE", "_wlm/query_group/analytics6", null);
        assertEquals(response5.getStatusLine().getStatusCode(), 200);
        String responseBody5 = EntityUtils.toString(response5.getEntity());
        assertTrue(responseBody5.contains("\"acknowledged\":true"));

        Response response6 = performOperation("GET", "_wlm/query_group", null);
        assertEquals(response6.getStatusLine().getStatusCode(), 200);
        String responseBody6 = EntityUtils.toString(response6.getEntity());
        assertTrue(responseBody6.contains("\"query_groups\""));
        assertTrue(responseBody6.contains("\"users3\""));
        assertFalse(responseBody6.contains("\"analytics6\""));
        performOperation("DELETE", "_wlm/query_group/users3", null);
    }

    static String getCreateJson(String name, String resiliencyMode, double cpu, double memory) {
        return "{\n"
            + "    \"name\": \""
            + name
            + "\",\n"
            + "    \"resiliency_mode\": \""
            + resiliencyMode
            + "\",\n"
            + "    \"resource_limits\": {\n"
            + "        \"cpu\" : "
            + cpu
            + ",\n"
            + "        \"memory\" : "
            + memory
            + "\n"
            + "    }\n"
            + "}";
    }

    static String getUpdateJson(String resiliencyMode, double cpu, double memory) {
        return "{\n"
            + "    \"resiliency_mode\": \""
            + resiliencyMode
            + "\",\n"
            + "    \"resource_limits\": {\n"
            + "        \"cpu\" : "
            + cpu
            + ",\n"
            + "        \"memory\" : "
            + memory
            + "\n"
            + "    }\n"
            + "}";
    }

    Response performOperation(String method, String uriPath, String json) throws IOException {
        Request request = new Request(method, uriPath);
        if (json != null) {
            request.setJsonEntity(json);
        }
        return client().performRequest(request);
    }
}
