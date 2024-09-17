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

public class WorkloadManagementRestIT extends OpenSearchRestTestCase {

    public void testCreate() throws Exception {
        Request request = new Request("PUT", "_wlm/query_group");
        request.setJsonEntity(getCreateJson("analytics", "enforced", 0.4, 0.2));
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        client().performRequest(new Request("DELETE", "_wlm/query_group/analytics"));
    }

    public void testMultipleCreate() throws Exception {
        Request request = new Request("PUT", "_wlm/query_group");
        request.setJsonEntity(getCreateJson("analytics2", "enforced", 0.4, 0.2));
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Request request2 = new Request("PUT", "_wlm/query_group");
        request2.setJsonEntity(getCreateJson("users", "soft", 0.2, 0.1));
        Response response2 = client().performRequest(request2);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);

        Request request3 = new Request("PUT", "_wlm/query_group");
        request3.setJsonEntity(getCreateJson("users2", "soft", 0.41, 0.71));
        assertThrows(ResponseException.class, () -> client().performRequest(request3));
        client().performRequest(new Request("DELETE", "_wlm/query_group/analytics2"));
        client().performRequest(new Request("DELETE", "_wlm/query_group/users"));
    }

    public void testGet() throws Exception {
        Request request = new Request("PUT", "_wlm/query_group");
        request.setJsonEntity(getCreateJson("analytics3", "enforced", 0.4, 0.2));
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Request request2 = new Request("GET", "_wlm/query_group/analytics3");
        Response response2 = client().performRequest(request2);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics3\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"enforced\""));
        assertTrue(responseBody2.contains("\"cpu\":0.4"));
        assertTrue(responseBody2.contains("\"memory\":0.2"));

        Request request3 = new Request("GET", "_wlm/query_group/analytics97");
        assertThrows(ResponseException.class, () -> client().performRequest(request3));
        client().performRequest(new Request("DELETE", "_wlm/query_group/analytics3"));
    }

    public void testDelete() throws Exception {
        Request request = new Request("PUT", "_wlm/query_group");
        request.setJsonEntity(getCreateJson("analytics4", "enforced", 0.4, 0.2));
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Request request2 = new Request("DELETE", "_wlm/query_group/analytics4");
        Response response2 = client().performRequest(request2);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"acknowledged\":true"));

        Request request3 = new Request("DELETE", "_wlm/query_group/analytics99");
        assertThrows(ResponseException.class, () -> client().performRequest(request3));
    }

    public void testUpdate() throws Exception {
        Request request = new Request("PUT", "_wlm/query_group");
        request.setJsonEntity(getCreateJson("analytics5", "enforced", 0.4, 0.2));
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Request request2 = new Request("PUT", "_wlm/query_group/analytics5");
        request2.setJsonEntity(getUpdateJson("monitor", 0.41, 0.21));
        Response response2 = client().performRequest(request2);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics5\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"monitor\""));
        assertTrue(responseBody2.contains("\"cpu\":0.41"));
        assertTrue(responseBody2.contains("\"memory\":0.21"));

        Request request3 = new Request("PUT", "_wlm/query_group/analytics5");
        request3.setJsonEntity(
            "{\n" + "    \"resource_limits\": {\n" + "        \"cpu\" : 1.1,\n" + "        \"memory\" : -0.1\n" + "    }\n" + "}'"
        );
        assertThrows(ResponseException.class, () -> client().performRequest(request3));

        Request request4 = new Request("PUT", "_wlm/query_group/analytics98");
        request4.setJsonEntity(getUpdateJson("monitor", 0.43, 0.23));
        assertThrows(ResponseException.class, () -> client().performRequest(request4));
        client().performRequest(new Request("DELETE", "_wlm/query_group/analytics5"));
    }

    public void testCRUD() throws Exception {
        Request request = new Request("PUT", "_wlm/query_group");
        request.setJsonEntity(getCreateJson("analytics6", "enforced", 0.4, 0.2));
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        Request request1 = new Request("PUT", "_wlm/query_group/analytics6");
        request1.setJsonEntity(getUpdateJson("monitor", 0.41, 0.21));
        Response response1 = client().performRequest(request1);
        assertEquals(response1.getStatusLine().getStatusCode(), 200);

        Request request2 = new Request("GET", "_wlm/query_group/analytics6");
        Response response2 = client().performRequest(request2);
        assertEquals(response2.getStatusLine().getStatusCode(), 200);
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics6\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"monitor\""));
        assertTrue(responseBody2.contains("\"cpu\":0.41"));
        assertTrue(responseBody2.contains("\"memory\":0.21"));

        Request request3 = new Request("PUT", "_wlm/query_group");
        request3.setJsonEntity(getCreateJson("users3", "monitor", 0.6, 0.8));
        assertThrows(ResponseException.class, () -> client().performRequest(request3));

        Request request4 = new Request("PUT", "_wlm/query_group");
        request4.setJsonEntity(getCreateJson("users3", "monitor", 0.59, 0.79));
        Response response4 = client().performRequest(request4);
        assertEquals(response4.getStatusLine().getStatusCode(), 200);

        Request request5 = new Request("DELETE", "_wlm/query_group/analytics6");
        Response response5 = client().performRequest(request5);
        assertEquals(response5.getStatusLine().getStatusCode(), 200);
        String responseBody5 = EntityUtils.toString(response5.getEntity());
        assertTrue(responseBody5.contains("\"acknowledged\":true"));

        Request request6 = new Request("GET", "_wlm/query_group/");
        Response response6 = client().performRequest(request6);
        assertEquals(response6.getStatusLine().getStatusCode(), 200);
        String responseBody6 = EntityUtils.toString(response6.getEntity());
        assertTrue(responseBody6.contains("\"query_groups\""));
        assertTrue(responseBody6.contains("\"users3\""));
        assertFalse(responseBody6.contains("\"analytics6\""));
        client().performRequest(new Request("DELETE", "_wlm/query_group/users3"));
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
}
