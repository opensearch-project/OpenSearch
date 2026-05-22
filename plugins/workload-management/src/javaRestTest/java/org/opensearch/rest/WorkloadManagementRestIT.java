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
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;

public class WorkloadManagementRestIT extends OpenSearchRestTestCase {

    @Before
    public void enableWlmMode() throws Exception {
        setWlmMode("enabled");
    }

    public void testCreate() throws Exception {
        Response response = performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics", "enforced", 0.4, 0.2));
        assertEquals(200, response.getStatusLine().getStatusCode());
        performOperation("DELETE", "_wlm/workload_group/analytics", null);
    }

    public void testMultipleCreate() throws Exception {
        Response response = performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics2", "enforced", 0.4, 0.2));
        assertEquals(200, response.getStatusLine().getStatusCode());

        Response response2 = performOperation("PUT", "_wlm/workload_group", getCreateJson("users", "soft", 0.2, 0.1));
        assertEquals(200, response2.getStatusLine().getStatusCode());

        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", getCreateJson("users2", "soft", 0.41, 0.71))
        );
        performOperation("DELETE", "_wlm/workload_group/analytics2", null);
        performOperation("DELETE", "_wlm/workload_group/users", null);
    }

    public void testGet() throws Exception {
        Response response = performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics3", "enforced", 0.4, 0.2));
        assertEquals(200, response.getStatusLine().getStatusCode());

        Response response2 = performOperation("GET", "_wlm/workload_group/analytics3", null);
        assertEquals(200, response2.getStatusLine().getStatusCode());
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics3\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"enforced\""));
        assertTrue(responseBody2.contains("\"cpu\":0.4"));
        assertTrue(responseBody2.contains("\"memory\":0.2"));

        assertThrows(ResponseException.class, () -> performOperation("GET", "_wlm/workload_group/analytics97", null));
        performOperation("DELETE", "_wlm/workload_group/analytics3", null);
    }

    public void testDelete() throws Exception {
        Response response = performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics4", "enforced", 0.4, 0.2));
        assertEquals(200, response.getStatusLine().getStatusCode());

        Response response2 = performOperation("DELETE", "_wlm/workload_group/analytics4", null);
        assertEquals(200, response2.getStatusLine().getStatusCode());
        assertTrue(EntityUtils.toString(response2.getEntity()).contains("\"acknowledged\":true"));

        assertThrows(ResponseException.class, () -> performOperation("DELETE", "_wlm/workload_group/analytics99", null));
    }

    public void testUpdate() throws Exception {
        Response response = performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics5", "enforced", 0.4, 0.2));
        assertEquals(200, response.getStatusLine().getStatusCode());

        Response response2 = performOperation("PUT", "_wlm/workload_group/analytics5", getUpdateJson("monitor", 0.41, 0.21));
        assertEquals(200, response2.getStatusLine().getStatusCode());
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics5\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"monitor\""));
        assertTrue(responseBody2.contains("\"cpu\":0.41"));
        assertTrue(responseBody2.contains("\"memory\":0.21"));

        String json = """
            {
                "resource_limits": {
                    "cpu" : 1.1,
                    "memory" : -0.1
                }
            }""";
        assertThrows(ResponseException.class, () -> performOperation("PUT", "_wlm/workload_group/analytics5", json));
        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group/analytics98", getUpdateJson("monitor", 0.43, 0.23))
        );
        performOperation("DELETE", "_wlm/workload_group/analytics5", null);
    }

    public void testCRUD() throws Exception {
        Response response = performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics6", "enforced", 0.4, 0.2));
        assertEquals(200, response.getStatusLine().getStatusCode());

        Response response1 = performOperation("PUT", "_wlm/workload_group/analytics6", getUpdateJson("monitor", 0.41, 0.21));
        assertEquals(200, response1.getStatusLine().getStatusCode());

        Response response2 = performOperation("GET", "_wlm/workload_group/analytics6", null);
        assertEquals(200, response2.getStatusLine().getStatusCode());
        String responseBody2 = EntityUtils.toString(response2.getEntity());
        assertTrue(responseBody2.contains("\"name\":\"analytics6\""));
        assertTrue(responseBody2.contains("\"resiliency_mode\":\"monitor\""));
        assertTrue(responseBody2.contains("\"cpu\":0.41"));
        assertTrue(responseBody2.contains("\"memory\":0.21"));

        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", getCreateJson("users3", "monitor", 0.6, 0.8))
        );

        Response response4 = performOperation("PUT", "_wlm/workload_group", getCreateJson("users3", "monitor", 0.59, 0.79));
        assertEquals(200, response4.getStatusLine().getStatusCode());

        Response response5 = performOperation("DELETE", "_wlm/workload_group/analytics6", null);
        assertEquals(200, response5.getStatusLine().getStatusCode());
        String responseBody5 = EntityUtils.toString(response5.getEntity());
        assertTrue(responseBody5.contains("\"acknowledged\":true"));

        Response response6 = performOperation("GET", "_wlm/workload_group", null);
        assertEquals(200, response6.getStatusLine().getStatusCode());
        String responseBody6 = EntityUtils.toString(response6.getEntity());
        assertTrue(responseBody6.contains("\"workload_groups\""));
        assertTrue(responseBody6.contains("\"users3\""));
        assertFalse(responseBody6.contains("\"analytics6\""));
        performOperation("DELETE", "_wlm/workload_group/users3", null);
    }

    public void testOperationWhenWlmDisabled() throws Exception {
        setWlmMode("disabled");
        assertThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", getCreateJson("analytics", "enforced", 0.4, 0.2))
        );
        assertThrows(ResponseException.class, () -> performOperation("DELETE", "_wlm/workload_group/analytics4", null));
        assertOK(performOperation("GET", "_wlm/workload_group/", null));
    }

    public void testSearchSettings() throws Exception {
        // Create with all search settings
        String createJson = """
            {
                "name": "search_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "search.default_search_timeout": "30s",
                    "search.cancel_after_time_interval": "1m",
                    "search.max_concurrent_shard_requests": "5",
                    "search.batched_reduce_size": "512"
                }
            }""";
        Response response = performOperation("PUT", "_wlm/workload_group", createJson);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify settings in GET response
        Response getResponse = performOperation("GET", "_wlm/workload_group/search_test", null);
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertTrue(responseBody.contains("\"settings\""));
        assertFalse(responseBody.contains("\"override_request_values\""));
        assertTrue(responseBody.contains("\"search.default_search_timeout\":\"30s\""));
        assertTrue(responseBody.contains("\"search.cancel_after_time_interval\":\"1m\""));
        assertTrue(responseBody.contains("\"search.max_concurrent_shard_requests\":\"5\""));
        assertTrue(responseBody.contains("\"search.batched_reduce_size\":\"512\""));

        // Update search settings
        String updateJson = """
            {
                "settings": {
                    "search.default_search_timeout": "1m",
                    "search.cancel_after_time_interval": "5m",
                    "search.max_concurrent_shard_requests": "10",
                    "search.batched_reduce_size": "256"
                }
            }""";
        Response updateResponse = performOperation("PUT", "_wlm/workload_group/search_test", updateJson);
        assertEquals(200, updateResponse.getStatusLine().getStatusCode());

        // Verify updated settings
        Response getResponse2 = performOperation("GET", "_wlm/workload_group/search_test", null);
        String responseBody2 = EntityUtils.toString(getResponse2.getEntity());
        assertTrue(responseBody2.contains("\"search.default_search_timeout\":\"1m\""));
        assertTrue(responseBody2.contains("\"search.cancel_after_time_interval\":\"5m\""));
        assertTrue(responseBody2.contains("\"search.max_concurrent_shard_requests\":\"10\""));
        assertTrue(responseBody2.contains("\"search.batched_reduce_size\":\"256\""));

        performOperation("DELETE", "_wlm/workload_group/search_test", null);
    }

    public void testSearchSettingsOverrideRequestValues() throws Exception {
        // Create a WLM group with override_request_values=true so WLM settings win over request params
        String createJson = """
            {
                "name": "override_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "search.default_search_timeout": "30s",
                    "search.max_concurrent_shard_requests": "3",
                    "search.batched_reduce_size": "64",
                    "override_request_values": "true"
                }
            }""";
        Response response = performOperation("PUT", "_wlm/workload_group", createJson);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Verify override_request_values is "true" in GET response
        Response getResponse = performOperation("GET", "_wlm/workload_group/override_test", null);
        assertTrue(EntityUtils.toString(getResponse.getEntity()).contains("\"override_request_values\":\"true\""));

        // Toggle to false and verify
        String toggleJson = """
            {"settings": {"override_request_values": "false"}}""";
        Response toggleResponse = performOperation("PUT", "_wlm/workload_group/override_test", toggleJson);
        assertEquals(200, toggleResponse.getStatusLine().getStatusCode());
        Response getResponse2 = performOperation("GET", "_wlm/workload_group/override_test", null);
        assertTrue(EntityUtils.toString(getResponse2.getEntity()).contains("\"override_request_values\":\"false\""));

        // Exercise the full request path: create an index, run a search with the WLM header.
        // This confirms the listener is wired into the request flow without errors. Override
        // semantics themselves are verified in WorkloadGroupRequestOperationListenerTests.
        performOperation("PUT", "wlm-test-idx", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        performOperation("POST", "wlm-test-idx/_doc", "{\"msg\":\"hello\"}");
        performOperation("POST", "wlm-test-idx/_refresh", null);

        Request searchRequest = new Request("POST", "wlm-test-idx/_search");
        searchRequest.setJsonEntity("{\"query\":{\"match_all\":{}},\"timeout\":\"1m\"}");
        searchRequest.setOptions(searchRequest.getOptions().toBuilder().addHeader("X-opaque-id", "wlm=override_test"));
        Response searchResponse = client().performRequest(searchRequest);
        assertEquals(200, searchResponse.getStatusLine().getStatusCode());
        assertTrue(EntityUtils.toString(searchResponse.getEntity()).contains("\"hits\""));

        performOperation("DELETE", "wlm-test-idx", null);
        performOperation("DELETE", "_wlm/workload_group/override_test", null);
    }

    public void testSearchSettingsInvalidSettingsRejected() throws Exception {
        // Unknown setting key should be rejected
        String unknownKeyJson = """
            {
                "name": "invalid_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "unknown_setting": "value"
                }
            }""";
        ResponseException unknownKeyException = expectThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", unknownKeyJson)
        );
        assertTrue(EntityUtils.toString(unknownKeyException.getResponse().getEntity()).contains("Unknown WLM setting: unknown_setting"));

        // Invalid value for max_concurrent_shard_requests (must be >= 1)
        String invalidIntJson = """
            {
                "name": "invalid_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "search.max_concurrent_shard_requests": "0"
                }
            }""";
        ResponseException invalidIntException = expectThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", invalidIntJson)
        );
        String invalidIntBody = EntityUtils.toString(invalidIntException.getResponse().getEntity());
        assertTrue(invalidIntBody.contains("search.max_concurrent_shard_requests"));
        assertTrue(invalidIntBody.contains("must be >= 1"));

        // Invalid value for batched_reduce_size (must be >= 2)
        String invalidBatchJson = """
            {
                "name": "invalid_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "search.batched_reduce_size": "1"
                }
            }""";
        ResponseException invalidBatchException = expectThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", invalidBatchJson)
        );
        String invalidBatchBody = EntityUtils.toString(invalidBatchException.getResponse().getEntity());
        assertTrue(invalidBatchBody.contains("search.batched_reduce_size"));
        assertTrue(invalidBatchBody.contains("must be >= 2"));

        // Invalid time value
        String invalidTimeJson = """
            {
                "name": "invalid_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "search.cancel_after_time_interval": "not_a_time"
                }
            }""";
        ResponseException invalidTimeException = expectThrows(
            ResponseException.class,
            () -> performOperation("PUT", "_wlm/workload_group", invalidTimeJson)
        );
        String invalidTimeBody = EntityUtils.toString(invalidTimeException.getResponse().getEntity());
        assertTrue(invalidTimeBody.contains("search.cancel_after_time_interval"));
        assertTrue(invalidTimeBody.contains("Invalid value"));
    }

    public void testSearchSettingsMergeSemantics() throws Exception {
        // Create with multiple settings
        String createJson = """
            {
                "name": "merge_test",
                "resiliency_mode": "enforced",
                "resource_limits": {"cpu": 0.3, "memory": 0.3},
                "settings": {
                    "search.default_search_timeout": "30s",
                    "search.max_concurrent_shard_requests": "5",
                    "override_request_values": "true"
                }
            }""";
        Response response = performOperation("PUT", "_wlm/workload_group", createJson);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Update only timeout — other settings should persist
        String updateJson = """
            {
                "settings": {
                    "search.default_search_timeout": "1m"
                }
            }""";
        Response updateResponse = performOperation("PUT", "_wlm/workload_group/merge_test", updateJson);
        assertEquals(200, updateResponse.getStatusLine().getStatusCode());

        // Verify merge: timeout updated, max_concurrent persists, override persists
        Response getResponse = performOperation("GET", "_wlm/workload_group/merge_test", null);
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertTrue(responseBody.contains("\"search.default_search_timeout\":\"1m\""));
        assertTrue(responseBody.contains("\"search.max_concurrent_shard_requests\":\"5\""));
        assertTrue(responseBody.contains("\"override_request_values\":\"true\""));

        // Clear all settings with empty object
        String clearJson = """
            {
                "settings": {}
            }""";
        Response clearResponse = performOperation("PUT", "_wlm/workload_group/merge_test", clearJson);
        assertEquals(200, clearResponse.getStatusLine().getStatusCode());

        // Verify cleared — all settings should be gone
        Response getResponse2 = performOperation("GET", "_wlm/workload_group/merge_test", null);
        String responseBody2 = EntityUtils.toString(getResponse2.getEntity());
        assertFalse(responseBody2.contains("\"override_request_values\""));
        assertFalse(responseBody2.contains("\"search.default_search_timeout\""));
        assertFalse(responseBody2.contains("\"search.max_concurrent_shard_requests\""));

        performOperation("DELETE", "_wlm/workload_group/merge_test", null);
    }

    static String getCreateJson(String name, String resiliencyMode, double cpu, double memory) {
        return String.format(Locale.ROOT, """
            {
                "name": "%s",
                "resiliency_mode": "%s",
                "resource_limits": {
                    "cpu" : %s,
                    "memory" : %s
                },
                "settings": {}
            }""", name, resiliencyMode, cpu, memory);
    }

    static String getUpdateJson(String resiliencyMode, double cpu, double memory) {
        return String.format(Locale.ROOT, """
            {
                "resiliency_mode": "%s",
                "resource_limits": {
                    "cpu" : %s,
                    "memory" : %s
                }
            }""", resiliencyMode, cpu, memory);
    }

    Response performOperation(String method, String uriPath, String json) throws IOException {
        Request request = new Request(method, uriPath);
        if (json != null) {
            request.setJsonEntity(json);
        }
        return client().performRequest(request);
    }

    private void setWlmMode(String mode) throws Exception {
        String settingJson = String.format(Locale.ROOT, """
            {
              "persistent": {
                "wlm.workload_group.mode": "%s"
              }
            }
            """, mode);

        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(settingJson);
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
