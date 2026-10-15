/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexingPressure;
import org.opensearch.index.ShardIndexingPressureSettings;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.rest.RestStatus.CREATED;
import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class BulkShardIndexingPressureRestIT extends HttpSmokeTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "16KB")
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testShardPressureRejectionsReturnBulkItemErrors() throws Exception {
        final String index = "bulk-pressure";
        final Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        final Response createResponse = getRestClient().performRequest(createIndex);
        assertEquals(OK.getStatus(), createResponse.getStatusLine().getStatusCode());
        assertEquals(Boolean.TRUE, entityAsMap(createResponse).get("acknowledged"));
        ensureGreen(index);

        // A single request exceeds the node limit, so rejection does not depend on concurrent indexing or timing.
        final String oversizedSource = "{\"message\":\"" + "x".repeat(64 * 1024) + "\"}";
        final Request rejectedBulk = new Request("POST", "/" + index + "/_bulk");
        rejectedBulk.setJsonEntity(
            "{\"index\":{\"_id\":\"rejected-1\"}}\n"
                + oversizedSource
                + "\n{\"index\":{\"_id\":\"rejected-2\"}}\n"
                + oversizedSource
                + "\n"
        );
        final Response rejectedResponse = getRestClient().performRequest(rejectedBulk);
        assertEquals(OK.getStatus(), rejectedResponse.getStatusLine().getStatusCode());
        final Map<String, Object> rejectedBody = entityAsMap(rejectedResponse);
        assertEquals(Boolean.TRUE, rejectedBody.get("errors"));
        final List<Map<String, Object>> rejectedItems = (List<Map<String, Object>>) rejectedBody.get("items");
        assertEquals(2, rejectedItems.size());
        for (int i = 0; i < rejectedItems.size(); i++) {
            final Map<String, Object> item = (Map<String, Object>) rejectedItems.get(i).get("index");
            assertEquals(index, item.get("_index"));
            assertEquals("rejected-" + (i + 1), item.get("_id"));
            assertEquals(TOO_MANY_REQUESTS.getStatus(), item.get("status"));
            final Map<String, Object> error = (Map<String, Object>) item.get("error");
            assertEquals("rejected_execution_exception", error.get("type"));
            assertDocumentMissing(index, "rejected-" + (i + 1));
        }

        final Request successfulBulk = new Request("POST", "/" + index + "/_bulk");
        successfulBulk.setJsonEntity(
            "{\"index\":{\"_id\":\"accepted\"}}\n{\"message\":\"small bulk request after rejection\",\"sequence\":42}\n"
        );
        final Response successfulResponse = getRestClient().performRequest(successfulBulk);
        assertEquals(OK.getStatus(), successfulResponse.getStatusLine().getStatusCode());
        final Map<String, Object> successfulBody = entityAsMap(successfulResponse);
        assertEquals(Boolean.FALSE, successfulBody.get("errors"));
        final List<Map<String, Object>> successfulItems = (List<Map<String, Object>>) successfulBody.get("items");
        assertEquals(1, successfulItems.size());
        final Map<String, Object> successfulItem = (Map<String, Object>) successfulItems.get(0).get("index");
        assertEquals(CREATED.getStatus(), successfulItem.get("status"));
        assertEquals("accepted", successfulItem.get("_id"));
        final Response storedResponse = getRestClient().performRequest(new Request("GET", "/" + index + "/_doc/accepted"));
        assertEquals(OK.getStatus(), storedResponse.getStatusLine().getStatusCode());
        final Map<String, Object> storedBody = entityAsMap(storedResponse);
        assertEquals(Boolean.TRUE, storedBody.get("found"));
        assertEquals(Map.of("message", "small bulk request after rejection", "sequence", 42), storedBody.get("_source"));

        // The single-document API must still unwrap its one failed bulk item into an HTTP-level rejection.
        final Request rejectedSingle = new Request("PUT", "/" + index + "/_doc/rejected-single");
        rejectedSingle.setJsonEntity(oversizedSource);
        assertRequestRejected(rejectedSingle);
        assertDocumentMissing(index, "rejected-single");

        try {
            setShardPressureEnabled(false);
            // With shard pressure disabled, whole-request coordinating admission must still return HTTP 429.
            final Request rejectedNodeBulk = new Request("POST", "/" + index + "/_bulk");
            rejectedNodeBulk.setJsonEntity("{\"index\":{\"_id\":\"rejected-node\"}}\n" + oversizedSource + "\n");
            assertRequestRejected(rejectedNodeBulk);
            assertDocumentMissing(index, "rejected-node");
        } finally {
            setShardPressureEnabled(null);
        }
    }

    @SuppressWarnings("unchecked")
    public void testBulkReturnsSuccessAndShardPressureFailure() throws Exception {
        final String successfulIndex = "bulk-pressure-success";
        final String rejectedIndex = "bulk-pressure-rejected";
        // Separate one-shard indices guarantee distinct shard groups without relying on document routing.
        for (String index : List.of(successfulIndex, rejectedIndex)) {
            final Request createIndex = new Request("PUT", "/" + index);
            createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
            final Response createResponse = getRestClient().performRequest(createIndex);
            assertEquals(OK.getStatus(), createResponse.getStatusLine().getStatusCode());
            assertEquals(Boolean.TRUE, entityAsMap(createResponse).get("acknowledged"));
        }
        ensureGreen(successfulIndex, rejectedIndex);

        final String successfulSource = "{\"message\":\"small request on a healthy shard\",\"sequence\":42}";
        final String rejectedSource = "{\"message\":\"" + "x".repeat(64 * 1024) + "\"}";
        final Request bulk = new Request("POST", "/_bulk");
        bulk.setJsonEntity(
            "{\"index\":{\"_index\":\""
                + successfulIndex
                + "\",\"_id\":\"accepted\"}}\n"
                + successfulSource
                + "\n{\"index\":{\"_index\":\""
                + rejectedIndex
                + "\",\"_id\":\"rejected\"}}\n"
                + rejectedSource
                + "\n"
        );

        final Response response = getRestClient().performRequest(bulk);
        assertEquals(OK.getStatus(), response.getStatusLine().getStatusCode());
        final Map<String, Object> body = entityAsMap(response);
        assertEquals(Boolean.TRUE, body.get("errors"));
        final List<Map<String, Object>> items = (List<Map<String, Object>>) body.get("items");
        assertEquals(2, items.size());

        final Map<String, Object> successfulItem = (Map<String, Object>) items.get(0).get("index");
        assertEquals(successfulIndex, successfulItem.get("_index"));
        assertEquals("accepted", successfulItem.get("_id"));
        assertEquals(CREATED.getStatus(), successfulItem.get("status"));
        assertEquals("created", successfulItem.get("result"));
        assertFalse(successfulItem.containsKey("error"));

        final Map<String, Object> rejectedItem = (Map<String, Object>) items.get(1).get("index");
        assertEquals(rejectedIndex, rejectedItem.get("_index"));
        assertEquals("rejected", rejectedItem.get("_id"));
        assertEquals(TOO_MANY_REQUESTS.getStatus(), rejectedItem.get("status"));
        final Map<String, Object> error = (Map<String, Object>) rejectedItem.get("error");
        assertEquals("rejected_execution_exception", error.get("type"));

        final Response storedResponse = getRestClient().performRequest(new Request("GET", "/" + successfulIndex + "/_doc/accepted"));
        assertEquals(OK.getStatus(), storedResponse.getStatusLine().getStatusCode());
        final Map<String, Object> storedBody = entityAsMap(storedResponse);
        assertEquals(Boolean.TRUE, storedBody.get("found"));
        assertEquals(Map.of("message", "small request on a healthy shard", "sequence", 42), storedBody.get("_source"));
        assertDocumentMissing(rejectedIndex, "rejected");
    }

    @SuppressWarnings("unchecked")
    private void assertRequestRejected(Request request) throws IOException {
        final ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        final Response response = exception.getResponse();
        assertEquals(TOO_MANY_REQUESTS.getStatus(), response.getStatusLine().getStatusCode());
        final Map<String, Object> body = entityAsMap(response);
        assertEquals(TOO_MANY_REQUESTS.getStatus(), body.get("status"));
        final Map<String, Object> error = (Map<String, Object>) body.get("error");
        assertEquals("rejected_execution_exception", error.get("type"));
    }

    private void assertDocumentMissing(String index, String id) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_doc/" + id);
        final ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        final Response response = exception.getResponse();
        assertEquals(NOT_FOUND.getStatus(), response.getStatusLine().getStatusCode());
        assertEquals(Boolean.FALSE, entityAsMap(response).get("found"));
    }

    private void setShardPressureEnabled(Boolean enabled) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"transient\":{\"" + ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey() + "\":" + enabled + "}}"
        );
        final Response response = getRestClient().performRequest(request);
        assertEquals(OK.getStatus(), response.getStatusLine().getStatusCode());
        assertEquals(Boolean.TRUE, entityAsMap(response).get("acknowledged"));
    }
}
