/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Before;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class RestClusterPutWeightedRoutingActionTests extends RestActionTestCase {
    private RestClusterPutWeightedRoutingAction action;

    @Before
    public void setupAction() {
        action = new RestClusterPutWeightedRoutingAction();
        controller().registerHandler(action);
    }

    public void testCreateRequest_SupportedRequestBody() throws IOException {
        String req = "{\"us-east-1c\" : \"1\", \"us-east-1d\":\"1\", \"us-east-1a\":\"0\"}";
        RestRequest restRequest = buildRestRequest(req);
        ClusterPutWeightedRoutingRequest clusterPutWeightedRoutingRequest = RestClusterPutWeightedRoutingAction.createRequest(restRequest);
        assertEquals("zone", clusterPutWeightedRoutingRequest.wrrWeight().attributeName());
        assertNotNull(clusterPutWeightedRoutingRequest.wrrWeight().weights());
        assertEquals("1", clusterPutWeightedRoutingRequest.wrrWeight().weights().get("us-east-1c"));
        assertEquals("1", clusterPutWeightedRoutingRequest.wrrWeight().weights().get("us-east-1d"));
        assertEquals("0", clusterPutWeightedRoutingRequest.wrrWeight().weights().get("us-east-1a"));
    }

    public void testCreateRequest_UnsupportedRequestBody() throws IOException {
        Map<String, String> params = new HashMap<>();
        String req = "[\"us-east-1c\" : \"1\", \"us-east-1d\":\"1\", \"us-east-1a\":\"0\"]";
        RestRequest restRequest = buildRestRequest(req);
        assertThrows(OpenSearchParseException.class, () -> RestClusterPutWeightedRoutingAction.createRequest(restRequest));
    }

    public void testCreateRequest_MalformedRequestBody() throws IOException {
        Map<String, String> params = new HashMap<>();

        String req = "{\"us-east-1c\" : \"1\" \"us-east-1d\":\"1\", \"us-east-1a\":\"0\"}";
        RestRequest restRequest = buildRestRequest(req);
        assertThrows(JsonParseException.class, () -> RestClusterPutWeightedRoutingAction.createRequest(restRequest));
    }

    public void testCreateRequest_EmptyRequestBody() throws IOException {
        String req = "{}";
        RestRequest restRequest = buildRestRequest(req);
        assertThrows(OpenSearchParseException.class, () -> RestClusterPutWeightedRoutingAction.createRequest(restRequest));
    }

    private RestRequest buildRestRequest(String content) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_cluster/routing/awareness/zone/weights")
            .withParams(singletonMap("attribute", "zone"))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

}
