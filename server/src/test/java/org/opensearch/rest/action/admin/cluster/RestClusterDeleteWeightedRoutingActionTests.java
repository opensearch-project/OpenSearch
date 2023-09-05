/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.junit.Before;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import java.io.IOException;

import static java.util.Collections.singletonMap;

public class RestClusterDeleteWeightedRoutingActionTests extends RestActionTestCase {
    private RestClusterPutWeightedRoutingAction action;

    @Before
    public void setupAction() {
        action = new RestClusterPutWeightedRoutingAction();
        controller().registerHandler(action);
    }

    public void testDeleteRequest_SupportedRequestBody() throws IOException {
        String req = "{\"_version\":2}";
        RestRequest restRequest = buildRestRequest(req);
        ClusterDeleteWeightedRoutingRequest clusterDeleteWeightedRoutingRequest = RestClusterDeleteWeightedRoutingAction.createRequest(
            restRequest
        );
        assertEquals(2, clusterDeleteWeightedRoutingRequest.getVersion());

        restRequest = buildRestRequestWithAwarenessAttribute(req);
        clusterDeleteWeightedRoutingRequest = RestClusterDeleteWeightedRoutingAction.createRequest(restRequest);
        assertEquals("zone", clusterDeleteWeightedRoutingRequest.getAwarenessAttribute());
        assertEquals(2, clusterDeleteWeightedRoutingRequest.getVersion());
    }

    public void testDeleteRequest_BadRequest() throws IOException {
        String req = "{\"_ver\":2}";
        RestRequest restRequest = buildRestRequest(req);
        assertThrows(OpenSearchParseException.class, () -> RestClusterDeleteWeightedRoutingAction.createRequest(restRequest));

        RestRequest restRequest2 = buildRestRequestWithAwarenessAttribute(req);
        assertThrows(OpenSearchParseException.class, () -> RestClusterDeleteWeightedRoutingAction.createRequest(restRequest2));
    }

    private RestRequest buildRestRequestWithAwarenessAttribute(String content) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/_cluster/routing/awareness/zone/weights")
            .withParams(singletonMap("attribute", "zone"))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

    private RestRequest buildRestRequest(String content) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/_cluster/routing/awareness/weights")
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

    public void testCreateRequest_EmptyRequestBody() throws IOException {
        String req = "{}";
        RestRequest restRequest = buildRestRequest(req);
        assertThrows(OpenSearchParseException.class, () -> RestClusterDeleteWeightedRoutingAction.createRequest(restRequest));

        RestRequest restRequest2 = buildRestRequestWithAwarenessAttribute(req);
        assertThrows(OpenSearchParseException.class, () -> RestClusterDeleteWeightedRoutingAction.createRequest(restRequest2));
    }
}
