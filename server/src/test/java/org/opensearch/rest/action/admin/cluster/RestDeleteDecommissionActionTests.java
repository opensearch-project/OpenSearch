/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.junit.Before;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestDeleteDecommissionActionTests extends RestActionTestCase {

    private RestDeleteDecommissionAction action;

    @Before
    public void setupAction() {
        action = new RestDeleteDecommissionAction();
        controller().registerHandler(action);
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        RestHandler.Route route = routes.get(0);
        assertEquals(route.getMethod(), RestRequest.Method.DELETE);
        assertEquals("/_cluster/decommission/awareness/{awareness_attribute_name}/{awareness_attribute_value}", route.getPath());
    }

    public void testCreateRequest() {
        Map<String, String> params = new HashMap<>();
        params.put("awareness_attribute_name", "zone");
        params.put("awareness_attribute_value", "zone-1");
        params.put("timeout", "10s");

        RestRequest deprecatedRequest = buildRestRequest(params);

        DeleteDecommissionRequest request = action.createRequest(deprecatedRequest);
        assertEquals(request.getDecommissionAttribute().attributeName(), "zone");
        assertEquals(request.getDecommissionAttribute().attributeValue(), "zone-1");
        assertEquals(request.getTimeout(), TimeValue.timeValueSeconds(10L));
    }

    public void testCreateRequestWithDefaultTimeout() {
        Map<String, String> params = new HashMap<>();
        params.put("awareness_attribute_name", "zone");
        params.put("awareness_attribute_value", "zone-1");

        RestRequest deprecatedRequest = buildRestRequest(params);

        DeleteDecommissionRequest request = action.createRequest(deprecatedRequest);
        assertEquals(request.getDecommissionAttribute().attributeName(), "zone");
        assertEquals(request.getDecommissionAttribute().attributeValue(), "zone-1");
        assertEquals(request.getTimeout(), TimeValue.timeValueSeconds(300L));
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/_cluster/decommission/awareness/{awareness_attribute_name}/{awareness_attribute_value}")
            .withParams(params)
            .build();
    }

}
