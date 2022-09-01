/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.junit.Before;
import org.opensearch.action.admin.cluster.decommission.awareness.put.PutDecommissionRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RestPutDecommissionActionTests extends RestActionTestCase {

    private RestPutDecommissionAction action;

    @Before
    public void setupAction() {
        action = new RestPutDecommissionAction();
        controller().registerHandler(action);
    }

    public void testCreateRequest() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("awareness_attribute_name", "zone");
        params.put("awareness_attribute_value", "zone-1");
        params.put("timeout", "10s");

        RestRequest deprecatedRequest = buildRestRequest(params);

        PutDecommissionRequest request = action.createRequest(deprecatedRequest);
        assertEquals(request.getDecommissionAttribute().attributeName(), "zone");
        assertEquals(request.getDecommissionAttribute().attributeValue(), "zone-1");
        assertEquals(request.getTimeout(), TimeValue.timeValueSeconds(10L));
        assertEquals(deprecatedRequest.getHttpRequest().method(), RestRequest.Method.PUT);
    }

    public void testCreateRequestWithDefaultTimeout() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("awareness_attribute_name", "zone");
        params.put("awareness_attribute_value", "zone-1");

        RestRequest deprecatedRequest = buildRestRequest(params);

        PutDecommissionRequest request = action.createRequest(deprecatedRequest);
        assertEquals(request.getDecommissionAttribute().attributeName(), "zone");
        assertEquals(request.getDecommissionAttribute().attributeValue(), "zone-1");
        assertEquals(request.getTimeout(), TimeValue.timeValueSeconds(300L));
        assertEquals(deprecatedRequest.getHttpRequest().method(), RestRequest.Method.PUT);
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_cluster/decommission/awareness/{awareness_attribute_name}/{awareness_attribute_value}")
            .withParams(params)
            .build();
    }
}
