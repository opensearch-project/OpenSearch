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
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import java.util.List;

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
        assertEquals("/_cluster/decommission/awareness", route.getPath());
    }

    public void testCreateRequest() {
        DeleteDecommissionRequest request = action.createRequest();
        assertNotNull(request);
    }

    private FakeRestRequest buildRestRequest() {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/_cluster/decommission/awareness")
            .build();
    }
}
