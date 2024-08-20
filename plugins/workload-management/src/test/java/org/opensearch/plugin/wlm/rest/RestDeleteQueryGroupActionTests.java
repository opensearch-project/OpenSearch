/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

public class RestDeleteQueryGroupActionTests extends OpenSearchTestCase {

    /**
     * Test case to validate the construction for RestDeleteQueryGroupAction
     */
    public void testConstruction() {
        RestDeleteQueryGroupAction action = new RestDeleteQueryGroupAction();
        assertNotNull(action);
        assertEquals("delete_query_group", action.getName());
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(DELETE, route.getMethod());
        assertEquals("_wlm/query_group/{name}", route.getPath());
    }
}
