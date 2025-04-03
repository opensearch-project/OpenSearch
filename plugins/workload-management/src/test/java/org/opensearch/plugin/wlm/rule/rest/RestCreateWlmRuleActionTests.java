
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.rest;

import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestCreateWlmRuleActionTests extends OpenSearchTestCase {
    /**
     * Test case to validate the construction for RestGetRuleAction
     */
    public void testConstruction() {
        RestCreateWlmRuleAction action = new RestCreateWlmRuleAction();
        assertNotNull(action);
        assertEquals("create_rule", action.getName());
        List<RestHandler.Route> routes = action.routes();
        assertEquals(2, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(POST, route.getMethod());
        assertEquals("_wlm/rule/", route.getPath());
        route = routes.get(1);
        assertEquals(PUT, route.getMethod());
        assertEquals("_wlm/rule/", route.getPath());
    }
}
