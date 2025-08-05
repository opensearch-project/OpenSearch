/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.test.OpenSearchTestCase;

public class RestUpdateRuleActionTests extends OpenSearchTestCase {
    RestUpdateRuleAction action = new RestUpdateRuleAction();;

    public void testGetName() {
        assertEquals("update_rule", action.getName());
    }

    public void testRoutes() {
        var routes = action.routes();
        assertEquals(1, routes.size());
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("_rules/{featureType}/{id}")));
    }
}
