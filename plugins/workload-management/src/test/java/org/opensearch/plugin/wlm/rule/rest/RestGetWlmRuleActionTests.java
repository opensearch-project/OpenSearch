
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.rest;

import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;

import static org.opensearch.plugin.wlm.rule.QueryGroupAttribute.INDEX_PATTERN;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetWlmRuleActionTests extends OpenSearchTestCase {
    /**
     * Test case to validate the construction for RestGetRuleAction
     */
    public void testConstruction() {
        RestGetWlmRuleAction action = new RestGetWlmRuleAction();
        assertNotNull(action);
        assertEquals("get_rule", action.getName());
        List<RestHandler.Route> routes = action.routes();
        assertEquals(2, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(GET, route.getMethod());
        assertEquals("_wlm/rule/", route.getPath());
        route = routes.get(1);
        assertEquals(GET, route.getMethod());
        assertEquals("_wlm/rule/{_id}", route.getPath());
    }

    public void testParseAttributeValues() {
        RestGetWlmRuleAction restGetWlmRuleAction = new RestGetWlmRuleAction();
        HashSet<String> result = restGetWlmRuleAction.parseAttributeValues("a,b,c", INDEX_PATTERN.name());
        assertEquals(3, result.size());
        assertTrue(result.contains("a"));
    }

    public void testParseAttributeValues_Error() {
        RestGetWlmRuleAction restGetWlmRuleAction = new RestGetWlmRuleAction();
        int attributeLen = QueryGroupFeatureType.DEFAULT_MAX_ATTRIBUTE_VALUES + 1;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < attributeLen; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("v").append(i);
        }
        assertThrows(
            IllegalArgumentException.class,
            () -> restGetWlmRuleAction.parseAttributeValues(sb.toString(), INDEX_PATTERN.getName())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> restGetWlmRuleAction.parseAttributeValues(
                randomAlphaOfLength(QueryGroupFeatureType.DEFAULT_MAX_ATTRIBUTE_VALUE_LENGTH + 1),
                INDEX_PATTERN.getName()
            )
        );
    }

    public void testGetAttributeFromName() {
        RestGetWlmRuleAction restGetWlmRuleAction = new RestGetWlmRuleAction();
        assertEquals(INDEX_PATTERN, restGetWlmRuleAction.getAttributeFromName(INDEX_PATTERN.getName()));
        assertThrows(IllegalArgumentException.class, () -> restGetWlmRuleAction.getAttributeFromName("invalid"));
    }

}
