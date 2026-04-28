/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rule.InMemoryRuleProcessingServiceTests;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.util.Map;

import static org.opensearch.rule.rest.RestGetRuleAction.FEATURE_TYPE;

public class RestGetRuleActionTests extends OpenSearchTestCase {

    private RestGetRuleAction action;

    @Before
    public void setUpAction() {
        action = new RestGetRuleAction();
    }

    public void testGetName() {
        assertEquals("get_rule", action.getName());
    }

    public void testRoutes() {
        var routes = action.routes();
        assertEquals(2, routes.size());
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("_rules/{featureType}/")));
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("_rules/{featureType}/{id}")));
    }

    public void testPrepareRequestFiltersAllowedAttributes() {
        FeatureType featureType = InMemoryRuleProcessingServiceTests.WLMFeatureType.WLM;
        String validAttrName = featureType.getAllowedAttributesRegistry().keySet().iterator().next();
        Map<String, String> params = Map.of(FEATURE_TYPE, featureType.getName(), validAttrName, "value1", "invalidAttr", "shouldBeIgnored");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.GET).withParams(params).build();
        assertNotNull(action.prepareRequest(request, null));
    }
}
