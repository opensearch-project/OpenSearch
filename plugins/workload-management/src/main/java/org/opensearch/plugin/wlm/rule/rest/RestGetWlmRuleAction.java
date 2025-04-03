/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.rest;

import org.opensearch.action.ActionType;
import org.opensearch.autotagging.Attribute;
import org.opensearch.plugin.wlm.rule.QueryGroupAttribute;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.GetWlmRuleAction;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.rest.RestGetRuleAction;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action to get workload management Rules
 * @opensearch.experimental
 */
public class RestGetWlmRuleAction extends RestGetRuleAction {

    /**
     * Constructor for RestGetWlmRuleAction
     */
    public RestGetWlmRuleAction() {
        super();
    }

    @Override
    public String getName() {
        return "get_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_wlm/rule/"), new Route(GET, "_wlm/rule/{_id}"));
    }

    @Override
    protected Attribute getAttributeFromName(String name) {
        return QueryGroupAttribute.fromName(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends ActionType<? extends GetRuleResponse>> T retrieveGetRuleActionInstance() {
        return (T) GetWlmRuleAction.INSTANCE;
    }

    @Override
    protected GetRuleRequest buildGetRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter) {
        return new GetRuleRequest(id, attributeFilters, searchAfter, QueryGroupFeatureType.INSTANCE);
    }
}
