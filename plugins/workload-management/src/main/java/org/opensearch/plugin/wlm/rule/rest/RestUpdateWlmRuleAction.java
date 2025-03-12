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
import org.opensearch.autotagging.FeatureType;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.UpdateWlmRuleAction;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.rest.RestUpdateRuleAction;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to update a workload management Rule
 * @opensearch.experimental
 */
public class RestUpdateWlmRuleAction extends RestUpdateRuleAction {

    /**
     * Constructor for RestUpdateWlmRuleAction
     */
    public RestUpdateWlmRuleAction() {
        super();
    }

    @Override
    public String getName() {
        return "update_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/rule/{_id}"), new Route(PUT, "_wlm/rule/{_id}"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends ActionType<UpdateRuleResponse>> T retrieveUpdateRuleActionInstance() {
        return (T) UpdateWlmRuleAction.INSTANCE;
    }

    @Override
    protected FeatureType retrieveFeatureTypeInstance() {
        return QueryGroupFeatureType.INSTANCE;
    }

    @Override
    protected UpdateRuleRequest buildUpdateRuleRequest(
        String id,
        String description,
        Map<Attribute, Set<String>> attributeMap,
        String featureValue
    ) {
        return new UpdateRuleRequest(id, description, attributeMap, featureValue, QueryGroupFeatureType.INSTANCE);
    }
}
