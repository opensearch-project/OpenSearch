/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.rest;

import org.opensearch.action.ActionType;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.CreateWlmRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.rest.RestCreateRuleAction;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to create a Rule in workload management
 * @opensearch.experimental
 */
public class RestCreateWlmRuleAction extends RestCreateRuleAction {

    /**
     * Constructor for RestCreateWlmRuleAction
     */
    public RestCreateWlmRuleAction() {
        super();
    }

    @Override
    public String getName() {
        return "create_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/rule/"), new Route(PUT, "_wlm/rule/"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends ActionType<? extends CreateRuleResponse>> T retrieveCreateRuleActionInstance() {
        return (T) CreateWlmRuleAction.INSTANCE;
    }

    @Override
    protected FeatureType retrieveFeatureTypeInstance() {
        return QueryGroupFeatureType.INSTANCE;
    }

    @Override
    protected CreateRuleRequest buildCreateRuleRequest(Rule rule) {
        return new CreateRuleRequest(rule);
    }
}
