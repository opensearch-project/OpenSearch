/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionType;
import org.opensearch.rule.action.GetRuleResponse;

/**
 * Action type for getting Rules in workload management
 * @opensearch.experimental
 */
public class GetWlmRuleAction extends ActionType<GetRuleResponse> {

    /**
     * An instance of GetWlmRuleAction
     */
    public static final GetWlmRuleAction INSTANCE = new GetWlmRuleAction();

    /**
     * Name for GetWlmRuleAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_get";

    /**
     * Default constructor for GetWlmRuleAction
     */
    private GetWlmRuleAction() {
        super(NAME, GetRuleResponse::new);
    }
}
