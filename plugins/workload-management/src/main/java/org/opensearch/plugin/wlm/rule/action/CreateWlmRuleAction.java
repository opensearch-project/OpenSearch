/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionType;
import org.opensearch.rule.action.CreateRuleResponse;

/**
 * Action type for creating a Rule in workload management
 * @opensearch.experimental
 */
public class CreateWlmRuleAction extends ActionType<CreateRuleResponse> {

    /**
     * An instance of CreateWlmRuleAction
     */
    public static final CreateWlmRuleAction INSTANCE = new CreateWlmRuleAction();

    /**
     * Name for CreateWlmRuleAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_create";

    /**
     * Default constructor
     */
    private CreateWlmRuleAction() {
        super(NAME, CreateRuleResponse::new);
    }
}
