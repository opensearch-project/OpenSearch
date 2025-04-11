/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionType;
import org.opensearch.rule.action.UpdateRuleResponse;

/**
 * Action type for updating a Rule in workload management
 *
 * @opensearch.experimental
 */
public class UpdateWlmRuleAction extends ActionType<UpdateRuleResponse> {

    /**
     * An instance of UpdateWlmRuleAction
     */
    public static final UpdateWlmRuleAction INSTANCE = new UpdateWlmRuleAction();

    /**
     * Name for UpdateWlmRuleAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_update";

    /**
     * Default constructor
     */
    private UpdateWlmRuleAction() {
        super(NAME, UpdateRuleResponse::new);
    }
}
