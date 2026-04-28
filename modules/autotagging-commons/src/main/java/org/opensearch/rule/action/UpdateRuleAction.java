/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.ActionType;

/**
 * Action type for updating Rules
 * @opensearch.experimental
 */
public class UpdateRuleAction extends ActionType<UpdateRuleResponse> {

    /**
     * An instance of UpdateRuleAction
     */
    public static final UpdateRuleAction INSTANCE = new UpdateRuleAction();

    /**
     * Name for UpdateRuleAction
     */
    public static final String NAME = "cluster:admin/opensearch/rule/_update";

    /**
     * Default constructor for UpdateRuleAction
     */
    private UpdateRuleAction() {
        super(NAME, UpdateRuleResponse::new);
    }
}
