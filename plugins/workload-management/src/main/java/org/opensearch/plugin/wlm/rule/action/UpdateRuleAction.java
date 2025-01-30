/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionType;

/**
 * Transport action to update Rule
 *
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
    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_update";

    /**
     * Default constructor
     */
    private UpdateRuleAction() {
        super(NAME, UpdateRuleResponse::new);
    }
}
