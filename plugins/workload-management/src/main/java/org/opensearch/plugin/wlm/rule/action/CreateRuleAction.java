/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionType;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;

/**
 * Transport action to create Rule
 *
 * @opensearch.experimental
 */
public class CreateRuleAction extends ActionType<CreateRuleResponse> {

    /**
     * An instance of CreateQueryGroupAction
     */
    public static final CreateRuleAction INSTANCE = new CreateRuleAction();

    /**
     * Name for CreateQueryGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_create";

    /**
     * Default constructor
     */
    private CreateRuleAction() {
        super(NAME, CreateRuleResponse::new);
    }
}
