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
 * Transport action to get Rule
 * @opensearch.experimental
 */
public class GetRuleAction extends ActionType<GetRuleResponse> {

    /**
     * An instance of GetRuleAction
     */
    public static final GetRuleAction INSTANCE = new GetRuleAction();

    /**
     * Name for GetRuleAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_get";

    /**
     * Default constructor
     */
    private GetRuleAction() {
        super(NAME, GetRuleResponse::new);
    }
}
