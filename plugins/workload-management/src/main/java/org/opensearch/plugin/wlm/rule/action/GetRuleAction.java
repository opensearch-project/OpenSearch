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
 * Action type for getting Rules in workload management
 * @opensearch.experimental
 */
public class GetRuleAction extends ActionType<GetRuleResponse> {

    public static final GetRuleAction INSTANCE = new GetRuleAction();

    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_get";

    private GetRuleAction() {
        super(NAME, GetRuleResponse::new);
    }
}
