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
 * Action type for deleting Rules in workload management
 * @opensearch.experimental
 */
public class DeleteRuleAction extends ActionType<DeleteRuleResponse> {

    public static final DeleteRuleAction INSTANCE = new DeleteRuleAction();

    public static final String NAME = "cluster:admin/opensearch/wlm/rule/_delete";

    private DeleteRuleAction() {
        super(NAME, DeleteRuleResponse::new);
    }
}

