/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Action type for deleting a Rule
 * @opensearch.experimental
 */
public class DeleteRuleAction extends ActionType<AcknowledgedResponse> {

    /**
     * An instance of DeleteRuleAction
     */
    public static final DeleteRuleAction INSTANCE = new DeleteRuleAction();

    /**
     * Name for DeleteRuleAction
     */
    public static final String NAME = "cluster:admin/opensearch/rule/_delete";

    /**
     * Default constructor
     */
    private DeleteRuleAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
