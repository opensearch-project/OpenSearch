/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

/**
 * Transport action for delete QueryGroup
 *
 * @opensearch.experimental
 */
public class DeleteQueryGroupAction extends ActionType<AcknowledgedResponse> {

    /**
     /**
     * An instance of DeleteQueryGroupAction
     */
    public static final DeleteQueryGroupAction INSTANCE = new DeleteQueryGroupAction();

    /**
     * Name for DeleteQueryGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/query_group/_delete";

    /**
     * Default constructor
     */
    private DeleteQueryGroupAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
