/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionType;

/**
 * Transport action to create QueryGroup
 *
 * @opensearch.experimental
 */
public class CreateQueryGroupAction extends ActionType<CreateQueryGroupResponse> {

    /**
     * An instance of CreateQueryGroupAction
     */
    public static final CreateQueryGroupAction INSTANCE = new CreateQueryGroupAction();

    /**
     * Name for CreateQueryGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/query_group/_create";

    /**
     * Default constructor
     */
    private CreateQueryGroupAction() {
        super(NAME, CreateQueryGroupResponse::new);
    }
}
