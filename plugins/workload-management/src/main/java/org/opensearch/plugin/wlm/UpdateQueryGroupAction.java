/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionType;

/**
 * Transport action to update QueryGroup
 *
 * @opensearch.api
 */
public class UpdateQueryGroupAction extends ActionType<UpdateQueryGroupResponse> {

    /**
     * An instance of UpdateQueryGroupAction
     */
    public static final UpdateQueryGroupAction INSTANCE = new UpdateQueryGroupAction();

    /**
     * Name for UpdateQueryGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/query_group/_update";

    /**
     * Default constructor
     */
    private UpdateQueryGroupAction() {
        super(NAME, UpdateQueryGroupResponse::new);
    }
}
