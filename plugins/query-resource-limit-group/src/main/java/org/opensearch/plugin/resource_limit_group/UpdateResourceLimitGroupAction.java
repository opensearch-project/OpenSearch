/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionType;

/**
 * Rest action to update Resource Limit Group
 *
 * @opensearch.api
 */
public class UpdateResourceLimitGroupAction extends ActionType<UpdateResourceLimitGroupResponse> {

    /**
     * An instance of UpdateResourceLimitGroupAction
     */
    public static final UpdateResourceLimitGroupAction INSTANCE = new UpdateResourceLimitGroupAction();

    /**
     * Name for UpdateResourceLimitGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/resource_limit_group/_update";

    /**
     * Default constructor
     */
    private UpdateResourceLimitGroupAction() {
        super(NAME, UpdateResourceLimitGroupResponse::new);
    }
}
