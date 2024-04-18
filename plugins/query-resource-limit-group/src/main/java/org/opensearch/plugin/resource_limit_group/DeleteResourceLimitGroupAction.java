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
 * Rest action to delete Resource Limit Group
 *
 * @opensearch.api
 */
public class DeleteResourceLimitGroupAction extends ActionType<DeleteResourceLimitGroupResponse> {

    /**
    /**
     * An instance of DeleteResourceLimitGroupAction
     */
    public static final DeleteResourceLimitGroupAction INSTANCE = new DeleteResourceLimitGroupAction();

    /**
     * Name for DeleteResourceLimitGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/resource_limit_group/_delete";

    /**
     * Default constructor
     */
    private DeleteResourceLimitGroupAction() {
        super(NAME, DeleteResourceLimitGroupResponse::new);
    }
}
