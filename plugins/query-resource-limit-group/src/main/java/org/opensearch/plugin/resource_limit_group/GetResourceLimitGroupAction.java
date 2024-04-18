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
 * Rest action to get Resource Limit Group
 *
 * @opensearch.api
 */
public class GetResourceLimitGroupAction extends ActionType<GetResourceLimitGroupResponse> {

    /**
    /**
     * An instance of GetResourceLimitGroupAction
     */
    public static final GetResourceLimitGroupAction INSTANCE = new GetResourceLimitGroupAction();

    /**
     * Name for GetResourceLimitGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/resource_limit_group/_get";

    /**
     * Default constructor
     */
    private GetResourceLimitGroupAction() {
        super(NAME, GetResourceLimitGroupResponse::new);
    }
}
