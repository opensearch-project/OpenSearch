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
 * Rest action to create Resource Limit Group
 *
 * @opensearch.api
 */
public class CreateResourceLimitGroupAction extends ActionType<CreateResourceLimitGroupResponse> {

    /**
     * An instance of CreateResourceLimitGroupAction
     */
    public static final CreateResourceLimitGroupAction INSTANCE = new CreateResourceLimitGroupAction();

    /**
     * Name for CreateResourceLimitGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/resource_limit_group/_create";

    /**
     * Default constructor
     */
    private CreateResourceLimitGroupAction() {
        super(NAME, CreateResourceLimitGroupResponse::new);
    }
}
