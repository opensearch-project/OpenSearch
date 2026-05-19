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
 * Transport action to create WorkloadGroup
 *
 * @opensearch.experimental
 */
public class CreateWorkloadGroupAction extends ActionType<CreateWorkloadGroupResponse> {

    /**
     * An instance of CreateWorkloadGroupAction
     */
    public static final CreateWorkloadGroupAction INSTANCE = new CreateWorkloadGroupAction();

    /**
     * Name for CreateWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/workload_group/_create";

    /**
     * Default constructor
     */
    private CreateWorkloadGroupAction() {
        super(NAME, CreateWorkloadGroupResponse::new);
    }
}
