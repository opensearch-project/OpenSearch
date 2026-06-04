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
 * Transport action to update WorkloadGroup
 *
 * @opensearch.experimental
 */
public class UpdateWorkloadGroupAction extends ActionType<UpdateWorkloadGroupResponse> {

    /**
     * An instance of UpdateWorkloadGroupAction
     */
    public static final UpdateWorkloadGroupAction INSTANCE = new UpdateWorkloadGroupAction();

    /**
     * Name for UpdateWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/workload_group/_update";

    /**
     * Default constructor
     */
    private UpdateWorkloadGroupAction() {
        super(NAME, UpdateWorkloadGroupResponse::new);
    }
}
