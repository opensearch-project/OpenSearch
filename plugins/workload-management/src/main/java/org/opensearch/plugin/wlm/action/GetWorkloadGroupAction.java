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
 * Transport action to get WorkloadGroup
 *
 * @opensearch.experimental
 */
public class GetWorkloadGroupAction extends ActionType<GetWorkloadGroupResponse> {

    /**
     * An instance of GetWorkloadGroupAction
     */
    public static final GetWorkloadGroupAction INSTANCE = new GetWorkloadGroupAction();

    /**
     * Name for GetWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/workload_group/_get";

    /**
     * Default constructor
     */
    private GetWorkloadGroupAction() {
        super(NAME, GetWorkloadGroupResponse::new);
    }
}
