/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Transport action for delete WorkloadGroup
 *
 * @opensearch.experimental
 */
public class DeleteWorkloadGroupAction extends ActionType<AcknowledgedResponse> {

    /**
     /**
     * An instance of DeleteWorkloadGroupAction
     */
    public static final DeleteWorkloadGroupAction INSTANCE = new DeleteWorkloadGroupAction();

    /**
     * Name for DeleteWorkloadGroupAction
     */
    public static final String NAME = "cluster:admin/opensearch/wlm/workload_group/_delete";

    /**
     * Default constructor
     */
    private DeleteWorkloadGroupAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
