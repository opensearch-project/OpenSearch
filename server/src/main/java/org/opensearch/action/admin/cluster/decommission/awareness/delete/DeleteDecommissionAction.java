/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.delete;

import org.opensearch.action.ActionType;

public class DeleteDecommissionAction extends ActionType<DeleteDecommissionResponse> {
    public static final DeleteDecommissionAction INSTANCE = new DeleteDecommissionAction();
    public static final String NAME = "cluster:admin/decommission/awareness/delete";

    private DeleteDecommissionAction() {
        super(NAME, DeleteDecommissionResponse::new);
    }
}
