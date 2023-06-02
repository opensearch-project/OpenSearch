/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.delete;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 *  Delete decommission state action.
 *
 * @opensearch.internal
 */
public class DeleteDecommissionStateAction extends ActionType<DeleteDecommissionStateResponse> {
    public static final DeleteDecommissionStateAction INSTANCE = new DeleteDecommissionStateAction();
    public static final String NAME = "cluster:admin/decommission/awareness/delete";

    private DeleteDecommissionStateAction() {
        super(NAME, DeleteDecommissionStateResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Cluster_ALL);
    }
}
