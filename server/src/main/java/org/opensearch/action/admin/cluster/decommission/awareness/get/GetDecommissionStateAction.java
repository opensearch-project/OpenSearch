/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 * Get decommission action
 *
 * @opensearch.internal
 */
public class GetDecommissionStateAction extends ActionType<GetDecommissionStateResponse> {

    public static final GetDecommissionStateAction INSTANCE = new GetDecommissionStateAction();
    public static final String NAME = "cluster:admin/decommission/awareness/get";

    private GetDecommissionStateAction() {
        super(NAME, GetDecommissionStateResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Cluster_ALL);
    }
}
