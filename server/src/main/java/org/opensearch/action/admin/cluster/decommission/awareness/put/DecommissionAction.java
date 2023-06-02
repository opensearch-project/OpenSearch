/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 * Register decommission action
 *
 * @opensearch.internal
 */
public final class DecommissionAction extends ActionType<DecommissionResponse> {
    public static final DecommissionAction INSTANCE = new DecommissionAction();
    public static final String NAME = "cluster:admin/decommission/awareness/put";

    private DecommissionAction() {
        super(NAME, DecommissionResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Cluster_ALL);
    }
}
