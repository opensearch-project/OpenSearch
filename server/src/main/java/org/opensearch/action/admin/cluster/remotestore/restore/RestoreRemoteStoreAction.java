/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 * Restore remote store action
 *
 * @opensearch.internal
 */
public final class RestoreRemoteStoreAction extends ActionType<RestoreRemoteStoreResponse> {

    public static final RestoreRemoteStoreAction INSTANCE = new RestoreRemoteStoreAction();
    public static final String NAME = "cluster:admin/remotestore/restore";

    private RestoreRemoteStoreAction() {
        super(NAME, RestoreRemoteStoreResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Cluster_ALL);
    }
}
