/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 * Remote store stats action
 *
 * @opensearch.internal
 */
public class RemoteStoreStatsAction extends ActionType<RemoteStoreStatsResponse> {

    public static final RemoteStoreStatsAction INSTANCE = new RemoteStoreStatsAction();
    public static final String NAME = "cluster:monitor/_remotestore/stats";

    private RemoteStoreStatsAction() {
        super(NAME, RemoteStoreStatsResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Cluster_ALL);
    }
}
