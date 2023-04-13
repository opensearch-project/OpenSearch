/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.ActionType;

public class RemoteStoreStatsAction extends ActionType<RemoteStoreStatsResponse> {

    public static final RemoteStoreStatsAction INSTANCE = new RemoteStoreStatsAction();
    public static final String NAME = "cluster:monitor/_cat/remote_store";

    private RemoteStoreStatsAction() {
        super(NAME, RemoteStoreStatsResponse::new);
    }
}
