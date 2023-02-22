/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.delete;

import org.opensearch.action.ActionType;

/**
 * Action to delete weights for weighted round-robin shard routing policy.
 *
 * @opensearch.internal
 */
public class ClusterDeleteWeightedRoutingAction extends ActionType<ClusterDeleteWeightedRoutingResponse> {
    public static final ClusterDeleteWeightedRoutingAction INSTANCE = new ClusterDeleteWeightedRoutingAction();
    public static final String NAME = "cluster:admin/routing/awareness/weights/delete";

    private ClusterDeleteWeightedRoutingAction() {
        super(NAME, ClusterDeleteWeightedRoutingResponse::new);
    }
}
