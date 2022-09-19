/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.opensearch.action.ActionType;

/**
 * Action to update weights for weighted round-robin shard routing policy.
 *
 * @opensearch.internal
 */
public final class ClusterPutWeightedRoutingAction extends ActionType<ClusterPutWeightedRoutingResponse> {

    public static final ClusterPutWeightedRoutingAction INSTANCE = new ClusterPutWeightedRoutingAction();
    public static final String NAME = "cluster:admin/routing/awareness/weights/put";

    private ClusterPutWeightedRoutingAction() {
        super(NAME, ClusterPutWeightedRoutingResponse::new);
    }

}
