/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.get;

import org.opensearch.action.ActionType;

/**
 * Action to get weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWeightedRoutingAction extends ActionType<ClusterGetWeightedRoutingResponse> {
    public static final ClusterGetWeightedRoutingAction INSTANCE = new ClusterGetWeightedRoutingAction();
    public static final String NAME = "cluster:admin/routing/awareness/weights/get";

    private ClusterGetWeightedRoutingAction() {
        super(NAME, ClusterGetWeightedRoutingResponse::new);
    }
}
