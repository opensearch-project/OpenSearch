/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionType;

/**
 * Action to get weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWRRWeightsAction extends ActionType<ClusterGetWRRWeightsResponse> {
    public static final ClusterGetWRRWeightsAction INSTANCE = new ClusterGetWRRWeightsAction();
    public static final String NAME = "cluster:admin/routing/awareness/weights/get";

    private ClusterGetWRRWeightsAction() {
        super(NAME, ClusterGetWRRWeightsResponse::new);
    }
}
