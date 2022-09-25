/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.action.ActionType;

/**
 * Action to update weights for weighted round-robin shard routing policy.
 *
 * @opensearch.internal
 */
public final class ClusterPutWRRWeightsAction extends ActionType<ClusterPutWRRWeightsResponse> {

    public static final ClusterPutWRRWeightsAction INSTANCE = new ClusterPutWRRWeightsAction();
    public static final String NAME = "cluster:admin/routing/awareness/weights/put";

    private ClusterPutWRRWeightsAction() {
        super(NAME, ClusterPutWRRWeightsResponse::new);
    }

}
