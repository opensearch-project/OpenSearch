/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.common.annotation.PublicApi;

/**
 * Request builder to update weights for weighted round-robin shard routing policy.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class ClusterPutWeightedRoutingRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterPutWeightedRoutingRequest,
    ClusterPutWeightedRoutingResponse,
    ClusterPutWeightedRoutingRequestBuilder> {
    public ClusterPutWeightedRoutingRequestBuilder(OpenSearchClient client, ClusterAddWeightedRoutingAction action) {
        super(client, action, new ClusterPutWeightedRoutingRequest());
    }

    public ClusterPutWeightedRoutingRequestBuilder setWeightedRouting(WeightedRouting weightedRouting) {
        request.setWeightedRouting(weightedRouting);
        return this;
    }

    public ClusterPutWeightedRoutingRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }
}
