/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.routing.WRRWeight;

import java.util.List;

/**
 * Request builder to get weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWRRWeightsRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterGetWRRWeightsRequest,
    ClusterGetWRRWeightsResponse,
    ClusterGetWRRWeightsRequestBuilder> {

    public ClusterGetWRRWeightsRequestBuilder(OpenSearchClient client, ClusterGetWRRWeightsAction action) {
        super(client, action, new ClusterGetWRRWeightsRequest());
    }

    public ClusterGetWRRWeightsRequestBuilder setWeights(List<WRRWeight> weights) {
        request.weights(weights);
        return this;
    }

    public ClusterGetWRRWeightsRequestBuilder setRequestLocal(boolean local) {
        request.local(local);
        return this;

    }


}
