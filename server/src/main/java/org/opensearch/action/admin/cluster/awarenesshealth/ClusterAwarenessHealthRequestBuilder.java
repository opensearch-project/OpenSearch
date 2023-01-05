/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.awarenesshealth;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Builder for requesting cluster Awareness health
 *
 * @opensearch.internal
 */
public class ClusterAwarenessHealthRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterAwarenessHealthRequest,
    ClusterAwarenessHealthResponse,
    ClusterAwarenessHealthRequestBuilder> {

    protected ClusterAwarenessHealthRequestBuilder(
        OpenSearchClient client,
        ActionType<ClusterAwarenessHealthResponse> action,
        ClusterAwarenessHealthRequest request
    ) {
        super(client, action, request);
    }

    /**
     * @param awarenessAttributeName name of awareness attribute
     * @return current object
     */
    public ClusterAwarenessHealthRequestBuilder setAttributeName(String awarenessAttributeName) {
        request.setAwarenessAttributeName(awarenessAttributeName);
        return this;
    }
}
