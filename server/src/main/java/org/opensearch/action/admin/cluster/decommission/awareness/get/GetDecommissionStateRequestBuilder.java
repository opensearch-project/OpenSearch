/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

/**
 * Get decommission request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class GetDecommissionStateRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    GetDecommissionStateRequest,
    GetDecommissionStateResponse,
    GetDecommissionStateRequestBuilder> {

    /**
     * Creates new get decommissioned attributes request builder
     */
    public GetDecommissionStateRequestBuilder(OpenSearchClient client, GetDecommissionStateAction action) {
        super(client, action, new GetDecommissionStateRequest());
    }

    /**
     * @param attributeName name of attribute
     * @return current object
     */
    public GetDecommissionStateRequestBuilder setAttributeName(String attributeName) {
        request.attributeName(attributeName);
        return this;
    }
}
