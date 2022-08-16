/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.get;

import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

public class GetDecommissionRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    GetDecommissionRequest,
    GetDecommissionResponse,
    GetDecommissionRequestBuilder> {

    // TODO - Support for get attribute with the name?

    /**
     * Creates new get decommissioned attributes request builder
     */
    public GetDecommissionRequestBuilder(OpenSearchClient client, GetDecommissionAction action) {
        super(client, action, new GetDecommissionRequest());
    }
}
