/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.delete;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Builder for Delete decommission request.
 *
 * @opensearch.internal
 */
public class DeleteDecommissionStateRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    DeleteDecommissionStateRequest,
    DeleteDecommissionStateResponse,
    DeleteDecommissionStateRequestBuilder> {

    public DeleteDecommissionStateRequestBuilder(OpenSearchClient client, DeleteDecommissionStateAction action) {
        super(client, action, new DeleteDecommissionStateRequest());
    }
}
