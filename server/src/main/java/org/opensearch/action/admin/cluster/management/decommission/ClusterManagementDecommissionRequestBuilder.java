/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.management.decommission;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.unit.TimeValue;

/**
 * Builder for a decommission request
 *
 * @opensearch.internal
 */
public class ClusterManagementDecommissionRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    ClusterManagementDecommissionRequest,
    ClusterManagementDecommissionResponse,
    ClusterManagementDecommissionRequestBuilder> {
    public ClusterManagementDecommissionRequestBuilder(OpenSearchClient client, ClusterManagementDecommissionAction action) {
        super(client, action, new ClusterManagementDecommissionRequest());
    }

    /**
     * Set Node Id
     */
    public ClusterManagementDecommissionRequestBuilder setNodeIds(String... nodeIds) {
        request.setNodeIds(nodeIds);
        return this;
    }

    /**
     * Set Node name
     */
    public ClusterManagementDecommissionRequestBuilder setNodeNames(String... nodeNames) {
        request.setNodeNames(nodeNames);
        return this;
    }

    /**
     * Set timeout
     */
    public ClusterManagementDecommissionRequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return this;
    }
}

