/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.unit.TimeValue;

/**
 * Register decommission request builder
 *
 * @opensearch.internal
 */
public class DecommissionRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    DecommissionRequest,
    DecommissionResponse,
    DecommissionRequestBuilder> {

    public DecommissionRequestBuilder(OpenSearchClient client, ActionType<DecommissionResponse> action, DecommissionRequest request) {
        super(client, action, request);
    }

    /**
     * @param decommissionAttribute decommission attribute
     * @return current object
     */
    public DecommissionRequestBuilder setDecommissionedAttribute(DecommissionAttribute decommissionAttribute) {
        request.setDecommissionAttribute(decommissionAttribute);
        return this;
    }

    /**
     * Sets retryOnClusterManagerChange for decommission request
     *
     * @param retryOnClusterManagerChange boolean for request to retry decommission action on cluster manager change
     * @return current object
     */
    public DecommissionRequestBuilder setRetryOnClusterManagerChange(boolean retryOnClusterManagerChange) {
        request.setRetryOnClusterManagerChange(retryOnClusterManagerChange);
        return this;
    }

    /**
     * Sets the retry timeout for the decommission request
     *
     * @param retryTimeout retry time out for the request
     * @return current object
     */
    public DecommissionRequestBuilder setRetryTimeout(TimeValue retryTimeout) {
        request.setRetryTimeout(retryTimeout);
        return this;
    }
}
