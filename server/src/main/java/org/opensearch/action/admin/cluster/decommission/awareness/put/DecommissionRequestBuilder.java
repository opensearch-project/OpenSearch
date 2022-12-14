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

    public DecommissionRequestBuilder setDelayTimeOut(TimeValue delayTimeOut) {
        request.setDelayTimeout(delayTimeOut);
        return this;
    }

    public DecommissionRequestBuilder setNoDelay(boolean noDelay) {
        request.setNoDelay(noDelay);
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
     * Sets the timeout for the decommission request
     *
     * @param timeout time out for the request
     * @return current object
     */
    public DecommissionRequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return this;
    }
}
