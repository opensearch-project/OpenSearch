/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.clustermanager;

import org.opensearch.action.ActionType;
import org.opensearch.action.ProtobufActionRequestBuilder;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.ProtobufOpenSearchClient;
import org.opensearch.common.unit.TimeValue;

/**
 * Base request builder for cluster-manager node operations
*
* @opensearch.internal
*/
public abstract class ProtobufClusterManagerNodeOperationRequestBuilder<
    Request extends ProtobufClusterManagerNodeRequest<Request>,
    Response extends ProtobufActionResponse,
    RequestBuilder extends ProtobufClusterManagerNodeOperationRequestBuilder<Request, Response, RequestBuilder>> extends ProtobufActionRequestBuilder<
        Request,
        Response> {

    protected ProtobufClusterManagerNodeOperationRequestBuilder(ProtobufOpenSearchClient client, ProtobufActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the cluster-manager node timeout in case the cluster-manager has not yet been discovered.
    */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setClusterManagerNodeTimeout(TimeValue timeout) {
        request.clusterManagerNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Sets the cluster-manager node timeout in case the cluster-manager has not yet been discovered.
    *
    * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #setClusterManagerNodeTimeout(TimeValue)}
    */
    @SuppressWarnings("unchecked")
    @Deprecated
    public final RequestBuilder setMasterNodeTimeout(TimeValue timeout) {
        return setClusterManagerNodeTimeout(timeout);
    }

    /**
     * Sets the cluster-manager node timeout in case the cluster-manager has not yet been discovered.
    */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setClusterManagerNodeTimeout(String timeout) {
        request.clusterManagerNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Sets the cluster-manager node timeout in case the cluster-manager has not yet been discovered.
    *
    * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #setClusterManagerNodeTimeout(String)}
    */
    @SuppressWarnings("unchecked")
    @Deprecated
    public final RequestBuilder setMasterNodeTimeout(String timeout) {
        return setClusterManagerNodeTimeout(timeout);
    }
}
