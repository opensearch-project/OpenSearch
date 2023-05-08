/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.clustermanager;

import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.ProtobufOpenSearchClient;

/**
 * Base request builder for cluster-manager node read operations that can be executed on the local node as well
*
* @opensearch.internal
*/
public abstract class ProtobufClusterManagerNodeReadOperationRequestBuilder<
    Request extends ProtobufClusterManagerNodeReadRequest<Request>,
    Response extends ProtobufActionResponse,
    RequestBuilder extends ProtobufClusterManagerNodeReadOperationRequestBuilder<Request, Response, RequestBuilder>> extends
    ProtobufClusterManagerNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected ProtobufClusterManagerNodeReadOperationRequestBuilder(ProtobufOpenSearchClient client, ProtobufActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Specifies if the request should be executed on local node rather than on master
    */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setLocal(boolean local) {
        request.local(local);
        return (RequestBuilder) this;
    }
}
