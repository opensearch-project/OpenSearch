/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.nodes;

import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.ProtobufActionRequestBuilder;
import org.opensearch.client.ProtobufOpenSearchClient;
import org.opensearch.common.unit.TimeValue;

/**
 * Builder for Operation Requests
*
* @opensearch.internal
*/
public abstract class ProtobufNodesOperationRequestBuilder<
    Request extends ProtobufBaseNodesRequest<Request>,
    Response extends ProtobufBaseNodesResponse,
    RequestBuilder extends ProtobufNodesOperationRequestBuilder<Request, Response, RequestBuilder>> extends ProtobufActionRequestBuilder<
        Request,
        Response> {

    protected ProtobufNodesOperationRequestBuilder(ProtobufOpenSearchClient client, ProtobufActionType<Response> action, Request request) {
        super(client, action, request);
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setNodesIds(String... nodesIds) {
        request.nodesIds(nodesIds);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return (RequestBuilder) this;
    }
}
