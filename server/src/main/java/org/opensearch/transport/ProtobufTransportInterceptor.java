/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.ProtobufWriteable.Reader;

/**
 * This interface allows plugins to intercept requests on both the sender and the receiver side.
*
* @opensearch.internal
*/
public interface ProtobufTransportInterceptor {
    /**
     * This is called for each handler that is registered via
    * {@link ProtobufTransportService#registerRequestHandler(String, String, boolean, boolean, Reader, ProtobufTransportRequestHandler)} or
    * {@link ProtobufTransportService#registerRequestHandler(String, String, Reader, ProtobufTransportRequestHandler)}. The returned handler is
    * used instead of the passed in handler. By default the provided handler is returned.
    */
    default <T extends ProtobufTransportRequest> ProtobufTransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        ProtobufTransportRequestHandler<T> actualHandler
    ) {
        return actualHandler;
    }

    /**
     * This is called up-front providing the actual low level {@link AsyncSender} that performs the low level send request.
    * The returned sender is used to send all requests that come in via
    * {@link ProtobufTransportService#sendRequest(DiscoveryNode, String, ProtobufTransportRequest, ProtobufTransportResponseHandler)} or
    * {@link ProtobufTransportService#sendRequest(DiscoveryNode, String, ProtobufTransportRequest, TransportRequestOptions, ProtobufTransportResponseHandler)}.
    * This allows plugins to perform actions on each send request including modifying the request context etc.
    */
    default AsyncSender interceptSender(AsyncSender sender) {
        return sender;
    }

    /**
     * A simple interface to decorate
    * {@link #sendRequest(Transport.ProtobufConnection, String, ProtobufTransportRequest, TransportRequestOptions, ProtobufTransportResponseHandler)}
    */
    interface AsyncSender {
        <T extends ProtobufTransportResponse> void sendRequest(
            Transport.ProtobufConnection connection,
            String action,
            ProtobufTransportRequest request,
            TransportRequestOptions options,
            ProtobufTransportResponseHandler<T> handler
        );
    }
}
