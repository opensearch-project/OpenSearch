/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.cluster.node.ProtobufDiscoveryNode;

/**
 * Listens for transport messages
*
* @opensearch.internal
*/
public interface ProtobufTransportMessageListener {

    ProtobufTransportMessageListener NOOP_LISTENER = new ProtobufTransportMessageListener() {
    };

    /**
     * Called once a request is received
    * @param requestId the internal request ID
    * @param action the request action
    *
    */
    default void onRequestReceived(long requestId, String action) {}

    /**
     * Called for every action response sent after the response has been passed to the underlying network implementation.
    * @param requestId the request ID (unique per client)
    * @param action the request action
    * @param response the response send
    */
    default void onResponseSent(long requestId, String action, ProtobufTransportResponse response) {}

    /***
     * Called for every failed action response after the response has been passed to the underlying network implementation.
    * @param requestId the request ID (unique per client)
    * @param action the request action
    * @param error the error sent back to the caller
    */
    default void onResponseSent(long requestId, String action, Exception error) {}

    /**
     * Called for every request sent to a server after the request has been passed to the underlying network implementation
    * @param node the node the request was sent to
    * @param requestId the internal request id
    * @param action the action name
    * @param request the actual request
    * @param finalOptions the request options
    */
    default void onRequestSent(
        ProtobufDiscoveryNode node,
        long requestId,
        String action,
        ProtobufTransportRequest request,
        TransportRequestOptions finalOptions
    ) {}

    /**
     * Called for every response received
    * @param requestId the request id for this reponse
    * @param context the response context or null if the context was already processed ie. due to a timeout.
    */
    default void onResponseReceived(long requestId, ProtobufTransport.ResponseContext context) {}
}
