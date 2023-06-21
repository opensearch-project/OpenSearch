/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.client;

import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

/**
 * Interface for an OpenSearch client implementation
*
* @opensearch.internal
*/
public interface ProtobufOpenSearchClient {

    /**
     * Executes a generic action, denoted by an {@link ProtobufActionType}.
    *
    * @param action           The action type to execute.
    * @param request          The action request.
    * @param <Request>        The request type.
    * @param <Response>       the response type.
    * @return A future allowing to get back the response.
    */
    <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> ActionFuture<Response> execute(
        ProtobufActionType<Response> action,
        Request request
    );

    /**
     * Executes a generic action, denoted by an {@link ProtobufActionType}.
    *
    * @param action           The action type to execute.
    * @param request          The action request.
    * @param listener         The listener to receive the response back.
    * @param <Request>        The request type.
    * @param <Response>       The response type.
    */
    <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void execute(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    /**
     * Returns the threadpool used to execute requests on this client
    */
    ThreadPool threadPool();

}
