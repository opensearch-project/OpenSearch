/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.rest;

/**
 * Handler for REST requests
*
* @opensearch.api
*/
@FunctionalInterface
public interface ClientAgnosticRestHandler<T> {

    /**
     * Handles a rest request.
    * @param request The request to handle
    * @param channel The channel to write the request response to
    * @param client A client to use to make internal requests on behalf of the original request
    */
    void handleRequest(RestRequest request, RestChannel channel, T client) throws Exception;

}
