/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * An action that forwards REST requests to an extension
 */
public class RestSendToExtensionAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestSendToExtensionAction.class);

    private final List<Route> routes;
    private final DiscoveryExtension discoveryExtension;
    private final TransportService transportService;

    /**
     * Instantiates this object using a {@link RegisterRestActionsRequest} to populate the routes.
     *
     * @param restActionsRequest A request encapsulating a list of Strings with the API methods and URIs.
     * @param transportService The OpenSearch transport service
     * @param discoveryExtension The extension node to which to send actions
     */
    public RestSendToExtensionAction(RegisterRestActionsRequest restActionsRequest, DiscoveryExtension discoveryExtension, TransportService transportService) {
        List<Route> restActionsAsRoutes = new ArrayList<>();
        for (String restAction : restActionsRequest.getRestActions()) {
            RestRequest.Method method;
            String uri;
            try {
                int delim = restAction.indexOf(' ');
                method = RestRequest.Method.valueOf(restAction.substring(0, delim));
                uri = "/_extensions/_" + restActionsRequest.getUniqueId() + restAction.substring(delim).trim();
            } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                throw new IllegalArgumentException(restAction + " does not begin with a valid REST method");
            }
            logger.info("Registering: " + method + " " + uri);
            restActionsAsRoutes.add(new Route(method, uri));
        }
        this.routes = unmodifiableList(restActionsAsRoutes);
        this.discoveryExtension = discoveryExtension;
        this.transportService = transportService;
    }

    @Override
    public String getName() {
        return "send_to_extension_action";
    }

    @Override
    public List<Route> routes() {
        return this.routes;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String message ="Forwarding the request " + request.getHttpRequest().method() + " " + request.getHttpRequest().uri() + " to " + discoveryExtension; 
        logger.info(message);
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.ACCEPTED, message));
    }
}
