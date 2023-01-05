/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.extensions.AcknowledgedResponse;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Handles requests to register extension REST actions.
 *
 * @opensearch.internal
 */
public class RestActionsRequestHandler {

    private final RestController restController;
    private final Map<String, DiscoveryExtensionNode> extensionIdMap;
    private final TransportService transportService;

    /**
     * Instantiates a new REST Actions Request Handler using the Node's RestController.
     *
     * @param restController  The Node's {@link RestController}.
     * @param extensionIdMap  A map of extension uniqueId to DiscoveryExtensionNode
     * @param transportService  The Node's transportService
     */
    public RestActionsRequestHandler(
        RestController restController,
        Map<String, DiscoveryExtensionNode> extensionIdMap,
        TransportService transportService
    ) {
        this.restController = restController;
        this.extensionIdMap = extensionIdMap;
        this.transportService = transportService;
    }

    /**
     * Handles a {@link RegisterRestActionsRequest}.
     *
     * @param restActionsRequest  The request to handle.
     * @return A {@link AcknowledgedResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    public TransportResponse handleRegisterRestActionsRequest(RegisterRestActionsRequest restActionsRequest) throws Exception {
        DiscoveryExtensionNode discoveryExtensionNode = extensionIdMap.get(restActionsRequest.getUniqueId());
        RestHandler handler = new RestSendToExtensionAction(restActionsRequest, discoveryExtensionNode, transportService);
        restController.registerHandler(handler);
        return new AcknowledgedResponse(true);
    }
}
