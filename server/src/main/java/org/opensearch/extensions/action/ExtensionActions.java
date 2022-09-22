/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.extensions.ExtensionBooleanResponse;
import org.opensearch.extensions.ExtensionsOrchestrator;
import org.opensearch.extensions.RegisterTransportActionsRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * This class manages TransportActions for extensions
 *
 * @opensearch.internal
 */
public class ExtensionActions {
    private static final Logger logger = LogManager.getLogger(ExtensionActions.class);
    private Map<String, DiscoveryExtension> actionsMap;
    private final Map<String, DiscoveryExtension> extensionIdMap;
    private final TransportService transportService;
    private final NodeClient client;

    public ExtensionActions(Map<String, DiscoveryExtension> extensionIdMap, TransportService transportService, NodeClient client) {
        this.actionsMap = new HashMap<>();
        this.extensionIdMap = extensionIdMap;
        this.transportService = transportService;
        this.client = client;
    }

    void registerAction(String action, DiscoveryExtension extension) throws IllegalArgumentException {
        if (actionsMap.containsKey(action)) {
            throw new IllegalArgumentException("The " + action + " you are trying to register is already registered");
        }
        actionsMap.putIfAbsent(action, extension);
    }

    public DiscoveryExtension getExtension(String action) {
        return actionsMap.get(action);
    }

    /**
     * Handles a {@link RegisterTransportActionsRequest}.
     *
     * @param transportActionsRequest  The request to handle.
     * @return  A {@link ExtensionBooleanResponse} indicating success.
     */
    public TransportResponse handleRegisterTransportActionsRequest(RegisterTransportActionsRequest transportActionsRequest) {
        /*
         * We are proxying the transport Actions through ExtensionMainAction, so we really dont need to register dynamic actions for now.
         */
        logger.debug("Register Transport Actions request recieved ", transportActionsRequest);
        DiscoveryExtension extension = extensionIdMap.get(transportActionsRequest.getUniqueId());
        for (String action : transportActionsRequest.getTransportActions().keySet()) {
            registerAction(action, extension);
        }
        return new ExtensionBooleanResponse(true);
    }

    public TransportResponse handleTransportActionRequestFromExtension(TransportActionRequestFromExtension request) {
        DiscoveryExtension extension = extensionIdMap.get(request.getUniqueId());
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final TransportActionResponseToExtension response = new TransportActionResponseToExtension(new byte[0]);
        client.execute(
            ExtensionMainAction.INSTANCE,
            new ExtensionActionRequest(request.getAction(), request.getRequestBytes()),
            new ActionListener<ExtensionActionResponse>() {
                @Override
                public void onResponse(ExtensionActionResponse actionResponse) {
                    response.setResponseBytes(actionResponse.getResponseBytes());
                    inProgressLatch.countDown();
                }

                @Override
                public void onFailure(Exception exp) {
                    logger.debug("Transport request failed", exp);
                    byte[] responseBytes = ("Request failed: " + exp.getMessage()).getBytes(StandardCharsets.UTF_8);
                    response.setResponseBytes(responseBytes);
                    inProgressLatch.countDown();
                }
            }
        );
        return response;
    }

    public ExtensionActionResponse sendTransportRequestToExtension(ExtensionActionRequest request) {
        DiscoveryExtension extension = actionsMap.get(request.getAction());
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final ExtensionActionResponse extensionActionResponse = new ExtensionActionResponse(new byte[0]);
        final TransportResponseHandler<ExtensionActionResponse> extensionActionResponseTransportResponseHandler =
            new TransportResponseHandler<ExtensionActionResponse>() {

                @Override
                public ExtensionActionResponse read(StreamInput in) throws IOException {
                    return new ExtensionActionResponse(in);
                }

                @Override
                public void handleResponse(ExtensionActionResponse response) {
                    extensionActionResponse.setResponseBytes(response.getResponseBytes());
                    inProgressLatch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug("Transport request failed", exp);
                    byte[] responseBytes = ("Request failed: " + exp.getMessage()).getBytes(StandardCharsets.UTF_8);
                    extensionActionResponse.setResponseBytes(responseBytes);
                    inProgressLatch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            };
        try {
            transportService.sendRequest(
                extension,
                ExtensionsOrchestrator.REQUEST_EXTENSION_HANDLE_TRANSPORT_ACTION,
                new ExtensionHandleTransportRequest(request.getAction(), request.getRequestBytes()),
                extensionActionResponseTransportResponseHandler
            );
        } catch (Exception e) {
            logger.info("Failed to send transport action to extension " + extension.getName(), e);
        }
        return extensionActionResponse;
    }
}
