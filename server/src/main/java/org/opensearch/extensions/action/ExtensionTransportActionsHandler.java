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
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.extensions.AcknowledgedResponse;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.RegisterTransportActionsRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class manages TransportActions for extensions
 *
 * @opensearch.internal
 */
public class ExtensionTransportActionsHandler {
    private static final Logger logger = LogManager.getLogger(ExtensionTransportActionsHandler.class);
    private Map<String, DiscoveryExtensionNode> actionsMap;
    private final Map<String, DiscoveryExtensionNode> extensionIdMap;
    private final TransportService transportService;
    private final NodeClient client;

    public ExtensionTransportActionsHandler(
        Map<String, DiscoveryExtensionNode> extensionIdMap,
        TransportService transportService,
        NodeClient client
    ) {
        this.actionsMap = new HashMap<>();
        this.extensionIdMap = extensionIdMap;
        this.transportService = transportService;
        this.client = client;
    }

    /**
     * Method to register actions for extensions.
     *
     * @param action to be registered.
     * @param extension for which action is being registered.
     * @throws IllegalArgumentException when action being registered already is registered.
     */
    void registerAction(String action, DiscoveryExtensionNode extension) throws IllegalArgumentException {
        if (actionsMap.containsKey(action)) {
            throw new IllegalArgumentException("The " + action + " you are trying to register is already registered");
        }
        actionsMap.putIfAbsent(action, extension);
    }

    /**
     * Method to get extension for a given action.
     *
     * @param action for which to get the registered extension.
     * @return the extension.
     */
    public DiscoveryExtensionNode getExtension(String action) {
        return actionsMap.get(action);
    }

    /**
     * Handles a {@link RegisterTransportActionsRequest}.
     *
     * @param transportActionsRequest  The request to handle.
     * @return  A {@link AcknowledgedResponse} indicating success.
     */
    public TransportResponse handleRegisterTransportActionsRequest(RegisterTransportActionsRequest transportActionsRequest) {
        /*
         * We are proxying the transport Actions through ExtensionProxyAction, so we really dont need to register dynamic actions for now.
         */
        logger.debug("Register Transport Actions request recieved {}", transportActionsRequest);
        DiscoveryExtensionNode extension = extensionIdMap.get(transportActionsRequest.getUniqueId());
        try {
            for (String action : transportActionsRequest.getTransportActions()) {
                registerAction(action, extension);
            }
        } catch (Exception e) {
            logger.error("Could not register Transport Action " + e);
            return new AcknowledgedResponse(false);
        }
        return new AcknowledgedResponse(true);
    }

    /**
     * Method which handles transport action request from an extension.
     *
     * @param request from extension.
     * @return {@link TransportResponse} which is sent back to the transport action invoker.
     * @throws InterruptedException when message transport fails.
     */
    public TransportResponse handleTransportActionRequestFromExtension(TransportActionRequestFromExtension request) throws Exception {
        DiscoveryExtensionNode extension = extensionIdMap.get(request.getUniqueId());
        final CompletableFuture<ExtensionActionResponse> inProgressFuture = new CompletableFuture<>();
        final TransportActionResponseToExtension response = new TransportActionResponseToExtension(new byte[0]);
        client.execute(
            ExtensionProxyAction.INSTANCE,
            new ExtensionActionRequest(request.getAction(), request.getRequestBytes()),
            new ActionListener<ExtensionActionResponse>() {
                @Override
                public void onResponse(ExtensionActionResponse actionResponse) {
                    response.setResponseBytes(actionResponse.getResponseBytes());
                    inProgressFuture.complete(actionResponse);
                }

                @Override
                public void onFailure(Exception exp) {
                    logger.debug("Transport request failed", exp);
                    byte[] responseBytes = ("Request failed: " + exp.getMessage()).getBytes(StandardCharsets.UTF_8);
                    response.setResponseBytes(responseBytes);
                    inProgressFuture.completeExceptionally(exp);
                }
            }
        );
        try {
            inProgressFuture.orTimeout(ExtensionsManager.EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.info("No response from extension to request.");
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
        return response;
    }

    /**
     * Method to send transport action request to an extension to handle.
     *
     * @param request to extension to handle transport request.
     * @return {@link ExtensionActionResponse} which encapsulates the transport response from the extension.
     * @throws InterruptedException when message transport fails.
     */
    public ExtensionActionResponse sendTransportRequestToExtension(ExtensionActionRequest request) throws Exception {
        DiscoveryExtensionNode extension = actionsMap.get(request.getAction());
        if (extension == null) {
            throw new ActionNotFoundTransportException(request.getAction());
        }
        final CompletableFuture<ExtensionActionResponse> inProgressFuture = new CompletableFuture<>();
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
                    inProgressFuture.complete(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug("Transport request failed", exp);
                    byte[] responseBytes = ("Request failed: " + exp.getMessage()).getBytes(StandardCharsets.UTF_8);
                    extensionActionResponse.setResponseBytes(responseBytes);
                    inProgressFuture.completeExceptionally(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            };
        try {
            transportService.sendRequest(
                extension,
                ExtensionsManager.REQUEST_EXTENSION_HANDLE_TRANSPORT_ACTION,
                new ExtensionHandleTransportRequest(request.getAction(), request.getRequestBytes()),
                extensionActionResponseTransportResponseHandler
            );
        } catch (Exception e) {
            logger.info("Failed to send transport action to extension " + extension.getName(), e);
        }
        try {
            inProgressFuture.orTimeout(ExtensionsManager.EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                logger.info("No response from extension to request.");
            }
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
        return extensionActionResponse;
    }
}
