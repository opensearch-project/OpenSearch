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
import org.opensearch.action.ActionModule;
import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.extensions.AcknowledgedResponse;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class manages TransportActions for extensions
 *
 * @opensearch.internal
 */
public class ExtensionTransportActionsHandler {
    private static final Logger logger = LogManager.getLogger(ExtensionTransportActionsHandler.class);
    // Map of action name to Extension unique ID, populated locally
    private final Map<String, String> actionToIdMap = new ConcurrentHashMap<>();
    // Map of Extension unique ID to Extension Node, populated in Extensions Manager
    private final Map<String, DiscoveryExtensionNode> extensionIdMap;
    private final TransportService transportService;
    private final NodeClient client;
    private final ActionFilters actionFilters;
    private final DynamicActionRegistry dynamicActionRegistry;
    private final ExtensionsManager extensionsManager;

    public ExtensionTransportActionsHandler(
        Map<String, DiscoveryExtensionNode> extensionIdMap,
        TransportService transportService,
        NodeClient client,
        ActionModule actionModule,
        ExtensionsManager extensionsManager
    ) {
        this.extensionIdMap = extensionIdMap;
        this.transportService = transportService;
        this.client = client;
        this.actionFilters = actionModule.getActionFilters();
        this.dynamicActionRegistry = actionModule.getDynamicActionRegistry();
        this.extensionsManager = extensionsManager;
    }

    /**
     * Method to register actions for extensions.
     *
     * @param action to be registered.
     * @param uniqueId id of extension for which action is being registered.
     * @throws IllegalArgumentException when action being registered already is registered.
     */
    void registerAction(String action, String uniqueId) throws IllegalArgumentException {
        // Register the action in this handler so it knows which extension owns it
        if (actionToIdMap.putIfAbsent(action, uniqueId) != null) {
            throw new IllegalArgumentException("The action [" + action + "] you are trying to register is already registered");
        }
        // Register the action in the action module's dynamic actions map
        dynamicActionRegistry.registerDynamicAction(
            new ExtensionAction(uniqueId, action),
            new ExtensionTransportAction(action, actionFilters, transportService.getTaskManager(), extensionsManager)
        );
    }

    /**
     * Method to get extension for a given action.
     *
     * @param action for which to get the registered extension.
     * @return the extension or null if not found
     */
    public DiscoveryExtensionNode getExtension(String action) {
        String uniqueId = actionToIdMap.get(action);
        if (uniqueId == null) {
            throw new ActionNotFoundTransportException(action);
        }
        return extensionIdMap.get(uniqueId);
    }

    /**
     * Handles a {@link RegisterTransportActionsRequest}.
     *
     * @param transportActionsRequest  The request to handle.
     * @return  A {@link AcknowledgedResponse} indicating success.
     */
    public TransportResponse handleRegisterTransportActionsRequest(RegisterTransportActionsRequest transportActionsRequest) {
        try {
            for (String action : transportActionsRequest.getTransportActions()) {
                registerAction(action, transportActionsRequest.getUniqueId());
            }
        } catch (Exception e) {
            logger.error("Could not register Transport Action: " + e.getMessage());
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
    public RemoteExtensionActionResponse handleTransportActionRequestFromExtension(TransportActionRequestFromExtension request)
        throws Exception {
        String actionName = request.getAction();
        String uniqueId = actionToIdMap.get(actionName);
        final RemoteExtensionActionResponse response = new RemoteExtensionActionResponse(false, new byte[0]);
        // Fail fast if uniqueId is null
        if (uniqueId == null) {
            response.setResponseBytesAsString("Request failed: action [" + actionName + "] is not registered for any extension.");
            return response;
        }
        ExtensionAction extensionAction = new ExtensionAction(uniqueId, actionName);
        // Validate that this action has been registered
        if (dynamicActionRegistry.get(extensionAction) == null) {
            response.setResponseBytesAsString(
                "Request failed: action [" + actionName + "] is not registered for extension [" + uniqueId + "]."
            );
            return response;
        }
        DiscoveryExtensionNode extension = extensionIdMap.get(uniqueId);
        if (extension == null) {
            response.setResponseBytesAsString("Request failed: extension [" + uniqueId + "] can not be reached.");
            return response;
        }
        final CompletableFuture<RemoteExtensionActionResponse> inProgressFuture = new CompletableFuture<>();
        client.execute(
            extensionAction,
            new ExtensionActionRequest(request.getAction(), request.getRequestBytes()),
            new ActionListener<RemoteExtensionActionResponse>() {
                @Override
                public void onResponse(RemoteExtensionActionResponse actionResponse) {
                    response.setSuccess(actionResponse.isSuccess());
                    response.setResponseBytes(actionResponse.getResponseBytes());
                    inProgressFuture.complete(actionResponse);
                }

                @Override
                public void onFailure(Exception exp) {
                    logger.debug("Transport request failed", exp);
                    response.setResponseBytesAsString("Request failed: " + exp.getMessage());
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
        DiscoveryExtensionNode extension = getExtension(request.getAction());
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

    /**
     * Method to send transport action request from a remote extension to another extension to handle.
     *
     * @param request to extension to handle transport request.
     * @return {@link RemoteExtensionActionResponse} which encapsulates the transport response from the extension and its success.
     */
    public RemoteExtensionActionResponse sendRemoteTransportRequestToExtension(ExtensionActionRequest request) {
        DiscoveryExtensionNode extension = getExtension(request.getAction());
        final CompletableFuture<RemoteExtensionActionResponse> inProgressFuture = new CompletableFuture<>();
        final RemoteExtensionActionResponse extensionActionResponse = new RemoteExtensionActionResponse(false, new byte[0]);
        final TransportResponseHandler<RemoteExtensionActionResponse> extensionActionResponseTransportResponseHandler =
            new TransportResponseHandler<RemoteExtensionActionResponse>() {

                @Override
                public RemoteExtensionActionResponse read(StreamInput in) throws IOException {
                    return new RemoteExtensionActionResponse(in);
                }

                @Override
                public void handleResponse(RemoteExtensionActionResponse response) {
                    extensionActionResponse.setSuccess(response.isSuccess());
                    extensionActionResponse.setResponseBytes(response.getResponseBytes());
                    inProgressFuture.complete(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug("Transport request failed", exp);
                    extensionActionResponse.setResponseBytesAsString("Request failed: " + exp.getMessage());
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
                ExtensionsManager.REQUEST_EXTENSION_HANDLE_REMOTE_TRANSPORT_ACTION,
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
