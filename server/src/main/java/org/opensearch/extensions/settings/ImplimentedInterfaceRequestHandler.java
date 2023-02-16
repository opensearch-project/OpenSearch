package org.opensearch.extensions.settings;

import java.util.Map;
import java.util.logging.Logger;

import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.opensearch.extensions.AcknowledgedResponse;

/**
 * Handles requests to add list of interfaces which are implimented by an extension
 *
 * @opensearch.internal
 */
public class ImplimentedInterfaceRequestHandler {
    private final Map<String, DiscoveryExtensionNode> extensionIdMap;
    private final TransportService transportService;


    /**
     * Instantiates a new Implimented Interface Request Handler.
     * @param extensionIdMap  A map of extension uniqueId to DiscoveryExtensionNode
     * @param transportService  The Node's transportService
     */
    public ImplimentedInterfaceRequestHandler(Map<String, DiscoveryExtensionNode> extensionIdMap, TransportService transportService){
        this.extensionIdMap = extensionIdMap;
        this.transportService = transportService;
    }


    /**
     * Handles a {@link RegisterImplimentedInterfaceRequest}.
     *
     * @param implimentedInterfaceRequest  The request to handle.
     * @return A {@link AcknowledgedResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    public TransportResponse handleImplimentedInterfaceRequest(RegisterImplimentedInterfaceRequest implimentedInterfaceRequest) throws Exception {
        DiscoveryExtensionNode discoveryExtensionNode = extensionIdMap.get(implimentedInterfaceRequest.getUniqueId());
        discoveryExtensionNode.setImplimentedInterfaces(implimentedInterfaceRequest.getImplimentedInterfaces());
        System.out.println("interfaces List Set on DiscoveryExtNode : "+discoveryExtensionNode.getImplimentedInterfaces());
        return new AcknowledgedResponse(true);
    }
}
