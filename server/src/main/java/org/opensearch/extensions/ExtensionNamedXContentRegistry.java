/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.ParseField;
import org.opensearch.extensions.ExtensionsOrchestrator.OpenSearchRequestType;
import org.opensearch.transport.TransportService;

/**
 * API for Named Xcontents for extensions
 *
 * @opensearch.internal
 */
public class ExtensionNamedXContentRegistry {

    private static final Logger logger = LogManager.getLogger(ExtensionNamedXContentRegistry.class);

    private Map<DiscoveryNode, Map<Class, Map<String, Map<ParseField, ExtensionReader>>>> extensionNamedXContentRegistry;
    private List<DiscoveryExtension> extensionsInitializedList;
    private TransportService transportService;

    /**
     * Initializes a new ExtensionXContentRegistry
     *
     * @param extensionsInitializedList List of DiscoveryExtensions to send requests to
     * @param transportService Service that facilitates transport requests
     */
    public ExtensionNamedXContentRegistry(List<DiscoveryExtension> extensionsInitializedList, TransportService transportService) {
        this.extensionsInitializedList = extensionsInitializedList;
        this.extensionNamedXContentRegistry = new HashMap<>();
        this.transportService = transportService;

        getNamedXContents();
    }

    /**
     * Iterates through all discovered extensions, sends transport requests for named xcontent and consolidates all entires into a central named xcontent registry for extensions.
     */
    public void getNamedXContents() {

        // Retrieve named xcontent registry entries from each extension
        for (DiscoveryNode extensionNode : extensionsInitializedList) {
            try {
                Map<DiscoveryNode, Map<Class, Map<String, Map<ParseField, ExtensionReader>>>> extensionRegistry = getNamedXContents(
                    extensionNode
                );
                if (extensionRegistry.isEmpty() == false) {
                    this.extensionNamedXContentRegistry.putAll(extensionRegistry);
                }
            } catch (UnknownHostException e) {
                logger.error(e.toString());
            }
        }
    }

    /**
     * Sends a transport request for named xcontent to an extension, identified by the given DiscoveryNode, and processes the response into registry entries
     *
     * @param extensionNode DiscoveryNode identifying the extension
     * @throws UnknownHostException if connection to the extension node failed
     * @return A map of category classes and their associated ParseFields and readers for this discovery node
     */
    private Map<DiscoveryNode, Map<Class, Map<String, Map<ParseField, ExtensionReader>>>> getNamedXContents(DiscoveryNode extensionNode)
        throws UnknownHostException {
        NamedXContentRegistryResponseHandler namedXContentRegistryResponseHandler = new NamedXContentRegistryResponseHandler(
            extensionNode,
            transportService,
            ExtensionsOrchestrator.REQUEST_OPENSEARCH_NAMED_XCONTENT_REGISTRY
        );
        try {
            logger.info("Sending extension request type: " + ExtensionsOrchestrator.REQUEST_OPENSEARCH_NAMED_XCONTENT_REGISTRY);
            transportService.sendRequest(
                extensionNode,
                ExtensionsOrchestrator.REQUEST_OPENSEARCH_NAMED_XCONTENT_REGISTRY,
                new OpenSearchRequest(OpenSearchRequestType.REQUEST_OPENSEARCH_NAMED_XCONTENT_REGISTRY),
                namedXContentRegistryResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }

        return namedXContentRegistryResponseHandler.getExtensionRegistry();
    }

}
