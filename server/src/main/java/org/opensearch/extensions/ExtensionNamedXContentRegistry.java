/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.net.UnknownHostException;
import java.util.Collections;
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

        // TODO : Invoke during the consolidation of named xcontent within Node.java and return extension entries there
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

    /**
     * Iterates through list of discovered extensions and returns the callback method associated with the given category class, parsefield and name
     *
     * @param categoryClass Class that the XContent object extends
     * @param parseField ParseField associated with the XContent Reader
     * @param name Unique name identifiying the XContent object
     * @throws IllegalArgumentException if there is no reader associated with the given category class and name
     * @return A map of the discovery node and its associated extension reader
     */
    public Map<DiscoveryNode, ExtensionReader> getExtensionReader(Class categoryClass, ParseField parseField, String name) {

        ExtensionReader reader = null;
        DiscoveryNode extension = null;

        // The specific extension that the reader is associated with is not known, must iterate through all of them
        for (DiscoveryNode extensionNode : extensionsInitializedList) {
            reader = getExtensionReader(extensionNode, categoryClass, parseField, name);
            if (reader != null) {
                extension = extensionNode;
                break;
            }
        }

        // At this point, if reader does not exist throughout all extensionNodes, named xcontent is not registered, throw exception
        if (reader == null) {
            throw new IllegalArgumentException(
                "Unknown NamedXContent [" + categoryClass.getName() + "][" + parseField.toString() + "][" + name + "]"
            );
        }
        return Collections.singletonMap(extension, reader);
    }

    /**
     * Returns the callback method associated with the given extension node, category class, parseField and name
     *
     * @param extensionNode Discovery Node identifying the extension associated with the category class and name
     * @param categoryClass Class that the Writeable object extends
     * @param parseField ParseField associated with the XContent Reader
     * @param name Unique name identifying the Writeable object
     * @return The extension reader
     */
    private ExtensionReader getExtensionReader(DiscoveryNode extensionNode, Class categoryClass, ParseField parseField, String name) {
        ExtensionReader reader = null;

        Map<Class, Map<String, Map<ParseField, ExtensionReader>>> categoryMap = this.extensionNamedXContentRegistry.get(extensionNode);
        if (categoryMap != null) {

            Map<String, Map<ParseField, ExtensionReader>> nameMap = categoryMap.get(categoryClass);
            if (nameMap != null) {
                Map<ParseField, ExtensionReader> readerMap = nameMap.get(name);
                if (readerMap != null) {
                    reader = readerMap.get(parseField);
                }
            }
        }
        return reader;
    }

}
