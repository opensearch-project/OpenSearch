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
import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.extensions.ExtensionsManager.OpenSearchRequestType;
import org.opensearch.transport.TransportService;

/**
 * API for Named Writeables for extensions
 *
 * @opensearch.internal
 */
public class ExtensionNamedWriteableRegistry {

    private static final Logger logger = LogManager.getLogger(ExtensionNamedWriteableRegistry.class);

    private Map<DiscoveryNode, Map<Class<? extends NamedWriteable>, Map<String, ExtensionReader>>> extensionNamedWriteableRegistry;
    private List<DiscoveryExtensionNode> extensionsInitializedList;
    private TransportService transportService;

    /**
     * Initializes a new ExtensionNamedWriteableRegistry
     *
     * @param extensionsInitializedList List of DiscoveryExtensionNodes to send requests to
     * @param transportService Service that facilitates transport requests
     */
    public ExtensionNamedWriteableRegistry(List<DiscoveryExtensionNode> extensionsInitializedList, TransportService transportService) {
        this.extensionsInitializedList = extensionsInitializedList;
        this.extensionNamedWriteableRegistry = new HashMap<>();
        this.transportService = transportService;

        getNamedWriteables();
    }

    /**
     * Iterates through all discovered extensions, sends transport requests for named writeables and consolidates all entires into a central named writeable registry for extensions.
     */
    public void getNamedWriteables() {
        // Retrieve named writeable registry entries from each extension
        for (DiscoveryNode extensionNode : extensionsInitializedList) {
            try {
                Map<DiscoveryNode, Map<Class<? extends NamedWriteable>, Map<String, ExtensionReader>>> extensionRegistry =
                    getNamedWriteables(extensionNode);
                if (extensionRegistry.isEmpty() == false) {
                    this.extensionNamedWriteableRegistry.putAll(extensionRegistry);
                }
            } catch (UnknownHostException e) {
                logger.error(e.toString());
            }
        }

        // TODO : Invoke during the consolidation of named writeables within Node.java and return extension entries there
        // (https://github.com/opensearch-project/OpenSearch/issues/4067)
    }

    /**
     * Sends a transport request for named writeables to an extension, identified by the given DiscoveryNode, and processes the response into registry entries
     *
     * @param extensionNode DiscoveryNode identifying the extension
     * @throws UnknownHostException if connection to the extension node failed
     * @return A map of category classes and their associated names and readers for this discovery node
     */
    private Map<DiscoveryNode, Map<Class<? extends NamedWriteable>, Map<String, ExtensionReader>>> getNamedWriteables(
        DiscoveryNode extensionNode
    ) throws UnknownHostException {
        NamedWriteableRegistryResponseHandler namedWriteableRegistryResponseHandler = new NamedWriteableRegistryResponseHandler(
            extensionNode,
            transportService,
            ExtensionsManager.REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE
        );
        try {
            logger.info("Sending extension request type: " + ExtensionsManager.REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY);
            transportService.sendRequest(
                extensionNode,
                ExtensionsManager.REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY,
                new OpenSearchRequest(OpenSearchRequestType.REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY),
                namedWriteableRegistryResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }

        return namedWriteableRegistryResponseHandler.getExtensionRegistry();
    }

    /**
     * Iterates through list of discovered extensions and returns the callback method associated with the given category class and name
     *
     * @param categoryClass Class that the Writeable object extends
     * @param name Unique name identifiying the Writeable object
     * @throws IllegalArgumentException if there is no reader associated with the given category class and name
     * @return A map of the discovery node and its associated extension reader
     */
    public Map<DiscoveryNode, ExtensionReader> getExtensionReader(Class<? extends NamedWriteable> categoryClass, String name) {

        ExtensionReader reader = null;
        DiscoveryNode extension = null;

        // The specific extension that the reader is associated with is not known, must iterate through all of them
        for (DiscoveryNode extensionNode : extensionsInitializedList) {
            reader = getExtensionReader(extensionNode, categoryClass, name);
            if (reader != null) {
                extension = extensionNode;
                break;
            }
        }

        // At this point, if reader does not exist throughout all extensionNodes, named writeable is not registered, throw exception
        if (reader == null) {
            throw new IllegalArgumentException("Unknown NamedWriteable [" + categoryClass.getName() + "][" + name + "]");
        }
        return Collections.singletonMap(extension, reader);
    }

    /**
     * Returns the callback method associated with the given extension node, category class and name
     *
     * @param extensionNode Discovery Node identifying the extension associated with the category class and name
     * @param categoryClass Class that the Writeable object extends
     * @param name Unique name identifying the Writeable object
     * @return The extension reader
     */
    private ExtensionReader getExtensionReader(DiscoveryNode extensionNode, Class<? extends NamedWriteable> categoryClass, String name) {
        ExtensionReader reader = null;
        Map<Class<? extends NamedWriteable>, Map<String, ExtensionReader>> categoryMap = this.extensionNamedWriteableRegistry.get(
            extensionNode
        );
        if (categoryMap != null) {
            Map<String, ExtensionReader> readerMap = categoryMap.get(categoryClass);
            if (readerMap != null) {
                reader = readerMap.get(name);
            }
        }
        return reader;
    }

}
