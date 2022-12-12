/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.NamedWriteableRegistryParseRequest;
import org.opensearch.common.io.stream.NamedWriteableRegistryResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Response handler for NamedWriteableRegistry Requests
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryResponseHandler implements TransportResponseHandler<NamedWriteableRegistryResponse> {
    private static final Logger logger = LogManager.getLogger(NamedWriteableRegistryResponseHandler.class);

    private final Map<DiscoveryNode, Map<Class, Map<String, ExtensionReader>>> extensionRegistry;
    private final DiscoveryNode extensionNode;
    private final TransportService transportService;
    private final String requestType;

    /**
    * Instantiates a new NamedWriteableRegistry response handler
    *
    * @param extensionNode Discovery Node identifying the extension associated with the category class and name
    * @param transportService The transport service communicating with the SDK
    * @param requestType The type of request that OpenSearch will send to the SDK
    */
    public NamedWriteableRegistryResponseHandler(DiscoveryNode extensionNode, TransportService transportService, String requestType) {
        this.extensionRegistry = new HashMap();
        this.extensionNode = extensionNode;
        this.transportService = transportService;
        this.requestType = requestType;
    }

    /**
    * @return A map of the given DiscoveryNode and its inner named writeable registry map
    */
    public Map<DiscoveryNode, Map<Class, Map<String, ExtensionReader>>> getExtensionRegistry() {
        return Collections.unmodifiableMap(this.extensionRegistry);
    }

    /**
     * Transports a StreamInput, converted into a byte array, and associated category class to the given extension, identified by its discovery node
     *
     * @param extensionNode Discovery Node identifying the extension associated with the category class and name
     * @param categoryClass Class that the Writeable object extends
     * @param context StreamInput object to convert into a byte array and transport to the extension
     * @throws UnknownHostException if connection to the extension node failed
     */
    public void parseNamedWriteable(DiscoveryNode extensionNode, Class categoryClass, StreamInput context) throws UnknownHostException {
        NamedWriteableRegistryParseResponseHandler namedWriteableRegistryParseResponseHandler =
            new NamedWriteableRegistryParseResponseHandler();
        try {
            logger.info("Sending extension request type: " + requestType);
            transportService.sendRequest(
                extensionNode,
                requestType,
                new NamedWriteableRegistryParseRequest(categoryClass, context),
                namedWriteableRegistryParseResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public NamedWriteableRegistryResponse read(StreamInput in) throws IOException {
        return new NamedWriteableRegistryResponse(in);
    }

    @Override
    public void handleResponse(NamedWriteableRegistryResponse response) {

        logger.info("response {}", response);
        logger.info("EXTENSION [" + extensionNode.getName() + "] returned " + response.getRegistry().size() + " entries");

        if (response.getRegistry().isEmpty() == false) {

            // Extension has sent over entries to register, initialize inner category map
            Map<Class, Map<String, ExtensionReader>> categoryMap = new HashMap<>();

            // Reader map associated with this current category
            Map<String, ExtensionReader> readers = null;
            Class currentCategory = null;

            for (Map.Entry<String, Class> entry : response.getRegistry().entrySet()) {

                String name = entry.getKey();
                Class categoryClass = entry.getValue();
                if (currentCategory != categoryClass) {
                    // After first pass, readers and current category are set
                    if (currentCategory != null) {
                        categoryMap.put(currentCategory, readers);
                    }
                    readers = new HashMap<>();
                    currentCategory = categoryClass;
                }

                // Add name and callback method reference to inner reader map,
                ExtensionReader callBack = (en, cc, context) -> parseNamedWriteable(en, cc, (StreamInput) context);
                readers.put(name, callBack);
            }

            // Handle last category and reader entry
            categoryMap.put(currentCategory, readers);

            // Attach extension node to categoryMap
            extensionRegistry.put(extensionNode, categoryMap);
        }
    }

    @Override
    public void handleException(TransportException exp) {
        logger.error(new ParameterizedMessage("OpenSearchRequest failed"), exp);
    }

    @Override
    public String executor() {
        return ThreadPool.Names.GENERIC;
    }
}
