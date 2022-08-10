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
import org.opensearch.common.ParseField;
import org.opensearch.common.xcontent.NamedXContentRegistryParseRequest;
import org.opensearch.common.xcontent.NamedXContentRegistryResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Response handler for NamedXContentRegistry Requests
 *
 * @opensearch.internal
 */
public class NamedXContentRegistryResponseHandler implements TransportResponseHandler<NamedXContentRegistryResponse> {
    private static final Logger logger = LogManager.getLogger(NamedXContentRegistryResponseHandler.class);

    private final Map<DiscoveryNode, Map<Class, Map<String, Map<ParseField, ExtensionReader>>>> extensionRegistry;
    private final DiscoveryNode extensionNode;
    private final TransportService transportService;
    private final String requestType;

    /**
    * Instantiates a new NamedXContentRegistry response handler
    *
    * @param extensionNode Discovery Node identifying the extension associated with the category class and ParseField
    * @param transportService The transport service communicating with the SDK
    * @param requestType The type of request that OpenSearch will send to the SDK
    */
    public NamedXContentRegistryResponseHandler(DiscoveryNode extensionNode, TransportService transportService, String requestType) {
        this.extensionRegistry = new HashMap<>();
        this.extensionNode = extensionNode;
        this.transportService = transportService;
        this.requestType = requestType;
    }

    /**
     * Transports a StreamInput, converted into a byte array, and associated category class to the given extension, identified by its discovery node
     *
     * @param extensionNode Discovery Node identifying the extension associated with the category class and name
     * @param categoryClass Class that the XContent object extends
     * @param context String that the parser will be applied to
     * @throws UnknownHostException if connection to the extension node failed
     */
    public void parseNamedXContent(DiscoveryNode extensionNode, Class categoryClass, String context) throws UnknownHostException {
        NamedXContentRegistryParseResponseHandler namedXContentRegistryParseResponseHandler =
            new NamedXContentRegistryParseResponseHandler();
        try {
            logger.info("Sending extension request type: " + requestType);
            transportService.sendRequest(
                extensionNode,
                requestType,
                new NamedXContentRegistryParseRequest(categoryClass, context),
                namedXContentRegistryParseResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * @return A map of the given DiscoveryNode and its inner names xcontent registry map
     */

    public Map<DiscoveryNode, Map<Class, Map<String, Map<ParseField, ExtensionReader>>>> getExtensionRegistry() {
        return Collections.unmodifiableMap(this.extensionRegistry);
    }

    @Override
    public NamedXContentRegistryResponse read(StreamInput in) throws IOException {
        return new NamedXContentRegistryResponse(in);
    }

    @Override
    public void handleResponse(NamedXContentRegistryResponse response) {

        logger.info("response {}", response);
        logger.info("EXTENSION [" + extensionNode.getName() + "] returned " + response.getRegistry().size() + " entries");

        if (response.getRegistry().isEmpty() == false) {

            // Initialize inner category map
            Map<Class, Map<String, Map<ParseField, ExtensionReader>>> categoryMap = new HashMap<>();

            // ParseField name map associated with this current category
            Map<String, Map<ParseField, ExtensionReader>> nameMap = null;
            Class currentCategory = null;

            // Extract response entries and populate the registry
            for (Map.Entry<ParseField, Class> entry : response.getRegistry().entrySet()) {

                ParseField parseField = entry.getKey();
                Class categoryClass = entry.getValue();

                if (currentCategory != categoryClass) {
                    // After first pass, parsefield names and current category are set
                    if (currentCategory != null) {
                        categoryMap.put(currentCategory, nameMap);
                    }
                    nameMap = new HashMap<>();
                    currentCategory = categoryClass;
                }

                Map<ParseField, ExtensionReader> readerMap = null;
                String currentName = null;

                // Iterate through all parsefield names (including deprecated names)
                for (String name : parseField.getAllNamesIncludedDeprecated()) {

                    if (currentName != name) {
                        if (currentName != null) {
                            // After first pass, ParseFields are set with their corresponding parseField name
                            nameMap.put(currentName, readerMap);
                        }
                        readerMap = new HashMap<>();
                        currentName = name;
                    }
                    ExtensionReader callback = (en, cc, context) -> parseNamedXContent(en, cc, (String) context);
                    readerMap.put(parseField, callback);
                }

                // Handle last name and reader map
                nameMap.put(currentName, readerMap);
            }

            // Handle last category class and name map
            categoryMap.put(currentCategory, nameMap);

            // Attach extension node to category map
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
