/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterSettingsResponse;
import org.opensearch.cluster.LocalNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.io.stream.NamedWriteableRegistryParseRequest;
import org.opensearch.common.io.stream.NamedWriteableRegistryResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.discovery.PluginRequest;
import org.opensearch.discovery.PluginResponse;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndicesModuleNameResponse;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.node.ReportingService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The main class for Plugin Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionsOrchestrator implements ReportingService<PluginsAndModules> {
    public static final String REQUEST_EXTENSION_ACTION_NAME = "internal:discovery/extensions";
    public static final String INDICES_EXTENSION_POINT_ACTION_NAME = "indices:internal/extensions";
    public static final String INDICES_EXTENSION_NAME_ACTION_NAME = "indices:internal/name";
    public static final String REQUEST_EXTENSION_CLUSTER_STATE = "internal:discovery/clusterstate";
    public static final String REQUEST_EXTENSION_LOCAL_NODE = "internal:discovery/localnode";
    public static final String REQUEST_EXTENSION_CLUSTER_SETTINGS = "internal:discovery/clustersettings";
    public static final String REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY = "internal:discovery/namedwriteableregistry";
    public static final String REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE = "internal:discovery/parsenamedwriteable";

    private static final Logger logger = LogManager.getLogger(ExtensionsOrchestrator.class);

    /**
     * Enum for Extension Requests
     *
     * @opensearch.internal
     */
    public static enum RequestType {
        REQUEST_EXTENSION_CLUSTER_STATE,
        REQUEST_EXTENSION_LOCAL_NODE,
        REQUEST_EXTENSION_CLUSTER_SETTINGS,
        CREATE_COMPONENT,
        ON_INDEX_MODULE,
        GET_SETTINGS
    };

    /**
     * Enum for OpenSearch Requests
     *
     * @opensearch.internal
     */
    public static enum OpenSearchRequestType {
        REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY
    }

    private final Path extensionsPath;
    final List<DiscoveryExtension> extensionsList;
    List<DiscoveryExtension> extensionsInitializedList;
    TransportService transportService;
    ClusterService clusterService;
    Map<DiscoveryNode, Map<Class, Map<String, ExtensionReader>>> extensionNamedWriteableRegistry;

    public ExtensionsOrchestrator(Settings settings, Path extensionsPath) throws IOException {
        logger.info("ExtensionsOrchestrator initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.extensionsList = new ArrayList<DiscoveryExtension>();
        this.extensionsInitializedList = new ArrayList<DiscoveryExtension>();
        this.clusterService = null;
        this.extensionNamedWriteableRegistry = new HashMap<>();

        /*
         * Now Discover extensions
         */
        extensionsDiscovery();

    }

    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
        registerRequestHandler();
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    private void registerRequestHandler() {
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_CLUSTER_STATE,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_LOCAL_NODE,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_CLUSTER_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
    }

    @Override
    public PluginsAndModules info() {
        return null;
    }

    /*
     * Load and populate all extensions
     */
    private void extensionsDiscovery() throws IOException {
        logger.info("Extensions Config Directory :" + extensionsPath.toString());
        if (!FileSystemUtils.isAccessibleDirectory(extensionsPath, logger)) {
            return;
        }

        List<Extension> extensions = new ArrayList<Extension>();
        if (Files.exists(extensionsPath.resolve("extensions.yml"))) {
            try {
                extensions = readFromExtensionsYml(extensionsPath.resolve("extensions.yml")).getExtensions();
            } catch (IOException e) {
                throw new IOException("Could not read from extensions.yml", e);
            }
            for (Extension extension : extensions) {
                try {
                    extensionsList.add(
                        new DiscoveryExtension(
                            extension.getName(),
                            extension.getUniqueId(),
                            // placeholder for ephemeral id, will change with POC discovery
                            extension.getUniqueId(),
                            extension.getHostName(),
                            extension.getHostAddress(),
                            new TransportAddress(InetAddress.getByName(extension.getHostAddress()), Integer.parseInt(extension.getPort())),
                            new HashMap<String, String>(),
                            Version.fromString(extension.getOpensearchVersion()),
                            new PluginInfo(
                                extension.getName(),
                                extension.getDescription(),
                                extension.getVersion(),
                                Version.fromString(extension.getOpensearchVersion()),
                                extension.getJavaVersion(),
                                extension.getClassName(),
                                new ArrayList<String>(),
                                Boolean.parseBoolean(extension.hasNativeController())
                            )
                        )
                    );
                    logger.info("Loaded extension: " + extension);
                } catch (IllegalArgumentException e) {
                    logger.error(e.toString());
                }
            }
            if (!extensionsList.isEmpty()) {
                logger.info("Loaded all extensions");
            }
        } else {
            logger.info("Extensions.yml file is not present.  No extensions will be loaded.");
        }
    }

    public void extensionsInitialize() {
        for (DiscoveryNode extensionNode : extensionsList) {
            extensionInitialize(extensionNode);
        }
    }

    private void extensionInitialize(DiscoveryNode extensionNode) {

        final TransportResponseHandler<PluginResponse> pluginResponseHandler = new TransportResponseHandler<PluginResponse>() {

            @Override
            public PluginResponse read(StreamInput in) throws IOException {
                return new PluginResponse(in);
            }

            @Override
            public void handleResponse(PluginResponse response) {
                for (DiscoveryExtension extension : extensionsList) {
                    if (extension.getName().equals(response.getName())) {
                        extensionsInitializedList.add(extension);
                        break;
                    }
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(new ParameterizedMessage("Plugin request failed"), exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };
        try {
            transportService.connectToNode(extensionNode, true);
            transportService.sendRequest(
                extensionNode,
                REQUEST_EXTENSION_ACTION_NAME,
                new PluginRequest(transportService.getLocalNode(), new ArrayList<DiscoveryExtension>(extensionsList)),
                pluginResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    TransportResponse handleExtensionRequest(ExtensionRequest extensionRequest) throws Exception {
        // Read enum
        if (extensionRequest.getRequestType() == RequestType.REQUEST_EXTENSION_CLUSTER_STATE) {
            ClusterStateResponse clusterStateResponse = new ClusterStateResponse(
                clusterService.getClusterName(),
                clusterService.state(),
                false
            );
            return clusterStateResponse;
        } else if (extensionRequest.getRequestType() == RequestType.REQUEST_EXTENSION_LOCAL_NODE) {
            LocalNodeResponse localNodeResponse = new LocalNodeResponse(clusterService);
            return localNodeResponse;
        } else if (extensionRequest.getRequestType() == RequestType.REQUEST_EXTENSION_CLUSTER_SETTINGS) {
            ClusterSettingsResponse clusterSettingsResponse = new ClusterSettingsResponse(clusterService);
            return clusterSettingsResponse;
        }
        throw new Exception("Handler not present for the provided request");
    }

    /**
     * Iterates through all discovered extensions, sends transport requests for named writeables and consolidates all entires into a central named writeable registry for extensions.
     */
    public void getNamedWriteables() {

        // retrieve named writeable registry entries from each extension
        for (DiscoveryNode extensionNode : extensionsList) {
            try {
                Map<DiscoveryNode, Map<Class, Map<String, ExtensionReader>>> extensionRegistry = getNamedWriteables(extensionNode);
                if (extensionRegistry.isEmpty() == false) {
                    this.extensionNamedWriteableRegistry.putAll(extensionRegistry);
                }
            } catch (UnknownHostException e) {
                logger.error(e.toString());
            }
        }
    }

    /**
     * Sends a transport request for named writeables to an extension, identified by the given DiscoveryNode, and processes the response into registry entries
     *
     * @param extensionNode DiscoveryNode identifying the extension
     * @return A map of category classes and their associated names and readers for this discovery node
     */
    public Map<DiscoveryNode, Map<Class, Map<String, ExtensionReader>>> getNamedWriteables(DiscoveryNode extensionNode)
        throws UnknownHostException {

        // Initialize map of entries for this extension to return
        final Map<DiscoveryNode, Map<Class, Map<String, ExtensionReader>>> extensionRegistry = new HashMap<>();

        final TransportResponseHandler<NamedWriteableRegistryResponse> namedWriteableRegistryResponseHandler = new TransportResponseHandler<
            NamedWriteableRegistryResponse>() {

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

                    // Extract response entries and process fully qualified class name into category class instance
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
                        ExtensionReader newReader = (en, cc, context) -> parseNamedWriteable(en, cc, context);

                        // Validate that name has not yet been associated with a callback method
                        ExtensionReader oldReader = readers.put(name, newReader);
                        if (oldReader != null && oldReader.getClass() != null) {
                            throw new IllegalArgumentException(
                                "NamedWriteable ["
                                    + currentCategory.getName()
                                    + "]["
                                    + name
                                    + "]"
                                    + " is already registered for ["
                                    + oldReader.getClass().getName()
                                    + "],"
                                    + " cannot register ["
                                    + newReader.getClass().getName()
                                    + "]"
                            );
                        }
                    }

                    // Handle last category and reader entry
                    categoryMap.put(currentCategory, readers);

                    // Attach extension node to categoryMap
                    extensionRegistry.put(extensionNode, categoryMap);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error(new ParameterizedMessage("ExtensionRequest failed", exp));
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };

        logger.info("Sending extension request type: " + REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY);
        try {
            transportService.sendRequest(
                extensionNode,
                REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY,
                new OpenSearchRequest(OpenSearchRequestType.REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY),
                namedWriteableRegistryResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }

        return extensionRegistry;
    }

    /**
     * Iterates through list of discovered extensions and returns the callback method associated with the given category class and name
     *
     * @param categoryClass class that the Writeable object extends
     * @param name Unique name identifiying the Writeable object
     * @throws IllegalArgumentException if there is no reader associated with the given category class and name
     * @return A map of the discovery node and its associated extension reader
     */
    public Map<DiscoveryNode, ExtensionReader> getExtensionReader(Class categoryClass, String name) {

        ExtensionReader reader = null;
        DiscoveryNode extension = null;

        // The specific extension that the reader is associated with is not known, must iterate through all of them
        for (DiscoveryNode extensionNode : extensionsList) {
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
    public ExtensionReader getExtensionReader(DiscoveryNode extensionNode, Class categoryClass, String name) {
        ExtensionReader reader = null;
        Map<Class, Map<String, ExtensionReader>> categoryMap = this.extensionNamedWriteableRegistry.get(extensionNode);
        if (categoryMap != null) {
            Map<String, ExtensionReader> readerMap = categoryMap.get(categoryClass);
            if (readerMap != null) {
                reader = readerMap.get(name);
            }
        }
        return reader;
    }

    /**
     * Transports a byte array and associated category class to the given extension, identified by its discovery node
     *
     * @param extensionNode Discovery Node identifying the extension associated with the category class and name
     * @param categoryClass class that the Writeable object extends
     * @param context byte array generated from a {@link StreamInput} object to transport to the extension
     */
    public void parseNamedWriteable(DiscoveryNode extensionNode, Class categoryClass, byte[] context) throws UnknownHostException {
        logger.info("Sending extension request type: " + REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE);
        NamedWriteableRegistryParseResponseHandler namedWriteableRegistryParseResponseHandler =
            new NamedWriteableRegistryParseResponseHandler();
        try {
            transportService.sendRequest(
                extensionNode,
                REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE,
                new NamedWriteableRegistryParseRequest(categoryClass.getName(), context),
                namedWriteableRegistryParseResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    public void onIndexModule(IndexModule indexModule) throws UnknownHostException {
        for (DiscoveryNode extensionNode : extensionsList) {
            onIndexModule(indexModule, extensionNode);
        }
    }

    private void onIndexModule(IndexModule indexModule, DiscoveryNode extensionNode) throws UnknownHostException {
        logger.info("onIndexModule index:" + indexModule.getIndex());
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final CountDownLatch inProgressIndexNameLatch = new CountDownLatch(1);

        final TransportResponseHandler<IndicesModuleNameResponse> indicesModuleNameResponseHandler = new TransportResponseHandler<
            IndicesModuleNameResponse>() {
            @Override
            public void handleResponse(IndicesModuleNameResponse response) {
                logger.info("ACK Response" + response);
                inProgressIndexNameLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {

            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public IndicesModuleNameResponse read(StreamInput in) throws IOException {
                return new IndicesModuleNameResponse(in);
            }

        };

        final TransportResponseHandler<IndicesModuleResponse> indicesModuleResponseHandler = new TransportResponseHandler<
            IndicesModuleResponse>() {

            @Override
            public IndicesModuleResponse read(StreamInput in) throws IOException {
                return new IndicesModuleResponse(in);
            }

            @Override
            public void handleResponse(IndicesModuleResponse response) {
                logger.info("received {}", response);
                if (response.getIndexEventListener() == true) {
                    indexModule.addIndexEventListener(new IndexEventListener() {
                        @Override
                        public void beforeIndexRemoved(
                            IndexService indexService,
                            IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason
                        ) {
                            logger.info("Index Event Listener is called");
                            String indexName = indexService.index().getName();
                            logger.info("Index Name" + indexName.toString());
                            try {
                                logger.info("Sending request of index name to extension");
                                transportService.sendRequest(
                                    extensionNode,
                                    INDICES_EXTENSION_NAME_ACTION_NAME,
                                    new IndicesModuleRequest(indexModule),
                                    indicesModuleNameResponseHandler
                                );
                                /*
                                 * Making async synchronous for now.
                                 */
                                inProgressIndexNameLatch.await(100, TimeUnit.SECONDS);
                                logger.info("Received ack response from Extension");
                            } catch (Exception e) {
                                logger.error(e.toString());
                            }
                        }
                    });
                }
                inProgressLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error(new ParameterizedMessage("IndicesModuleRequest failed"), exp);
                inProgressLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };

        try {
            logger.info("Sending request to extension");
            transportService.sendRequest(
                extensionNode,
                INDICES_EXTENSION_POINT_ACTION_NAME,
                new IndicesModuleRequest(indexModule),
                indicesModuleResponseHandler
            );
            /*
             * Making async synchronous for now.
             */
            inProgressLatch.await(100, TimeUnit.SECONDS);
            logger.info("Received response from Extension");
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    private ExtensionsSettings readFromExtensionsYml(Path filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        InputStream input = Files.newInputStream(filePath);
        ExtensionsSettings extensionSettings = objectMapper.readValue(input, ExtensionsSettings.class);
        return extensionSettings;
    }

}
