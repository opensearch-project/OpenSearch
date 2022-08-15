/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.discovery.InitializeExtensionsRequest;
import org.opensearch.discovery.InitializeExtensionsResponse;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.node.ReportingService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    public static final String REQUEST_EXTENSION_REGISTER_REST_API = "internal:discovery/registerrestapi";
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
        REQUEST_EXTENSION_REGISTER_REST_API,
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
    Map<String, DiscoveryExtension> extensionIdMap;
    Map<String, List<String>> extensionRestApiMap;
    TransportService transportService;
    ClusterService clusterService;
    ExtensionNamedWriteableRegistry namedWriteableRegistry;

    /**
     * Instantiate a new ExtensionsOrchestrator object to handle requests and responses from extensions.
     *
     * @param settings  Settings from the node the orchestrator is running on.
     * @param extensionsPath  Path to a directory containing extensions.
     * @throws IOException  If the extensions discovery file is not properly retrieved.
     */
    public ExtensionsOrchestrator(Settings settings, Path extensionsPath) throws IOException {
        logger.info("ExtensionsOrchestrator initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.extensionsList = new ArrayList<DiscoveryExtension>();
        this.extensionsInitializedList = new ArrayList<DiscoveryExtension>();
        this.extensionIdMap = new HashMap<String, DiscoveryExtension>();
        this.extensionRestApiMap = new HashMap<String, List<String>>();
        this.clusterService = null;
        this.namedWriteableRegistry = null;

        /*
         * Now Discover extensions
         */
        extensionsDiscovery();

    }

    /**
     * Sets the transport service and registers request handlers.
     *
     * @param transportService  The transport service to set.
     */
    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
        registerRequestHandler();
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setNamedWriteableRegistry() {
        this.namedWriteableRegistry = new ExtensionNamedWriteableRegistry(extensionsInitializedList, transportService);
    }

    private void registerRequestHandler() {
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_REGISTER_REST_API,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterRestApiRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleRegisterRestApiRequest(request)))
        );
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
                    DiscoveryExtension de = new DiscoveryExtension(
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
                    );
                    extensionsList.add(de);
                    extensionIdMap.put(extension.getUniqueId(), de);
                    logger.info("Loaded extension: " + extension + " with id " + extension.getUniqueId());
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
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final TransportResponseHandler<InitializeExtensionsResponse> extensionResponseHandler = new TransportResponseHandler<
            InitializeExtensionsResponse>() {

            @Override
            public InitializeExtensionsResponse read(StreamInput in) throws IOException {
                return new InitializeExtensionsResponse(in);
            }

            @Override
            public void handleResponse(InitializeExtensionsResponse response) {
                for (DiscoveryExtension extension : extensionsList) {
                    if (extension.getName().equals(response.getName())) {
                        extensionsInitializedList.add(extension);
                        logger.info("Initialized extension: " + extension.getName());
                        break;
                    }
                }
                inProgressLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(new ParameterizedMessage("Extension initialization failed"), exp);
                inProgressLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };
        try {
            logger.info("Sending extension request type: " + REQUEST_EXTENSION_ACTION_NAME);
            transportService.connectToNode(extensionNode, true);
            transportService.sendRequest(
                extensionNode,
                REQUEST_EXTENSION_ACTION_NAME,
                new InitializeExtensionsRequest(transportService.getLocalNode(), new ArrayList<DiscoveryExtension>(extensionsList)),
                extensionResponseHandler
            );
            inProgressLatch.await(100, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * Handles a {@link RegisterRestApiRequest}.
     *
     * @param restApiRequest  The request to handle.
     * @return  A {@link RegisterRestApiResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    TransportResponse handleRegisterRestApiRequest(RegisterRestApiRequest restApiRequest) throws Exception {
        DiscoveryExtension extension = extensionIdMap.get(restApiRequest.getNodeId());
        if (extension == null) {
            throw new IllegalArgumentException(
                "API Request unique id " + restApiRequest.getNodeId() + " does not match a discovered extension."
            );
        }
        for (String restApi : restApiRequest.getRestApi()) {
            RestRequest.Method method;
            String uri;
            try {
                int delim = restApi.indexOf(' ');
                method = RestRequest.Method.valueOf(restApi.substring(0, delim));
                uri = restApi.substring(delim).trim();
            } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                throw new IllegalArgumentException(restApi + " does not begin with a valid REST method");
            }
            logger.info("Registering: " + method + " /_extensions/" + extension.getName() + uri);
            // TODO put more REST handler stuff here
            // Register using RestController.registerHandler, try to use plugin getRestHandler first
        }
        extensionRestApiMap.put(restApiRequest.getNodeId(), restApiRequest.getRestApi());
        return new RegisterRestApiResponse(
            "Registered node "
                + restApiRequest.getNodeId()
                + ", extension "
                + extension.getName()
                + " to handle REST API "
                + restApiRequest.getRestApi()
        );
    }

    /**
     * Handles an {@link ExtensionRequest}.
     *
     * @param extensionRequest  The request to handle, of a type defined in the {@link RequestType} enum.
     * @return  an Response matching the request.
     * @throws Exception if the request is not handled properly.
     */
    TransportResponse handleExtensionRequest(ExtensionRequest extensionRequest) throws Exception {
        switch (extensionRequest.getRequestType()) {
            case REQUEST_EXTENSION_CLUSTER_STATE:
                return new ClusterStateResponse(clusterService.getClusterName(), clusterService.state(), false);
            case REQUEST_EXTENSION_LOCAL_NODE:
                return new LocalNodeResponse(clusterService);
            case REQUEST_EXTENSION_CLUSTER_SETTINGS:
                return new ClusterSettingsResponse(clusterService);
            default:
                throw new Exception("Handler not present for the provided request");
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

        final TransportResponseHandler<ExtensionBooleanResponse> indicesModuleNameResponseHandler = new TransportResponseHandler<
            ExtensionBooleanResponse>() {
            @Override
            public void handleResponse(ExtensionBooleanResponse response) {
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
            public ExtensionBooleanResponse read(StreamInput in) throws IOException {
                return new ExtensionBooleanResponse(in);
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
                                logger.info("Sending extension request type: " + INDICES_EXTENSION_NAME_ACTION_NAME);
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
            logger.info("Sending extension request type: " + INDICES_EXTENSION_POINT_ACTION_NAME);
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
