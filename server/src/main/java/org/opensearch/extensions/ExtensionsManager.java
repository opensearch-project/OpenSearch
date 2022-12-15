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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterSettingsResponse;
import org.opensearch.cluster.LocalNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;

import org.opensearch.discovery.InitializeExtensionRequest;
import org.opensearch.discovery.InitializeExtensionResponse;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.extensions.rest.RegisterRestActionsRequest;
import org.opensearch.extensions.rest.RestActionsRequestHandler;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.rest.RestController;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * The main class for managing Extension communication with the OpenSearch Node.
 *
 * @opensearch.internal
 */
public class ExtensionsManager {
    public static final String REQUEST_EXTENSION_ACTION_NAME = "internal:discovery/extensions";
    public static final String INDICES_EXTENSION_POINT_ACTION_NAME = "indices:internal/extensions";
    public static final String INDICES_EXTENSION_NAME_ACTION_NAME = "indices:internal/name";
    public static final String REQUEST_EXTENSION_CLUSTER_STATE = "internal:discovery/clusterstate";
    public static final String REQUEST_EXTENSION_LOCAL_NODE = "internal:discovery/localnode";
    public static final String REQUEST_EXTENSION_CLUSTER_SETTINGS = "internal:discovery/clustersettings";
    public static final String REQUEST_EXTENSION_REGISTER_REST_ACTIONS = "internal:discovery/registerrestactions";
    public static final String REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY = "internal:discovery/namedwriteableregistry";
    public static final String REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE = "internal:discovery/parsenamedwriteable";
    public static final String REQUEST_REST_EXECUTE_ON_EXTENSION_ACTION = "internal:extensions/restexecuteonextensiontaction";
    public static final String REQUEST_EXTENSION_REGISTER_TRANSPORT_ACTIONS = "internal:discovery/registertransportactions";

    private static final Logger logger = LogManager.getLogger(ExtensionsManager.class);

    /**
     * Enum for Extension Requests
     *
     * @opensearch.internal
     */
    public static enum RequestType {
        REQUEST_EXTENSION_CLUSTER_STATE,
        REQUEST_EXTENSION_LOCAL_NODE,
        REQUEST_EXTENSION_CLUSTER_SETTINGS,
        REQUEST_EXTENSION_REGISTER_REST_ACTIONS,
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
    // A list of initialized extensions, a subset of the values of map below which includes all extensions
    private List<DiscoveryExtensionNode> extensions;
    private Map<String, DiscoveryExtensionNode> extensionIdMap;
    private RestActionsRequestHandler restActionsRequestHandler;
    private TransportService transportService;
    private ClusterService clusterService;

    public ExtensionsManager() {
        this.extensionsPath = Path.of("");
    }

    /**
     * Instantiate a new ExtensionsManager object to handle requests and responses from extensions. This is called during Node bootstrap.
     *
     * @param settings  Settings from the node the orchestrator is running on.
     * @param extensionsPath  Path to a directory containing extensions.
     * @throws IOException  If the extensions discovery file is not properly retrieved.
     */
    public ExtensionsManager(Settings settings, Path extensionsPath) throws IOException {
        logger.info("ExtensionsManager initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.extensions = new ArrayList<DiscoveryExtensionNode>();
        this.extensionIdMap = new HashMap<String, DiscoveryExtensionNode>();
        this.clusterService = null;

        /*
         * Now Discover extensions
         */
        discover();

    }

    /**
     * Initializes the {@link RestActionsRequestHandler}, {@link TransportService} and {@link ClusterService}. This is called during Node bootstrap.
     * Lists/maps of extensions have already been initialized but not yet populated.
     *
     * @param restController  The RestController on which to register Rest Actions.
     * @param transportService  The Node's transport service.
     * @param clusterService  The Node's cluster service.
     */
    public void initializeServicesAndRestHandler(
        RestController restController,
        TransportService transportService,
        ClusterService clusterService
    ) {
        this.restActionsRequestHandler = new RestActionsRequestHandler(restController, extensionIdMap, transportService);
        this.transportService = transportService;
        this.clusterService = clusterService;
        registerRequestHandler();
    }

    private void registerRequestHandler() {
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_REGISTER_REST_ACTIONS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterRestActionsRequest::new,
            ((request, channel, task) -> channel.sendResponse(restActionsRequestHandler.handleRegisterRestActionsRequest(request)))
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
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_REGISTER_TRANSPORT_ACTIONS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterTransportActionsRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleRegisterTransportActionsRequest(request)))
        );
    }

    /*
     * Load and populate all extensions
     */
    private void discover() throws IOException {
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
                loadExtension(extension);
            }
            if (!extensionIdMap.isEmpty()) {
                logger.info("Loaded all extensions");
            }
        } else {
            logger.info("Extensions.yml file is not present.  No extensions will be loaded.");
        }
    }

    /**
     * Loads a single extension
     * @param extension The extension to be loaded
     */
    private void loadExtension(Extension extension) throws IOException {
        if (extensionIdMap.containsKey(extension.getUniqueId())) {
            logger.info("Duplicate uniqueId " + extension.getUniqueId() + ". Did not load extension: " + extension);
        } else {
            try {
                DiscoveryExtensionNode discoveryExtensionNode = new DiscoveryExtensionNode(
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
                extensionIdMap.put(extension.getUniqueId(), discoveryExtensionNode);
                logger.info("Loaded extension with uniqueId " + extension.getUniqueId() + ": " + extension);
            } catch (IllegalArgumentException e) {
                throw e;
            }
        }
    }

    /**
     * Iterate through all extensions and initialize them.  Initialized extensions will be added to the {@link #extensions}.
     */
    public void initialize() {
        for (DiscoveryExtensionNode extension : extensionIdMap.values()) {
            initializeExtension(extension);
        }
    }

    private void initializeExtension(DiscoveryExtensionNode extension) {

        final CompletableFuture<InitializeExtensionResponse> inProgressFuture = new CompletableFuture<>();
        final TransportResponseHandler<InitializeExtensionResponse> initializeExtensionResponseHandler = new TransportResponseHandler<
            InitializeExtensionResponse>() {

            @Override
            public InitializeExtensionResponse read(StreamInput in) throws IOException {
                return new InitializeExtensionResponse(in);
            }

            @Override
            public void handleResponse(InitializeExtensionResponse response) {
                for (DiscoveryExtensionNode extension : extensionIdMap.values()) {
                    if (extension.getName().equals(response.getName())) {
                        extensions.add(extension);
                        logger.info("Initialized extension: " + extension.getName());
                        break;
                    }
                }
                inProgressFuture.complete(response);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error(new ParameterizedMessage("Extension initialization failed"), exp);
                inProgressFuture.completeExceptionally(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };
        try {
            logger.info("Sending extension request type: " + REQUEST_EXTENSION_ACTION_NAME);
            transportService.connectToExtensionNode(extension);
            transportService.sendRequest(
                extension,
                REQUEST_EXTENSION_ACTION_NAME,
                new InitializeExtensionRequest(transportService.getLocalNode(), extension),
                initializeExtensionResponseHandler
            );
            // TODO: make asynchronous
            inProgressFuture.get(100, TimeUnit.SECONDS);
        } catch (Exception e) {
            try {
                throw e;
            } catch (Exception e1) {
                logger.error(e.toString());
            }
        }
    }

    /**
     * Handles a {@link RegisterTransportActionsRequest}.
     *
     * @param transportActionsRequest  The request to handle.
     * @return  A {@link ExtensionBooleanResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    TransportResponse handleRegisterTransportActionsRequest(RegisterTransportActionsRequest transportActionsRequest) throws Exception {
        /*
         * TODO: https://github.com/opensearch-project/opensearch-sdk-java/issues/107
         * Register these new Transport Actions with ActionModule
         * and add support for NodeClient to recognise these actions when making transport calls.
         */
        return new ExtensionBooleanResponse(true);
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
                throw new IllegalStateException("Handler not present for the provided request");
        }
    }

    public void onIndexModule(IndexModule indexModule) throws UnknownHostException {
        for (DiscoveryNode extensionNode : extensionIdMap.values()) {
            onIndexModule(indexModule, extensionNode);
        }
    }

    private void onIndexModule(IndexModule indexModule, DiscoveryNode extensionNode) throws UnknownHostException {
        logger.info("onIndexModule index:" + indexModule.getIndex());
        final CompletableFuture<IndicesModuleResponse> inProgressFuture = new CompletableFuture<>();
        final CompletableFuture<ExtensionBooleanResponse> inProgressIndexNameFuture = new CompletableFuture<>();
        final TransportResponseHandler<ExtensionBooleanResponse> extensionBooleanResponseHandler = new TransportResponseHandler<
            ExtensionBooleanResponse>() {
            @Override
            public void handleResponse(ExtensionBooleanResponse response) {
                logger.info("ACK Response" + response);
                inProgressIndexNameFuture.complete(response);
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
                                    extensionBooleanResponseHandler
                                );
                                // TODO: make asynchronous
                                inProgressIndexNameFuture.get(100, TimeUnit.SECONDS);
                                logger.info("Received ack response from Extension");
                            } catch (Exception e) {
                                try {
                                    throw e;
                                } catch (Exception e1) {
                                    logger.error(e.toString());
                                }
                            }
                        }
                    });
                }
                inProgressFuture.complete(response);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error(new ParameterizedMessage("IndicesModuleRequest failed"), exp);
                inProgressFuture.completeExceptionally(exp);
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
            // TODO: make asynchronous
            inProgressFuture.get(100, TimeUnit.SECONDS);
            logger.info("Received response from Extension");
        } catch (Exception e) {
            try {
                throw e;
            } catch (Exception e1) {
                logger.error(e.toString());
            }
        }
    }

    private ExtensionsSettings readFromExtensionsYml(Path filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        InputStream input = Files.newInputStream(filePath);
        ExtensionsSettings extensionSettings = objectMapper.readValue(input, ExtensionsSettings.class);
        return extensionSettings;
    }

    public static String getRequestExtensionActionName() {
        return REQUEST_EXTENSION_ACTION_NAME;
    }

    public static String getIndicesExtensionPointActionName() {
        return INDICES_EXTENSION_POINT_ACTION_NAME;
    }

    public static String getIndicesExtensionNameActionName() {
        return INDICES_EXTENSION_NAME_ACTION_NAME;
    }

    public static String getRequestExtensionClusterState() {
        return REQUEST_EXTENSION_CLUSTER_STATE;
    }

    public static String getRequestExtensionLocalNode() {
        return REQUEST_EXTENSION_LOCAL_NODE;
    }

    public static String getRequestExtensionClusterSettings() {
        return REQUEST_EXTENSION_CLUSTER_SETTINGS;
    }

    public static Logger getLogger() {
        return logger;
    }

    public Path getExtensionsPath() {
        return extensionsPath;
    }

    public List<DiscoveryExtensionNode> getExtensions() {
        return extensions;
    }

    public TransportService getTransportService() {
        return transportService;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public static String getRequestExtensionRegisterRestActions() {
        return REQUEST_EXTENSION_REGISTER_REST_ACTIONS;
    }

    public static String getRequestOpensearchNamedWriteableRegistry() {
        return REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY;
    }

    public static String getRequestOpensearchParseNamedWriteable() {
        return REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE;
    }

    public static String getRequestRestExecuteOnExtensionAction() {
        return REQUEST_REST_EXECUTE_ON_EXTENSION_ACTION;
    }

    public Map<String, DiscoveryExtensionNode> getExtensionIdMap() {
        return extensionIdMap;
    }

    public RestActionsRequestHandler getRestActionsRequestHandler() {
        return restActionsRequestHandler;
    }

}
