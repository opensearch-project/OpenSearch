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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.discovery.InitializeExtensionsRequest;
import org.opensearch.discovery.InitializeExtensionsResponse;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.extensions.rest.RegisterRestActionsRequest;
import org.opensearch.extensions.rest.RestActionsRequestHandler;
import org.opensearch.extensions.settings.CustomSettingsRequestHandler;
import org.opensearch.extensions.settings.RegisterCustomSettingsRequest;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.node.ReportingService;
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
 * The main class for orchestrating Extension communication with the OpenSearch Node.
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
    public static final String REQUEST_EXTENSION_ENVIRONMENT_SETTINGS = "internal:discovery/enviornmentsettings";
    public static final String REQUEST_EXTENSION_ADD_SETTINGS_UPDATE_CONSUMER = "internal:discovery/addsettingsupdateconsumer";
    public static final String REQUEST_EXTENSION_UPDATE_SETTINGS = "internal:discovery/updatesettings";
    public static final String REQUEST_EXTENSION_REGISTER_CUSTOM_SETTINGS = "internal:discovery/registercustomsettings";
    public static final String REQUEST_EXTENSION_REGISTER_REST_ACTIONS = "internal:discovery/registerrestactions";
    public static final String REQUEST_EXTENSION_REGISTER_TRANSPORT_ACTIONS = "internal:discovery/registertransportactions";
    public static final String REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY = "internal:discovery/namedwriteableregistry";
    public static final String REQUEST_OPENSEARCH_PARSE_NAMED_WRITEABLE = "internal:discovery/parsenamedwriteable";
    public static final String REQUEST_EXTENSION_ACTION_LISTENER_ON_FAILURE = "internal:extensions/actionlisteneronfailure";
    public static final String REQUEST_REST_EXECUTE_ON_EXTENSION_ACTION = "internal:extensions/restexecuteonextensiontaction";

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
        REQUEST_EXTENSION_ACTION_LISTENER_ON_FAILURE,
        REQUEST_EXTENSION_REGISTER_REST_ACTIONS,
        REQUEST_EXTENSION_REGISTER_SETTINGS,
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
    List<DiscoveryExtension> extensionsInitializedList;
    // A map of extension uniqueId to full extension details used for node transport here and in the RestActionsRequestHandler
    Map<String, DiscoveryExtension> extensionIdMap;
    RestActionsRequestHandler restActionsRequestHandler;
    CustomSettingsRequestHandler customSettingsRequestHandler;
    TransportService transportService;
    ClusterService clusterService;
    ExtensionNamedWriteableRegistry namedWriteableRegistry;
    ExtensionActionListener listener;
    ExtensionActionListenerHandler listenerHandler;
    EnvironmentSettingsRequestHandler environmentSettingsRequestHandler;
    AddSettingsUpdateConsumerRequestHandler addSettingsUpdateConsumerRequestHandler;

    /**
     * Instantiate a new ExtensionsOrchestrator object to handle requests and responses from extensions. This is called during Node bootstrap.
     *
     * @param settings  Settings from the node the orchestrator is running on.
     * @param extensionsPath  Path to a directory containing extension configuration file.
     * @throws IOException  If the extensions discovery file is not properly retrieved.
     */
    public ExtensionsOrchestrator(Settings settings, Path extensionsPath) throws IOException {
        logger.info("ExtensionsOrchestrator initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.extensionsInitializedList = new ArrayList<DiscoveryExtension>();
        this.extensionIdMap = new HashMap<String, DiscoveryExtension>();
        this.clusterService = null;
        this.namedWriteableRegistry = null;
        this.listener = new ExtensionActionListener();

        /*
         * Now Discover extensions
         */
        extensionsDiscovery();

    }

    /**
     * Initializes the {@link RestActionsRequestHandler}, {@link TransportService}, {@link ClusterService} and environment settings. This is called during Node bootstrap.
     * Lists/maps of extensions have already been initialized but not yet populated.
     *
     * @param restController  The RestController on which to register Rest Actions.
     * @param settingsModule The module that binds the provided settings to interface
     * @param transportService  The Node's transport service.
     * @param clusterService  The Node's cluster service.
     * @param initialEnvironmentSettings The finalized view of settings for the Environment
     */
    public void initializeServicesAndRestHandler(
        RestController restController,
        SettingsModule settingsModule,
        TransportService transportService,
        ClusterService clusterService,
        Settings initialEnvironmentSettings
    ) {
        this.restActionsRequestHandler = new RestActionsRequestHandler(restController, extensionIdMap, transportService);
        this.listenerHandler = new ExtensionActionListenerHandler(listener);
        this.customSettingsRequestHandler = new CustomSettingsRequestHandler(settingsModule);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.environmentSettingsRequestHandler = new EnvironmentSettingsRequestHandler(initialEnvironmentSettings);
        this.addSettingsUpdateConsumerRequestHandler = new AddSettingsUpdateConsumerRequestHandler(
            clusterService,
            transportService,
            REQUEST_EXTENSION_UPDATE_SETTINGS
        );
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
            REQUEST_EXTENSION_REGISTER_CUSTOM_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterCustomSettingsRequest::new,
            ((request, channel, task) -> channel.sendResponse(customSettingsRequestHandler.handleRegisterCustomSettingsRequest(request)))
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
            REQUEST_EXTENSION_ACTION_LISTENER_ON_FAILURE,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionActionListenerOnFailureRequest::new,
            ((request, channel, task) -> channel.sendResponse(listenerHandler.handleExtensionActionListenerOnFailureRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_ENVIRONMENT_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            EnvironmentSettingsRequest::new,
            ((request, channel, task) -> channel.sendResponse(environmentSettingsRequestHandler.handleEnvironmentSettingsRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_ADD_SETTINGS_UPDATE_CONSUMER,
            ThreadPool.Names.GENERIC,
            false,
            false,
            AddSettingsUpdateConsumerRequest::new,
            ((request, channel, task) -> channel.sendResponse(
                addSettingsUpdateConsumerRequestHandler.handleAddSettingsUpdateConsumerRequest(request)
            ))
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
                if (extensionIdMap.containsKey(extension.getUniqueId())) {
                    logger.info("Duplicate uniqueId " + extension.getUniqueId() + ". Did not load extension: " + extension);
                } else {
                    try {
                        DiscoveryExtension discoveryExtension = new DiscoveryExtension(
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
                        extensionIdMap.put(extension.getUniqueId(), discoveryExtension);
                        logger.info("Loaded extension with uniqueId " + extension.getUniqueId() + ": " + extension);
                    } catch (IllegalArgumentException e) {
                        logger.error(e.toString());
                    }
                }
            }
            if (!extensionIdMap.isEmpty()) {
                logger.info("Loaded all extensions");
            }
        } else {
            logger.info("Extensions.yml file is not present.  No extensions will be loaded.");
        }
    }

    /**
     * Iterate through all extensions and initialize them.  Initialized extensions will be added to the {@link #extensionsInitializedList}, and the {@link #namedWriteableRegistry} will be initialized.
     */
    public void extensionsInitialize() {
        for (DiscoveryExtension extension : extensionIdMap.values()) {
            extensionInitialize(extension);
        }
        this.namedWriteableRegistry = new ExtensionNamedWriteableRegistry(extensionsInitializedList, transportService);
    }

    private void extensionInitialize(DiscoveryExtension extension) {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final TransportResponseHandler<InitializeExtensionsResponse> extensionResponseHandler = new TransportResponseHandler<
            InitializeExtensionsResponse>() {

            @Override
            public InitializeExtensionsResponse read(StreamInput in) throws IOException {
                return new InitializeExtensionsResponse(in);
            }

            @Override
            public void handleResponse(InitializeExtensionsResponse response) {
                for (DiscoveryExtension extension : extensionIdMap.values()) {
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
            transportService.connectToNode(extension, true);
            transportService.sendRequest(
                extension,
                REQUEST_EXTENSION_ACTION_NAME,
                new InitializeExtensionsRequest(transportService.getLocalNode(), extension),
                extensionResponseHandler
            );
            inProgressLatch.await(100, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error(e.toString());
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
                throw new IllegalArgumentException("Handler not present for the provided request");
        }
    }

    public void onIndexModule(IndexModule indexModule) throws UnknownHostException {
        for (DiscoveryNode extensionNode : extensionIdMap.values()) {
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
             * Making asynchronous for now.
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
