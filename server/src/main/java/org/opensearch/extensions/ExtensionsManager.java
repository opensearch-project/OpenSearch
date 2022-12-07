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
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;

import org.opensearch.discovery.PluginRequest;
import org.opensearch.discovery.PluginResponse;
import org.opensearch.env.Environment;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.AcknowledgedResponse;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * The main class for Plugin Extensibility
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
        CREATE_COMPONENT,
        ON_INDEX_MODULE,
        GET_SETTINGS
    };

    private final Path extensionsPath;
    private final List<DiscoveryExtensionNode> uninitializedExtensions;
    private List<DiscoveryExtensionNode> extensions;
    private TransportService transportService;
    private ClusterService clusterService;

    public ExtensionsManager() {
        this.extensionsPath = Path.of("");
        this.uninitializedExtensions = new ArrayList<DiscoveryExtensionNode>();
    }

    public ExtensionsManager(Settings settings, Path extensionsPath) throws IOException {
        logger.info("ExtensionsManager initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.uninitializedExtensions = new ArrayList<DiscoveryExtensionNode>();
        this.extensions = new ArrayList<DiscoveryExtensionNode>();
        this.clusterService = null;

        /*
         * Now Discover extensions
         */
        discover();

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
            if (!uninitializedExtensions.isEmpty()) {
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
        try {
            uninitializedExtensions.add(
                new DiscoveryExtensionNode(
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
            throw e;
        }
    }

    public void initialize() {
        for (DiscoveryNode extensionNode : uninitializedExtensions) {
            initializeExtension(extensionNode);
        }
    }

    private void initializeExtension(DiscoveryNode extensionNode) {

        final TransportResponseHandler<PluginResponse> pluginResponseHandler = new TransportResponseHandler<PluginResponse>() {

            @Override
            public PluginResponse read(StreamInput in) throws IOException {
                return new PluginResponse(in);
            }

            @Override
            public void handleResponse(PluginResponse response) {
                for (DiscoveryExtensionNode extension : uninitializedExtensions) {
                    if (extension.getName().equals(response.getName())) {
                        extensions.add(extension);
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
            transportService.connectToExtensionNode(extensionNode);
            transportService.sendRequest(
                extensionNode,
                REQUEST_EXTENSION_ACTION_NAME,
                new PluginRequest(transportService.getLocalNode(), new ArrayList<DiscoveryExtensionNode>(uninitializedExtensions)),
                pluginResponseHandler
            );
        } catch (Exception e) {
            throw e;
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
        throw new IllegalStateException("Handler not present for the provided request: " + extensionRequest.getRequestType());
    }

    public void onIndexModule(IndexModule indexModule) throws UnknownHostException {
        for (DiscoveryNode extensionNode : uninitializedExtensions) {
            onIndexModule(indexModule, extensionNode);
        }
    }

    private void onIndexModule(IndexModule indexModule, DiscoveryNode extensionNode) throws UnknownHostException {
        logger.info("onIndexModule index:" + indexModule.getIndex());
        final CompletableFuture<IndicesModuleResponse> inProgressFuture = new CompletableFuture<>();
        final CompletableFuture<AcknowledgedResponse> inProgressIndexNameFuture = new CompletableFuture<>();
        final TransportResponseHandler<AcknowledgedResponse> acknowledgedResponseHandler = new TransportResponseHandler<
            AcknowledgedResponse>() {
            @Override
            public void handleResponse(AcknowledgedResponse response) {
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
            public AcknowledgedResponse read(StreamInput in) throws IOException {
                return new AcknowledgedResponse(in);
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
                                    acknowledgedResponseHandler
                                );
                                /*
                                 * Making async synchronous for now.
                                 */
                                inProgressIndexNameFuture.get(100, TimeUnit.SECONDS);
                                logger.info("Received ack response from Extension");
                            } catch (Exception e) {
                                logger.error(e.toString());
                            }
                        }
                    });
                }
                inProgressFuture.complete(response);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.info(new ParameterizedMessage("IndicesModuleRequest failed"), exp);
                inProgressFuture.completeExceptionally(exp);
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
            inProgressFuture.get(100, TimeUnit.SECONDS);
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

    public List<DiscoveryExtensionNode> getUninitializedExtensions() {
        return uninitializedExtensions;
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

}
