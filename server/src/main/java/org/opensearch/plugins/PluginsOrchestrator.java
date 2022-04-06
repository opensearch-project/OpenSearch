/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.discovery.PluginRequest;
import org.opensearch.discovery.PluginResponse;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndicesModuleRequest;
import org.opensearch.index.IndicesModuleResponse;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.node.ReportingService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PluginsOrchestrator implements ReportingService<PluginsAndModules> {
    public static final String REQUEST_EXTENSION_ACTION_NAME = "internal:discovery/extensions";
    public static final String INDICES_EXTENSION_POINT_ACTION_NAME = "indices:internal/extensions";

    private static final Logger logger = LogManager.getLogger(PluginsOrchestrator.class);
    private final Path extensionsPath;
    final List<DiscoveryExtension> pluginsConfigSet;
    TransportService transportService;
    final DiscoveryNode extensionNode;

    public PluginsOrchestrator(Settings settings, Path extensionsPath) throws IOException {
        logger.info("PluginsOrchestrator initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = null;
        this.pluginsConfigSet = new ArrayList<DiscoveryExtension>();

        /*
         * Now Discover plugins
         */
        pluginsDiscovery();

        this.extensionNode = new DiscoveryNode(
            "node_extension",
            new TransportAddress(InetAddress.getByName("127.0.0.1"), 4532),
            Version.CURRENT
        );
    }

    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public PluginsAndModules info() {
        return null;
    }

    /*
     * Load all Independent plugins(for now)
     * Populate list of plugins
     */
    private void pluginsDiscovery() throws IOException {
        logger.info("PluginsDirectory :" + extensionsPath.toString());
        if (!FileSystemUtils.isAccessibleDirectory(extensionsPath, logger)) {
            return;
        }
        for (final Path plugin : PluginsService.findPluginDirs(extensionsPath)) {
            try {
                PluginInfo pluginInfo = PluginInfo.readFromProperties(plugin);
                /*
                 * TODO: Read from extensions.yml
                 */
                pluginsConfigSet.add(
                    new DiscoveryExtension(
                        "myfirstextension",
                        "id",
                        "extensionId",
                        "hostName",
                        "0.0.0.0",
                        new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                        null,
                        Version.CURRENT,
                        pluginInfo
                    )
                );

            } catch (final IOException e) {
                throw new IllegalStateException("Could not load plugin descriptor " + plugin.getFileName(), e);
            }
        }
        logger.info("Loaded independent plugins");
    }

    public void pluginsInitialize() {

        final TransportResponseHandler<PluginResponse> pluginResponseHandler = new TransportResponseHandler<PluginResponse>() {

            @Override
            public PluginResponse read(StreamInput in) throws IOException {
                return new PluginResponse(in);
            }

            @Override
            public void handleResponse(PluginResponse response) {
                logger.info("received {}", response);
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
            transportService.connectToNode(extensionNode);
            transportService.sendRequest(
                extensionNode,
                REQUEST_EXTENSION_ACTION_NAME,
                new PluginRequest(extensionNode, pluginsConfigSet),
                pluginResponseHandler
            );
        } catch (Exception e) {
            logger.error(e.toString());
        }

    }

    public void onIndexModule(IndexModule indexModule) throws UnknownHostException {
        logger.info("onIndexModule index:" + indexModule.getIndex());
        final TransportResponseHandler<IndicesModuleResponse> indicesModuleResponseHandler = new TransportResponseHandler<IndicesModuleResponse>() {

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
                        public void beforeIndexCreated(Index index, Settings indexSettings) {
                            beforeIndexCreatePO(index, indexSettings);
                        }
                    });
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(new ParameterizedMessage("IndicesModuleRequest failed"), exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };
        try {
            logger.info("Sending request to extension");
            transportService.sendRequest(extensionNode, INDICES_EXTENSION_POINT_ACTION_NAME, new IndicesModuleRequest(indexModule), indicesModuleResponseHandler);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    private void beforeIndexCreatePO(Index index, Settings indexSettings) {
        logger.info("beforeIndexCreated event handler");
    }
}
