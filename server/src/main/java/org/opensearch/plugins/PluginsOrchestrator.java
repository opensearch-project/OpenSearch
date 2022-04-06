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
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.discovery.PluginRequest;
import org.opensearch.discovery.PluginResponse;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.node.ReportingService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PluginsOrchestrator implements ReportingService<PluginsAndModules> {
    public static final String REQUEST_EXTENSION_ACTION_NAME = "internal:discovery/extensions";
    private static final Logger logger = LogManager.getLogger(PluginsOrchestrator.class);
    private final Path extensionsPath;
    final List<DiscoveryExtension> pluginsConfigSet;
    final TransportService transportService;

    public PluginsOrchestrator(Settings settings, Path extensionsPath, TransportService transportService) throws IOException {
        logger.info("PluginsOrchestrator initialized");
        this.extensionsPath = extensionsPath;
        this.transportService = transportService;
        this.pluginsConfigSet = new ArrayList<DiscoveryExtension>();

        /*
         * Now Discover plugins
         */
        pluginsDiscovery();

        transportService.registerRequestHandler(
            REQUEST_EXTENSION_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            PluginRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePluginsRequest(request))
        );
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
        final DiscoveryNode localNode = transportService.getLocalNode();

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
        logger.info(localNode.toString());
        try {
            /*DiscoveryNode extensionNode = new DiscoveryNode(
                "node_g",
                new TransportAddress(InetAddress.getByName("127.0.0.1"), 65432),
                Version.CURRENT
            );*/
            PluginInfo info = new PluginInfo(
                "c",
                "foo",
                "dummy",
                Version.CURRENT,
                "1.8",
                "dummyclass",
                "c",
                Collections.singletonList("foo"),
                true
            );

            // DiscoveryExtension extensionNode = new DiscoveryExtension(
            // "myfirstextension",
            // "id",
            // "extensionId",
            // "hostName",
            // "0.0.0.0",
            // new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            // Collections.emptyMap(),
            // Version.CURRENT,
            // info
            // );
            // DiscoveryNode extensionNode = new DiscoveryNode(
            // "myfirstextension",
            // "id",
            // "extensionId",
            // "hostName",
            // "0.0.0.0",
            // new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            // Collections.emptyMap(),
            // (Set<DiscoveryNodeRole>) DiscoveryNodeRole.MASTER_ROLE,
            // Version.CURRENT
            // );
            DiscoveryNode extensionNode = new DiscoveryNode(
                "node_extension",
                new TransportAddress(InetAddress.getByName("127.0.0.1"), 4532),
                Version.CURRENT
            );
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

    PluginResponse handlePluginsRequest(PluginRequest pluginRequest) {
        logger.info("Handling Plugins Request");
        return null;
    }
}
