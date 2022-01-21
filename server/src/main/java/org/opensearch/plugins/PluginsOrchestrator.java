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
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ReportingService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class PluginsOrchestrator implements ReportingService<PluginsAndModules> {
    public static final Setting<String> PATH_HOME_SETTING = Setting.simpleString("path.home", Setting.Property.NodeScope);
    private static final Logger logger = LogManager.getLogger(PluginsOrchestrator.class);
    private final Path pluginsv2Path;

    public PluginsOrchestrator(Settings settings, Path pluginsv2Path) throws IOException {
        logger.info("PluginsOrchestrator initialized");
        this.pluginsv2Path = pluginsv2Path;

        /*
         * Now Discover plugins
         */
        pluginsDiscovery();
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
        logger.info("PluginsDirectory :" + pluginsv2Path.toString());
        if (!FileSystemUtils.isAccessibleDirectory(pluginsv2Path, logger)) {
            return;
        }
        final Set<PluginInfo> pluginInfoSet = new HashSet<>();
        for (final Path plugin : PluginsService.findPluginDirs(pluginsv2Path)) {
            try {
                PluginInfo.readFromProperties(plugin);
            } catch (final IOException e) {
                throw new IllegalStateException("Could not load plugin descriptor " + plugin.getFileName(), e);
            }
        }
    }
}
