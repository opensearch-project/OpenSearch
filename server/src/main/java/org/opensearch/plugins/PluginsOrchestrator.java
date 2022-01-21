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
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ReportingService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class PluginsOrchestrator implements ReportingService<PluginsAndModules>  {
    public static final Setting<String> PATH_HOME_SETTING = Setting.simpleString("path.home", Setting.Property.NodeScope);
    private static final Logger logger = LogManager.getLogger(PluginsOrchestrator.class);
    private final Path pluginsv2Path;

    public PluginsOrchestrator(
        Settings settings,
        String pluginsDirectory
    ) throws IOException {
        logger.info("PluginsOrchestrator initialized");
        /*
         * Find opensearch home path
         */
        final Path homeFile;
        if (PATH_HOME_SETTING.exists(settings)) {
            homeFile = PathUtils.get(PATH_HOME_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            throw new IllegalStateException(PATH_HOME_SETTING.getKey() + " is not configured");
        }
        /*
         * Find new plugins directory
         */
        pluginsv2Path = homeFile.resolve(pluginsDirectory);

        /*
         * Now Discover plugins
         */
        pluginsDiscovery(pluginsv2Path);
    }

    @Override
    public PluginsAndModules info() {
        return null;
    }

    /*
     * Load all Independent plugins(for now)
     * Populate list of plugins
     */
    private void pluginsDiscovery(Path pluginsDirectory) throws IOException {
        logger.info("PluginsDirectory :" + pluginsDirectory.toString());
        if (!FileSystemUtils.isAccessibleDirectory(pluginsDirectory, logger)) {
            return;
        }
        final Set<PluginInfo> pluginInfoSet = new HashSet<>();
        for (final Path plugin : PluginsService.findPluginDirs(pluginsDirectory)) {
            try {
                PluginInfo.readFromProperties(plugin);
            } catch (final IOException e) {
                throw new IllegalStateException("Could not load plugin descriptor " + plugin.getFileName(), e);
            }
        }
    }
}
