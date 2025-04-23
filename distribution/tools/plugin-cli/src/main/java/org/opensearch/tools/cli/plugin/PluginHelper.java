/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.plugin;

import org.opensearch.plugins.PluginInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A helper class for the plugin-cli tasks.
 */
public class PluginHelper {

    private PluginHelper() {}

    /**
     * Verify if a plugin exists with any folder name.
     * @param pluginPath   the path for the plugins directory.
     * @param pluginName   the name of the concerned plugin.
     * @return             the path of the folder if the plugin exists.
     * @throws IOException if any I/O exception occurs while performing a file operation
     */
    public static Path verifyIfPluginExists(Path pluginPath, String pluginName) throws IOException {
        List<Path> pluginSubFolders = Files.walk(pluginPath, 1).filter(Files::isDirectory).collect(Collectors.toList());
        for (Path customPluginFolderPath : pluginSubFolders) {
            if (customPluginFolderPath != pluginPath
                && !((customPluginFolderPath.getFileName().toString()).contains(".installing"))
                && !((customPluginFolderPath.getFileName().toString()).contains(".removing"))) {
                final PluginInfo info = PluginInfo.readFromProperties(customPluginFolderPath);
                if (info.getName().equals(pluginName)) {
                    return customPluginFolderPath;
                }
            }
        }
        return pluginPath.resolve(pluginName);
    }
}
