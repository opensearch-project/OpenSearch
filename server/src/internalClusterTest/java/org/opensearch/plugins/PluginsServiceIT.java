/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PluginsServiceIT extends OpenSearchIntegTestCase {

    public void testNodeBootstrapWithCompatiblePlugin() throws IOException {
        // Prepare the plugins directory and then start a node
        Path baseDir = createTempDir();
        Path pluginDir = baseDir.resolve("plugins/dummy-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "dummy desc",
            "name",
            "dummyPlugin",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "test.DummyPlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, pluginDir.resolve("dummy-plugin.jar"));
        }
        internalCluster().startNode(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), baseDir));
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            // Ensure plugins service was able to load the plugin
            assertEquals(1, pluginsService.info().getPluginInfos().stream().filter(info -> info.getName().equals("dummyPlugin")).count());
        }
    }

    public void testNodeBootstrapWithRangeCompatiblePlugin() throws IOException {
        // Prepare the plugins directory and then start a node
        Path baseDir = createTempDir();
        Path pluginDir = baseDir.resolve("plugins/dummy-plugin");
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "dummy desc",
            "name",
            "dummyPlugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"~" + Version.CURRENT + "\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "test.DummyPlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, pluginDir.resolve("dummy-plugin.jar"));
        }
        internalCluster().startNode(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), baseDir));
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            // Ensure plugins service was able to load the plugin
            assertEquals(1, pluginsService.info().getPluginInfos().stream().filter(info -> info.getName().equals("dummyPlugin")).count());
        }
    }

    public void testNodeBootstrapWithInCompatiblePlugin() throws IOException {
        // Prepare the plugins directory with an incompatible plugin and attempt to start a node
        Path baseDir = createTempDir();
        Path pluginDir = baseDir.resolve("plugins/dummy-plugin");
        String incompatibleRange = "~"
            + VersionUtils.getVersion(Version.CURRENT.major, Version.CURRENT.minor, (byte) (Version.CURRENT.revision + 1));
        PluginTestUtil.writePluginProperties(
            pluginDir,
            "description",
            "dummy desc",
            "name",
            "dummyPlugin",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"" + incompatibleRange + "\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "test.DummyPlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, pluginDir.resolve("dummy-plugin.jar"));
        }
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> internalCluster().startNode(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), baseDir))
        );
        assertThat(e.getMessage(), containsString("Plugin [dummyPlugin] was built for OpenSearch version "));
    }
}
