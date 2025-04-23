/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.tools.cli.plugin;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.MockTerminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginTestUtil;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@LuceneTestCase.SuppressFileSystems("*")
public class ListPluginsCommandTests extends OpenSearchTestCase {

    private Path home;
    private Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Files.createDirectories(home.resolve("plugins"));
        Settings settings = Settings.builder().put("path.home", home).build();
        env = TestEnvironment.newEnvironment(settings);
    }

    static MockTerminal listPlugins(Path home) throws Exception {
        return listPlugins(home, new String[0]);
    }

    static MockTerminal listPlugins(Path home, String[] args) throws Exception {
        String[] argsAndHome = new String[args.length + 1];
        System.arraycopy(args, 0, argsAndHome, 0, args.length);
        argsAndHome[args.length] = "-Epath.home=" + home;
        MockTerminal terminal = new MockTerminal();
        int status = new ListPluginsCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                Settings.Builder builder = Settings.builder().put("path.home", home);
                settings.forEach((k, v) -> builder.put(k, v));
                final Settings realSettings = builder.build();
                return new Environment(realSettings, home.resolve("config"));
            }

            @Override
            protected boolean addShutdownHook() {
                return false;
            }
        }.main(argsAndHome, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }

    private static String buildMultiline(String... args) {
        return Arrays.stream(args).collect(Collectors.joining("\n", "", "\n"));
    }

    private static void buildFakePlugin(final Environment env, final String description, final String name, final String classname)
        throws IOException {
        buildFakePlugin(env, description, name, classname, false);
    }

    private static void buildFakePlugin(
        final Environment env,
        final String description,
        final String name,
        final String classname,
        final boolean hasNativeController
    ) throws IOException {
        PluginTestUtil.writePluginProperties(
            env.pluginsDir().resolve(name),
            "description",
            description,
            "name",
            name,
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            "1.8",
            "classname",
            classname,
            "custom.foldername",
            "custom-folder",
            "has.native.controller",
            Boolean.toString(hasNativeController)
        );
    }

    public void testPluginsDirMissing() throws Exception {
        Files.delete(env.pluginsDir());
        IOException e = expectThrows(IOException.class, () -> listPlugins(home));
        assertEquals("Plugins directory missing: " + env.pluginsDir(), e.getMessage());
    }

    public void testNoPlugins() throws Exception {
        MockTerminal terminal = listPlugins(home);
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testOnePlugin() throws Exception {
        buildFakePlugin(env, "fake desc", "fake", "org.fake");
        MockTerminal terminal = listPlugins(home);
        assertEquals(buildMultiline("fake"), terminal.getOutput());
    }

    public void testTwoPlugins() throws Exception {
        buildFakePlugin(env, "fake desc", "fake1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake2", "org.fake");
        MockTerminal terminal = listPlugins(home);
        assertEquals(buildMultiline("fake1", "fake2"), terminal.getOutput());
    }

    public void testPluginWithVerbose() throws Exception {
        buildFakePlugin(env, "fake desc", "fake_plugin", "org.fake");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsDir(),
                "fake_plugin",
                "- Plugin information:",
                "Name: fake_plugin",
                "Description: fake desc",
                "Version: 1.0",
                "OpenSearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: false",
                "Extended Plugins: []",
                " * Classname: org.fake",
                "Folder name: custom-folder"
            ),
            terminal.getOutput()
        );
    }

    public void testPluginWithNativeController() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake", true);
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsDir(),
                "fake_plugin1",
                "- Plugin information:",
                "Name: fake_plugin1",
                "Description: fake desc 1",
                "Version: 1.0",
                "OpenSearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: true",
                "Extended Plugins: []",
                " * Classname: org.fake",
                "Folder name: custom-folder"
            ),
            terminal.getOutput()
        );
    }

    public void testPluginWithVerboseMultiplePlugins() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsDir(),
                "fake_plugin1",
                "- Plugin information:",
                "Name: fake_plugin1",
                "Description: fake desc 1",
                "Version: 1.0",
                "OpenSearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: false",
                "Extended Plugins: []",
                " * Classname: org.fake",
                "Folder name: custom-folder",
                "fake_plugin2",
                "- Plugin information:",
                "Name: fake_plugin2",
                "Description: fake desc 2",
                "Version: 1.0",
                "OpenSearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: false",
                "Extended Plugins: []",
                " * Classname: org.fake2",
                "Folder name: custom-folder"
            ),
            terminal.getOutput()
        );
    }

    public void testPluginWithoutVerboseMultiplePlugins() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        MockTerminal terminal = listPlugins(home, new String[0]);
        String output = terminal.getOutput();
        assertEquals(buildMultiline("fake_plugin1", "fake_plugin2"), output);
    }

    public void testPluginWithoutDescriptorFile() throws Exception {
        final Path pluginDir = env.pluginsDir().resolve("fake1");
        Files.createDirectories(pluginDir);
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> listPlugins(home));
        assertEquals(pluginDir.resolve(PluginInfo.OPENSEARCH_PLUGIN_PROPERTIES).toString(), e.getFile());
    }

    public void testPluginWithWrongDescriptorFile() throws Exception {
        final Path pluginDir = env.pluginsDir().resolve("fake1");
        PluginTestUtil.writePluginProperties(pluginDir, "description", "fake desc");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> listPlugins(home));
        final Path descriptorPath = pluginDir.resolve(PluginInfo.OPENSEARCH_PLUGIN_PROPERTIES);
        assertEquals("property [name] is missing in [" + descriptorPath.toString() + "]", e.getMessage());
    }

    public void testExistingIncompatiblePlugin() throws Exception {
        PluginTestUtil.writePluginProperties(
            env.pluginsDir().resolve("fake_plugin1"),
            "description",
            "fake desc 1",
            "name",
            "fake_plugin1",
            "version",
            "1.0",
            "opensearch.version",
            LegacyESVersion.fromString("5.0.0").toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "org.fake1"
        );
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");

        MockTerminal terminal = listPlugins(home);
        String message = "plugin [fake_plugin1] was built for OpenSearch version 5.0.0 and is not compatible with " + Version.CURRENT;
        assertEquals("fake_plugin1\nfake_plugin2\n", terminal.getOutput());
        assertEquals("WARNING: " + message + "\n", terminal.getErrorOutput());

        String[] params = { "-s" };
        terminal = listPlugins(home, params);
        assertEquals("fake_plugin1\nfake_plugin2\n", terminal.getOutput());
    }

    public void testPluginWithDependencies() throws Exception {
        PluginTestUtil.writePluginProperties(
            env.pluginsDir().resolve("fake_plugin1"),
            "description",
            "fake desc 1",
            "name",
            "fake_plugin1",
            "version",
            "1.0",
            "dependencies",
            "{opensearch:\"" + Version.CURRENT + "\"}",
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "org.fake1"
        );
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsDir(),
                "fake_plugin1",
                "- Plugin information:",
                "Name: fake_plugin1",
                "Description: fake desc 1",
                "Version: 1.0",
                "OpenSearch Version: " + Version.CURRENT.toString(),
                "Java Version: " + System.getProperty("java.specification.version"),
                "Native Controller: false",
                "Extended Plugins: []",
                " * Classname: org.fake1",
                "Folder name: null"
            ),
            terminal.getOutput()
        );
    }
}
