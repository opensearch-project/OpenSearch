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
import org.opensearch.Version;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.MockTerminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.plugins.PluginTestUtil;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

@LuceneTestCase.SuppressFileSystems("*")
public class RemovePluginCommandTests extends OpenSearchTestCase {

    private Path home;
    private Environment env;

    static class MockRemovePluginCommand extends RemovePluginCommand {

        final Environment env;

        private MockRemovePluginCommand(final Environment env) {
            this.env = env;
        }

        @Override
        protected Environment createEnv(Map<String, String> settings) throws UserException {
            return env;
        }

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Files.createDirectories(home.resolve("bin"));
        Files.createFile(home.resolve("bin").resolve("opensearch"));
        Files.createDirectories(home.resolve("plugins"));
        Settings settings = Settings.builder().put("path.home", home).build();
        env = TestEnvironment.newEnvironment(settings);
    }

    void createPlugin(String name, String... additionalProps) throws IOException {
        createPlugin(env.pluginsDir(), name, Version.CURRENT, additionalProps);
    }

    void createPlugin(String name, Version version) throws IOException {
        createPlugin(env.pluginsDir(), name, version);
    }

    void createPlugin(Path path, String name, Version version, String... additionalProps) throws IOException {
        String[] properties = Stream.concat(
            Stream.of(
                "description",
                "dummy",
                "name",
                name,
                "version",
                "1.0",
                "opensearch.version",
                version.toString(),
                "java.version",
                System.getProperty("java.specification.version"),
                "classname",
                "SomeClass"
            ),
            Arrays.stream(additionalProps)
        ).toArray(String[]::new);
        String pluginFolderName = additionalProps.length == 0 ? name : additionalProps[1];
        PluginTestUtil.writePluginProperties(path.resolve(pluginFolderName), properties);
    }

    static MockTerminal removePlugin(String name, Path home, boolean purge) throws Exception {
        Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
        MockTerminal terminal = new MockTerminal();
        new MockRemovePluginCommand(env).execute(terminal, env, name, purge);
        return terminal;
    }

    static void assertRemoveCleaned(Environment env) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(env.pluginsDir())) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".removing")) {
                    fail("Removal dir still exists, " + file);
                }
            }
        }
    }

    public void testMissing() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin("dne", home, randomBoolean()));
        assertTrue(e.getMessage(), e.getMessage().contains("plugin [dne] not found"));
        assertRemoveCleaned(env);
    }

    public void testBasic() throws Exception {
        createPlugin("fake");
        Files.createFile(env.pluginsDir().resolve("fake").resolve("plugin.jar"));
        Files.createDirectory(env.pluginsDir().resolve("fake").resolve("subdir"));
        createPlugin("other");
        removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(env.pluginsDir().resolve("fake")));
        assertTrue(Files.exists(env.pluginsDir().resolve("other")));
        assertRemoveCleaned(env);
    }

    public void testRemovePluginWithCustomFolderName() throws Exception {
        createPlugin("fake", "custom.foldername", "custom-folder");
        Files.createFile(env.pluginsDir().resolve("custom-folder").resolve("plugin.jar"));
        Files.createDirectory(env.pluginsDir().resolve("custom-folder").resolve("subdir"));
        createPlugin("other");
        removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(env.pluginsDir().resolve("custom-folder")));
        assertTrue(Files.exists(env.pluginsDir().resolve("other")));
        assertRemoveCleaned(env);
    }

    public void testRemoveOldVersion() throws Exception {
        createPlugin(
            "fake",
            VersionUtils.randomVersionBetween(
                random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(),
                VersionUtils.getPreviousVersion()
            )
        );
        removePlugin("fake", home, randomBoolean());
        assertThat(Files.exists(env.pluginsDir().resolve("fake")), equalTo(false));
        assertRemoveCleaned(env);
    }

    public void testBin() throws Exception {
        createPlugin("fake");
        Path binDir = env.binDir().resolve("fake");
        Files.createDirectories(binDir);
        Files.createFile(binDir.resolve("somescript"));
        removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(env.pluginsDir().resolve("fake")));
        assertTrue(Files.exists(env.binDir().resolve("opensearch")));
        assertFalse(Files.exists(binDir));
        assertRemoveCleaned(env);
    }

    public void testBinNotDir() throws Exception {
        createPlugin("fake");
        Files.createFile(env.binDir().resolve("fake"));
        UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home, randomBoolean()));
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertTrue(Files.exists(env.pluginsDir().resolve("fake"))); // did not remove
        assertTrue(Files.exists(env.binDir().resolve("fake")));
        assertRemoveCleaned(env);
    }

    public void testConfigDirPreserved() throws Exception {
        createPlugin("fake");
        final Path configDir = env.configDir().resolve("fake");
        Files.createDirectories(configDir);
        Files.createFile(configDir.resolve("fake.yml"));
        final MockTerminal terminal = removePlugin("fake", home, false);
        assertTrue(Files.exists(env.configDir().resolve("fake")));
        assertThat(terminal.getOutput(), containsString(expectedConfigDirPreservedMessage(configDir)));
        assertRemoveCleaned(env);
    }

    public void testPurgePluginExists() throws Exception {
        createPlugin("fake");
        final Path configDir = env.configDir().resolve("fake");
        if (randomBoolean()) {
            Files.createDirectories(configDir);
            Files.createFile(configDir.resolve("fake.yml"));
        }
        final MockTerminal terminal = removePlugin("fake", home, true);
        assertFalse(Files.exists(env.configDir().resolve("fake")));
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
        assertRemoveCleaned(env);
    }

    public void testPurgePluginDoesNotExist() throws Exception {
        final Path configDir = env.configDir().resolve("fake");
        Files.createDirectories(configDir);
        Files.createFile(configDir.resolve("fake.yml"));
        final MockTerminal terminal = removePlugin("fake", home, true);
        assertFalse(Files.exists(env.configDir().resolve("fake")));
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
        assertRemoveCleaned(env);
    }

    public void testPurgeNothingExists() throws Exception {
        final UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home, true));
        assertThat(e, hasToString(containsString("plugin [fake] not found")));
    }

    public void testPurgeOnlyMarkerFileExists() throws Exception {
        final Path configDir = env.configDir().resolve("fake");
        final Path removing = env.pluginsDir().resolve(".removing-fake");
        Files.createFile(removing);
        final MockTerminal terminal = removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(removing));
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
    }

    public void testNoConfigDirPreserved() throws Exception {
        createPlugin("fake");
        final Path configDir = env.configDir().resolve("fake");
        final MockTerminal terminal = removePlugin("fake", home, randomBoolean());
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
    }

    public void testRemoveUninstalledPluginErrors() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home, randomBoolean()));
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertEquals("plugin [fake] not found; run 'opensearch-plugin list' to get list of installed plugins", e.getMessage());

        MockTerminal terminal = new MockTerminal();

        new MockRemovePluginCommand(env) {
            protected boolean addShutdownHook() {
                return false;
            }
        }.main(new String[] { "-Epath.home=" + home, "fake" }, terminal);
        try (
            BufferedReader reader = new BufferedReader(new StringReader(terminal.getOutput()));
            BufferedReader errorReader = new BufferedReader(new StringReader(terminal.getErrorOutput()))
        ) {
            assertEquals("searching in other folders to find if plugin exists with custom folder name", reader.readLine());
            assertEquals("-> removing [fake]...", reader.readLine());
            assertEquals(
                "ERROR: plugin [fake] not found; run 'opensearch-plugin list' to get list of installed plugins",
                errorReader.readLine()
            );
            assertNull(reader.readLine());
            assertNull(errorReader.readLine());
        }
    }

    public void testMissingPluginName() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin(null, home, randomBoolean()));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertEquals("plugin name is required", e.getMessage());
    }

    public void testRemoveWhenRemovingMarker() throws Exception {
        createPlugin("fake");
        Files.createFile(env.pluginsDir().resolve("fake").resolve("plugin.jar"));
        Files.createFile(env.pluginsDir().resolve(".removing-fake"));
        removePlugin("fake", home, randomBoolean());
    }

    private String expectedConfigDirPreservedMessage(final Path configDir) {
        return "-> preserving plugin config files [" + configDir + "] in case of upgrade; use --purge if not needed";
    }

}
