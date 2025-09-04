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

package org.opensearch.env;

import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

/**
 * Simple unit-tests for Environment.java
 */
public class EnvironmentTests extends OpenSearchTestCase {
    public Environment newEnvironment() {
        return newEnvironment(Settings.EMPTY);
    }

    public Environment newEnvironment(Settings settings) {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .build();
        return new Environment(build, null);
    }

    public void testRepositoryResolution() throws IOException {
        Environment environment = newEnvironment();
        assertThat(environment.resolveRepoFile("/test/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoFile("test/repos/repo1"), nullValue());
        environment = newEnvironment(
            Settings.builder()
                .putList(Environment.PATH_REPO_SETTING.getKey(), "/test/repos", "/another/repos", "/test/repos/../other")
                .build()
        );
        assertThat(environment.resolveRepoFile("/test/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("test/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("/another/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("/test/repos/../repo1"), nullValue());
        assertThat(environment.resolveRepoFile("/test/repos/../repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("/somethingeles/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoFile("/test/other/repo"), notNullValue());

        assertThat(environment.resolveRepoURL(URI.create("file:///test/repos/repo1").toURL()), notNullValue());
        assertThat(environment.resolveRepoURL(URI.create("file:/test/repos/repo1").toURL()), notNullValue());
        assertThat(environment.resolveRepoURL(URI.create("file://test/repos/repo1").toURL()), nullValue());
        assertThat(environment.resolveRepoURL(URI.create("file:///test/repos/../repo1").toURL()), nullValue());
        assertThat(environment.resolveRepoURL(URI.create("http://localhost/test/").toURL()), nullValue());

        assertThat(environment.resolveRepoURL(URI.create("jar:file:///test/repos/repo1!/repo/").toURL()), notNullValue());
        assertThat(environment.resolveRepoURL(URI.create("jar:file:/test/repos/repo1!/repo/").toURL()), notNullValue());
        assertThat(
            environment.resolveRepoURL(URI.create("jar:file:///test/repos/repo1!/repo/").toURL()).toString(),
            endsWith("repo1!/repo/")
        );
        assertThat(environment.resolveRepoURL(URI.create("jar:file:///test/repos/../repo1!/repo/").toURL()), nullValue());
        assertThat(environment.resolveRepoURL(URI.create("jar:http://localhost/test/../repo1?blah!/repo/").toURL()), nullValue());
    }

    public void testPathDataWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.dataFiles(), equalTo(new Path[] { pathHome.resolve("data") }));
    }

    public void testPathDataNotSetInEnvironmentIfNotSet() {
        final Settings settings = Settings.builder().put("path.home", createTempDir().toAbsolutePath()).build();
        assertFalse(Environment.PATH_DATA_SETTING.exists(settings));
        final Environment environment = new Environment(settings, null);
        assertFalse(Environment.PATH_DATA_SETTING.exists(environment.settings()));
    }

    public void testPathLogsWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.logsDir(), equalTo(pathHome.resolve("logs")));
    }

    public void testDefaultConfigPath() {
        final Path path = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", path).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.configDir(), equalTo(path.resolve("config")));
    }

    public void testConfigPath() {
        final Path configPath = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", createTempDir().toAbsolutePath()).build();
        final Environment environment = new Environment(settings, configPath);
        assertThat(environment.configDir(), equalTo(configPath));
    }

    public void testConfigPathWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.configDir(), equalTo(pathHome.resolve("config")));
    }

    public void testNodeDoesNotRequireLocalStorage() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).put("node.master", false).put("node.data", false).build();
        final Environment environment = new Environment(settings, null, false);
        assertThat(environment.dataFiles(), arrayWithSize(0));
    }

    public void testNodeDoesNotRequireLocalStorageButHasPathData() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Path pathData = pathHome.resolve("data");
        final Settings settings = Settings.builder()
            .put("path.home", pathHome)
            .put("path.data", pathData)
            .put("node.master", false)
            .put("node.data", false)
            .build();
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> new Environment(settings, null, false));
        assertThat(e, hasToString(containsString("node does not require local storage yet path.data is set to [" + pathData + "]")));
    }

    public void testNonExistentTempPathValidation() {
        Settings build = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(build, null, true, createTempDir().resolve("this_does_not_exist"));
        FileNotFoundException e = expectThrows(FileNotFoundException.class, environment::validateTmpDir);
        assertThat(e.getMessage(), startsWith("Temporary file directory ["));
        assertThat(e.getMessage(), endsWith("this_does_not_exist] does not exist or is not accessible"));
    }

    public void testTempPathValidationWhenRegularFile() throws IOException {
        Settings build = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(build, null, true, createTempFile("something", ".test"));
        IOException e = expectThrows(IOException.class, environment::validateTmpDir);
        assertThat(e.getMessage(), startsWith("Configured temporary file directory ["));
        assertThat(e.getMessage(), endsWith(".test] is not a directory"));
    }

    // test that environment paths are absolute and normalized
    public void testPathNormalization() throws IOException {
        final Setting<String> pidFileSetting;
        if (randomBoolean()) {
            pidFileSetting = Environment.NODE_PIDFILE_SETTING;
        } else {
            pidFileSetting = Environment.PIDFILE_SETTING;
        }

        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), "home")
            .put(Environment.PATH_DATA_SETTING.getKey(), "./home/../home/data")
            .put(Environment.PATH_LOGS_SETTING.getKey(), "./home/../home/logs")
            .put(Environment.PATH_REPO_SETTING.getKey(), "./home/../home/repo")
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), "./home/../home/shared_data")
            .put(pidFileSetting.getKey(), "./home/../home/pidfile")
            .build();

        // the above paths will be treated as relative to the working directory
        final Path workingDirectory = PathUtils.get(System.getProperty("user.dir"));

        final Environment environment = new Environment(settings, null, true, createTempDir());
        final String homePath = Environment.PATH_HOME_SETTING.get(environment.settings());
        assertPath(homePath, workingDirectory.resolve("home"));

        final Path home = PathUtils.get(homePath);

        final List<String> dataPaths = Environment.PATH_DATA_SETTING.get(environment.settings());
        assertThat(dataPaths, hasSize(1));
        assertPath(dataPaths.get(0), home.resolve("data"));

        final String logPath = Environment.PATH_LOGS_SETTING.get(environment.settings());
        assertPath(logPath, home.resolve("logs"));

        final List<String> repoPaths = Environment.PATH_REPO_SETTING.get(environment.settings());
        assertThat(repoPaths, hasSize(1));
        assertPath(repoPaths.get(0), home.resolve("repo"));

        final String sharedDataPath = Environment.PATH_SHARED_DATA_SETTING.get(environment.settings());
        assertPath(sharedDataPath, home.resolve("shared_data"));

        final String pidFile = pidFileSetting.get(environment.settings());
        assertPath(pidFile, home.resolve("pidfile"));

        if (pidFileSetting.isDeprecated()) {
            assertSettingDeprecationsAndWarnings(new Setting<?>[] { pidFileSetting });
        }
    }

    private void assertPath(final String actual, final Path expected) {
        assertIsAbsolute(actual);
        assertIsNormalized(actual);
        assertThat(PathUtils.get(actual), equalTo(expected));
    }

    private void assertIsAbsolute(final String path) {
        assertTrue("path [" + path + "] is not absolute", PathUtils.get(path).isAbsolute());
    }

    private void assertIsNormalized(final String path) {
        assertThat("path [" + path + "] is not normalized", PathUtils.get(path), equalTo(PathUtils.get(path).normalize()));
    }

}
