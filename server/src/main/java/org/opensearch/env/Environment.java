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

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The environment of where things exists.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "configures paths for the system")
// TODO: move PathUtils to be package-private here instead of
// public+forbidden api!
public class Environment {

    private static final Path[] EMPTY_PATH_ARRAY = new Path[0];

    public static final Setting<String> PATH_HOME_SETTING = Setting.simpleString("path.home", Property.NodeScope);
    public static final Setting<List<String>> PATH_DATA_SETTING = Setting.listSetting(
        "path.data",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<String> PATH_LOGS_SETTING = new Setting<>("path.logs", "", Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> PATH_REPO_SETTING = Setting.listSetting(
        "path.repo",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<String> PATH_SHARED_DATA_SETTING = Setting.simpleString("path.shared_data", Property.NodeScope);
    public static final Setting<String> PIDFILE_SETTING = Setting.simpleString("pidfile", Property.Deprecated, Property.NodeScope);
    public static final Setting<String> NODE_PIDFILE_SETTING = Setting.simpleString("node.pidfile", PIDFILE_SETTING, Property.NodeScope);

    private final Settings settings;

    private final Path[] dataFiles;

    private final Path[] repoFiles;

    private final Path configDir;

    private final Path pluginsDir;

    private final Path extensionsDir;

    private final Path modulesDir;

    private final Path sharedDataDir;

    /** location of bin/, used by plugin manager */
    private final Path binDir;

    /** location of lib/, */
    private final Path libDir;

    private final Path logsDir;

    /** Path to the PID file (can be null if no PID file is configured) **/
    private final Path pidFile;

    /** Path to the temporary file directory used by the JDK */
    private final Path tmpDir;

    public Environment(final Settings settings, final Path configPath) {
        this(settings, configPath, true);
    }

    public Environment(final Settings settings, final Path configPath, final boolean nodeLocalStorage) {
        this(settings, configPath, nodeLocalStorage, PathUtils.get(System.getProperty("java.io.tmpdir")));
    }

    // Should only be called directly by this class's unit tests
    Environment(final Settings settings, final Path configPath, final boolean nodeLocalStorage, final Path tmpPath) {
        final Path homeFile;
        if (PATH_HOME_SETTING.exists(settings)) {
            homeFile = PathUtils.get(PATH_HOME_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            throw new IllegalStateException(PATH_HOME_SETTING.getKey() + " is not configured");
        }

        if (configPath != null) {
            configDir = configPath.toAbsolutePath().normalize();
        } else {
            configDir = homeFile.resolve("config");
        }

        tmpDir = Objects.requireNonNull(tmpPath);

        pluginsDir = homeFile.resolve("plugins");
        extensionsDir = homeFile.resolve("extensions");

        List<String> dataPaths = PATH_DATA_SETTING.get(settings);
        if (nodeLocalStorage) {
            if (dataPaths.isEmpty() == false) {
                dataFiles = new Path[dataPaths.size()];
                for (int i = 0; i < dataPaths.size(); i++) {
                    dataFiles[i] = PathUtils.get(dataPaths.get(i)).toAbsolutePath().normalize();
                }
            } else {
                dataFiles = new Path[] { homeFile.resolve("data") };
            }
        } else {
            if (dataPaths.isEmpty()) {
                dataFiles = EMPTY_PATH_ARRAY;
            } else {
                final String paths = String.join(",", dataPaths);
                throw new IllegalStateException("node does not require local storage yet path.data is set to [" + paths + "]");
            }
        }
        if (PATH_SHARED_DATA_SETTING.exists(settings)) {
            sharedDataDir = PathUtils.get(PATH_SHARED_DATA_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            sharedDataDir = null;
        }
        List<String> repoPaths = PATH_REPO_SETTING.get(settings);
        if (repoPaths.isEmpty()) {
            repoFiles = EMPTY_PATH_ARRAY;
        } else {
            repoFiles = new Path[repoPaths.size()];
            for (int i = 0; i < repoPaths.size(); i++) {
                repoFiles[i] = PathUtils.get(repoPaths.get(i)).toAbsolutePath().normalize();
            }
        }

        // this is trappy, Setting#get(Settings) will get a fallback setting yet return false for Settings#exists(Settings)
        if (PATH_LOGS_SETTING.exists(settings)) {
            logsDir = PathUtils.get(PATH_LOGS_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            logsDir = homeFile.resolve("logs");
        }

        if (NODE_PIDFILE_SETTING.exists(settings) || PIDFILE_SETTING.exists(settings)) {
            pidFile = PathUtils.get(NODE_PIDFILE_SETTING.get(settings)).toAbsolutePath().normalize();
        } else {
            pidFile = null;
        }

        binDir = homeFile.resolve("bin");
        libDir = homeFile.resolve("lib");
        modulesDir = homeFile.resolve("modules");

        final Settings.Builder finalSettings = Settings.builder().put(settings);
        if (PATH_DATA_SETTING.exists(settings)) {
            finalSettings.putList(PATH_DATA_SETTING.getKey(), Arrays.stream(dataFiles).map(Path::toString).collect(Collectors.toList()));
        }
        finalSettings.put(PATH_HOME_SETTING.getKey(), homeFile);
        finalSettings.put(PATH_LOGS_SETTING.getKey(), logsDir.toString());
        if (PATH_REPO_SETTING.exists(settings)) {
            finalSettings.putList(
                Environment.PATH_REPO_SETTING.getKey(),
                Arrays.stream(repoFiles).map(Path::toString).collect(Collectors.toList())
            );
        }
        if (PATH_SHARED_DATA_SETTING.exists(settings)) {
            assert sharedDataDir != null;
            finalSettings.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataDir.toString());
        }
        if (NODE_PIDFILE_SETTING.exists(settings)) {
            assert pidFile != null;
            finalSettings.put(Environment.NODE_PIDFILE_SETTING.getKey(), pidFile.toString());
        } else if (PIDFILE_SETTING.exists(settings)) {
            assert pidFile != null;
            finalSettings.put(Environment.PIDFILE_SETTING.getKey(), pidFile.toString());
        }
        this.settings = finalSettings.build();
    }

    /**
     * The settings used to build this environment.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * The data location.
     */
    public Path[] dataFiles() {
        return dataFiles;
    }

    /**
     * The shared data location
     */
    public Path sharedDataDir() {
        return sharedDataDir;
    }

    /**
     * The shared filesystem repo locations.
     */
    public Path[] repoFiles() {
        return repoFiles;
    }

    /**
     * Resolves the specified location against the list of configured repository roots
     *
     * If the specified location doesn't match any of the roots, returns null.
     */
    public Path resolveRepoFile(String location) {
        return PathUtils.get(repoFiles, location);
    }

    /**
     * Checks if the specified URL is pointing to the local file system and if it does, resolves the specified url
     * against the list of configured repository roots
     *
     * If the specified url doesn't match any of the roots, returns null.
     */
    public URL resolveRepoURL(URL url) {
        try {
            if ("file".equalsIgnoreCase(url.getProtocol())) {
                if (url.getHost() == null || "".equals(url.getHost())) {
                    // only local file urls are supported
                    Path path = PathUtils.get(repoFiles, url.toURI());
                    if (path == null) {
                        // Couldn't resolve against known repo locations
                        return null;
                    }
                    // Normalize URL
                    return path.toUri().toURL();
                }
                return null;
            } else if ("jar".equals(url.getProtocol())) {
                String file = url.getFile();
                int pos = file.indexOf("!/");
                if (pos < 0) {
                    return null;
                }
                String jarTail = file.substring(pos);
                String filePath = file.substring(0, pos);
                URL internalUrl = new URL(filePath);
                URL normalizedUrl = resolveRepoURL(internalUrl);
                if (normalizedUrl == null) {
                    return null;
                }
                return new URL("jar", "", normalizedUrl.toExternalForm() + jarTail);
            } else {
                // It's not file or jar url and it didn't match the white list - reject
                return null;
            }
        } catch (MalformedURLException ex) {
            // cannot make sense of this file url
            return null;
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    /**
     * The config directory.
     */
    public Path configDir() {
        return configDir;
    }

    public Path pluginsDir() {
        return pluginsDir;
    }

    public Path extensionDir() {
        return extensionsDir;
    }

    public Path binDir() {
        return binDir;
    }

    public Path libDir() {
        return libDir;
    }

    public Path modulesDir() {
        return modulesDir;
    }

    public Path logsDir() {
        return logsDir;
    }

    /**
     * The PID file location (can be null if no PID file is configured)
     */
    public Path pidFile() {
        return pidFile;
    }

    /** Path to the default temp directory used by the JDK */
    public Path tmpDir() {
        return tmpDir;
    }

    /** Ensure the configured temp directory is a valid directory */
    public void validateTmpDir() throws IOException {
        if (Files.exists(tmpDir) == false) {
            throw new FileNotFoundException("Temporary file directory [" + tmpDir + "] does not exist or is not accessible");
        }
        if (Files.isDirectory(tmpDir) == false) {
            throw new IOException("Configured temporary file directory [" + tmpDir + "] is not a directory");
        }
    }

    public static FileStore getFileStore(final Path path) throws IOException {
        return new OpenSearchFileStore(Files.getFileStore(path));
    }

    /**
     * asserts that the two environments are equivalent for all things the environment cares about (i.e., all but the setting
     * object which may contain different setting)
     */
    public static void assertEquivalent(Environment actual, Environment expected) {
        assertEquals(actual.dataFiles(), expected.dataFiles(), "dataFiles");
        assertEquals(actual.repoFiles(), expected.repoFiles(), "repoFiles");
        assertEquals(actual.configDir(), expected.configDir(), "configDir");
        assertEquals(actual.pluginsDir(), expected.pluginsDir(), "pluginsDir");
        assertEquals(actual.binDir(), expected.binDir(), "binDir");
        assertEquals(actual.libDir(), expected.libDir(), "libDir");
        assertEquals(actual.modulesDir(), expected.modulesDir(), "modulesDir");
        assertEquals(actual.logsDir(), expected.logsDir(), "logsDir");
        assertEquals(actual.pidFile(), expected.pidFile(), "pidFile");
        assertEquals(actual.tmpDir(), expected.tmpDir(), "tmpDir");
    }

    private static void assertEquals(Object actual, Object expected, String name) {
        assert Objects.deepEquals(actual, expected) : "actual " + name + " [" + actual + "] is different than [ " + expected + "]";
    }
}
