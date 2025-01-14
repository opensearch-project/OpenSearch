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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.bootstrap;

import org.opensearch.cli.Command;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;
import org.opensearch.secure_sm.SecureSM;
import org.opensearch.transport.TcpTransport;

import java.io.IOException;
import java.net.SocketPermission;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.Permissions;
import java.security.Policy;
import java.security.URIParameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.bootstrap.FilePermissionUtils.addDirectoryPath;
import static org.opensearch.bootstrap.FilePermissionUtils.addSingleFilePath;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_PORT_DEFAULTS;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_PORTS;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_SETTING;

/**
 * Initializes SecurityManager with necessary permissions.
 * <br>
 * <h2>Initialization</h2>
 * The JVM is not initially started with security manager enabled,
 * instead we turn it on early in the startup process. This is a tradeoff
 * between security and ease of use:
 * <ul>
 *   <li>Assigns file permissions to user-configurable paths that can
 *       be specified from the command-line or {@code opensearch.yml}.</li>
 *   <li>Allows for some contained usage of native code that would not
 *       otherwise be permitted.</li>
 * </ul>
 * <br>
 * <h2>Permissions</h2>
 * Permissions use a policy file packaged as a resource, this file is
 * also used in tests. File permissions are generated dynamically and
 * combined with this policy file.
 * <p>
 * For each configured path, we ensure it exists and is accessible before
 * granting permissions, otherwise directory creation would require
 * permissions to parent directories.
 * <p>
 * In some exceptional cases, permissions are assigned to specific jars only,
 * when they are so dangerous that general code should not be granted the
 * permission, but there are extenuating circumstances.
 * <p>
 * Scripts (groovy) are assigned minimal permissions. This does not provide adequate
 * sandboxing, as these scripts still have access to OpenSearch classes, and could
 * modify members, etc that would cause bad things to happen later on their
 * behalf (no package protections are yet in place, this would need some
 * cleanups to the scripting apis). But still it can provide some defense for users
 * that enable dynamic scripting without being fully aware of the consequences.
 * <br>
 * <h2>Debugging Security</h2>
 * A good place to start when there is a problem is to turn on security debugging:
 * <pre>
 * OPENSEARCH_JAVA_OPTS="-Djava.security.debug=access,failure" bin/opensearch
 * </pre>
 * <p>
 * When running tests you have to pass it to the test runner like this:
 * <pre>
 * gradle test -Dtests.jvm.argline="-Djava.security.debug=access,failure" ...
 * </pre>
 * See <a href="https://docs.oracle.com/javase/7/docs/technotes/guides/security/troubleshooting-security.html">
 * Troubleshooting Security</a> for information.
 *
 * @opensearch.internal
 */
@SuppressWarnings("removal")
final class Security {
    private static final Pattern CODEBASE_JAR_WITH_CLASSIFIER = Pattern.compile("^(.+)-\\d+\\.\\d+[^-]*.*?[-]?([^-]+)?\\.jar$");

    /** no instantiation */
    private Security() {}

    /**
     * Initializes SecurityManager for the environment
     * Can only happen once!
     * @param environment configuration for generating dynamic permissions
     * @param filterBadDefaults true if we should filter out bad java defaults in the system policy.
     */
    static void configure(Environment environment, boolean filterBadDefaults) throws IOException, NoSuchAlgorithmException {

        // enable security policy: union of template and environment-based paths, and possibly plugin permissions
        Map<String, URL> codebases = getCodebaseJarMap(JarHell.parseClassPath());
        Policy.setPolicy(
            new OpenSearchPolicy(
                codebases,
                createPermissions(environment),
                getPluginPermissions(environment),
                filterBadDefaults,
                createRecursiveDataPathPermission(environment)
            )
        );

        // enable security manager
        final String[] classesThatCanExit = new String[] {
            // SecureSM matches class names as regular expressions so we escape the $ that arises from the nested class name
            OpenSearchUncaughtExceptionHandler.PrivilegedHaltAction.class.getName().replace("$", "\\$"),
            Command.class.getName() };
        System.setSecurityManager(new SecureSM(classesThatCanExit));

        // do some basic tests
        selfTest();
    }

    /**
     * Return a map from codebase name to codebase url of jar codebases used by OpenSearch core.
     */
    @SuppressForbidden(reason = "find URL path")
    static Map<String, URL> getCodebaseJarMap(Set<URL> urls) {
        Map<String, URL> codebases = new LinkedHashMap<>(); // maintain order
        for (URL url : urls) {
            try {
                String fileName = PathUtils.get(url.toURI()).getFileName().toString();
                if (fileName.endsWith(".jar") == false) {
                    // tests :(
                    continue;
                }
                codebases.put(fileName, url);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return codebases;
    }

    /**
     * Sets properties (codebase URLs) for policy files.
     * we look for matching plugins and set URLs to fit
     */
    @SuppressForbidden(reason = "proper use of URL")
    static Map<String, Policy> getPluginPermissions(Environment environment) throws IOException {
        Map<String, Policy> map = new HashMap<>();
        // collect up set of plugins and modules by listing directories.
        Set<Path> pluginsAndModules = new LinkedHashSet<>(PluginsService.findPluginDirs(environment.pluginsDir()));
        pluginsAndModules.addAll(PluginsService.findPluginDirs(environment.modulesDir()));

        // now process each one
        for (Path plugin : pluginsAndModules) {
            Path policyFile = plugin.resolve(PluginInfo.OPENSEARCH_PLUGIN_POLICY);
            if (Files.exists(policyFile)) {
                // first get a list of URLs for the plugins' jars:
                // we resolve symlinks so map is keyed on the normalize codebase name
                Set<URL> codebases = new LinkedHashSet<>(); // order is already lost, but some filesystems have it
                try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(plugin, "*.jar")) {
                    for (Path jar : jarStream) {
                        URL url = jar.toRealPath().toUri().toURL();
                        if (codebases.add(url) == false) {
                            throw new IllegalStateException("duplicate module/plugin: " + url);
                        }
                    }
                }

                // parse the plugin's policy file into a set of permissions
                Policy policy = readPolicy(policyFile.toUri().toURL(), getCodebaseJarMap(codebases));

                // consult this policy for each of the plugin's jars:
                for (URL url : codebases) {
                    if (map.put(url.getFile(), policy) != null) {
                        // just be paranoid ok?
                        throw new IllegalStateException("per-plugin permissions already granted for jar file: " + url);
                    }
                }
            }
        }

        return Collections.unmodifiableMap(map);
    }

    /**
     * Reads and returns the specified {@code policyFile}.
     * <p>
     * Jar files listed in {@code codebases} location will be provided to the policy file via
     * a system property of the short name: e.g. <code>${codebase.joda-convert-1.2.jar}</code>
     * would map to full URL.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Policy readPolicy(URL policyFile, Map<String, URL> codebases) {
        try {
            List<String> propertiesSet = new ArrayList<>();
            try {
                final Map<Map.Entry<String, URL>, String> jarsWithPossibleClassifiers = new HashMap<>();
                // set codebase properties
                for (Map.Entry<String, URL> codebase : codebases.entrySet()) {
                    final String name = codebase.getKey();
                    final URL url = codebase.getValue();

                    // We attempt to use a versionless identifier for each codebase. This assumes a specific version
                    // format in the jar filename. While we cannot ensure all jars in all plugins use this format, nonconformity
                    // only means policy grants would need to include the entire jar filename as they always have before.
                    final Matcher matcher = CODEBASE_JAR_WITH_CLASSIFIER.matcher(name);
                    if (matcher.matches() && matcher.group(2) != null) {
                        // There is a JAR that, possibly, has a classifier or SNAPSHOT at the end, examples are:
                        // - netty-tcnative-boringssl-static-2.0.61.Final-linux-x86_64.jar
                        // - kafka-server-common-3.6.1-test.jar
                        // - lucene-core-9.11.0-snapshot-8a555eb.jar
                        // - zstd-jni-1.5.5-5.jar
                        jarsWithPossibleClassifiers.put(codebase, matcher.group(2));
                    } else {
                        String property = "codebase." + name;
                        String aliasProperty = "codebase." + name.replaceFirst("-\\d+\\.\\d+.*\\.jar", "");
                        addCodebaseToSystemProperties(propertiesSet, url, property, aliasProperty);
                    }
                }

                // set codebase properties for JARs that might present with classifiers
                for (Map.Entry<Map.Entry<String, URL>, String> jarWithPossibleClassifier : jarsWithPossibleClassifiers.entrySet()) {
                    final Map.Entry<String, URL> codebase = jarWithPossibleClassifier.getKey();
                    final String name = codebase.getKey();
                    final URL url = codebase.getValue();

                    String property = "codebase." + name;
                    String aliasProperty = "codebase." + name.replaceFirst("-\\d+\\.\\d+.*\\.jar", "");
                    if (System.getProperties().containsKey(aliasProperty)) {
                        aliasProperty = aliasProperty + "@" + jarWithPossibleClassifier.getValue();
                    }

                    addCodebaseToSystemProperties(propertiesSet, url, property, aliasProperty);
                }

                return Policy.getInstance("JavaPolicy", new URIParameter(policyFile.toURI()));
            } finally {
                // clear codebase properties
                for (String property : propertiesSet) {
                    System.clearProperty(property);
                }
            }
        } catch (NoSuchAlgorithmException | URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse policy file `" + policyFile + "`", e);
        }
    }

    /** adds the codebase to properties and System properties */
    @SuppressForbidden(reason = "accesses System properties to configure codebases")
    private static void addCodebaseToSystemProperties(List<String> propertiesSet, final URL url, String property, String aliasProperty) {
        if (aliasProperty.equals(property) == false) {
            propertiesSet.add(aliasProperty);
            String previous = System.setProperty(aliasProperty, url.toString());
            if (previous != null) {
                throw new IllegalStateException(
                    "codebase property already set: " + aliasProperty + " -> " + previous + ", cannot set to " + url.toString()
                );
            }
        }
        propertiesSet.add(property);
        String previous = System.setProperty(property, url.toString());
        if (previous != null) {
            throw new IllegalStateException(
                "codebase property already set: " + property + " -> " + previous + ", cannot set to " + url.toString()
            );
        }
    }

    /** returns dynamic Permissions to configured paths and bind ports */
    static Permissions createPermissions(Environment environment) throws IOException {
        Permissions policy = new Permissions();
        addClasspathPermissions(policy);
        addFilePermissions(policy, environment);
        addBindPermissions(policy, environment.settings());
        return policy;
    }

    private static Permissions createRecursiveDataPathPermission(Environment environment) throws IOException {
        Permissions policy = new Permissions();
        for (Path path : environment.dataFiles()) {
            addDirectoryPath(policy, Environment.PATH_DATA_SETTING.getKey(), path, "read,readlink,write,delete", true);
        }
        return policy;
    }

    /** Adds access to classpath jars/classes for jar hell scan, etc */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static void addClasspathPermissions(Permissions policy) throws IOException {
        // add permissions to everything in classpath
        // really it should be covered by lib/, but there could be e.g. agents or similar configured)
        for (URL url : JarHell.parseClassPath()) {
            Path path;
            try {
                path = PathUtils.get(url.toURI());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            // resource itself
            if (Files.isDirectory(path)) {
                addDirectoryPath(policy, "class.path", path, "read,readlink", false);
            } else {
                addSingleFilePath(policy, path, "read,readlink");
            }
        }
    }

    /**
     * Adds access to all configurable paths.
     */
    static void addFilePermissions(Permissions policy, Environment environment) throws IOException {
        // read-only dirs
        addDirectoryPath(policy, Environment.PATH_HOME_SETTING.getKey(), environment.binDir(), "read,readlink", false);
        addDirectoryPath(policy, Environment.PATH_HOME_SETTING.getKey(), environment.libDir(), "read,readlink", false);
        addDirectoryPath(policy, Environment.PATH_HOME_SETTING.getKey(), environment.modulesDir(), "read,readlink", false);
        addDirectoryPath(policy, Environment.PATH_HOME_SETTING.getKey(), environment.pluginsDir(), "read,readlink", false);
        addDirectoryPath(policy, "path.conf'", environment.configDir(), "read,readlink", false);
        // read-write dirs
        addDirectoryPath(policy, "java.io.tmpdir", environment.tmpDir(), "read,readlink,write,delete", false);
        addDirectoryPath(policy, Environment.PATH_LOGS_SETTING.getKey(), environment.logsDir(), "read,readlink,write,delete", false);
        if (environment.sharedDataDir() != null) {
            addDirectoryPath(
                policy,
                Environment.PATH_SHARED_DATA_SETTING.getKey(),
                environment.sharedDataDir(),
                "read,readlink,write,delete",
                false
            );
        }
        final Set<Path> dataFilesPaths = new HashSet<>();
        for (Path path : environment.dataFiles()) {
            addDirectoryPath(policy, Environment.PATH_DATA_SETTING.getKey(), path, "read,readlink,write,delete", false);
            /*
             * We have to do this after adding the path because a side effect of that is that the directory is created; the Path#toRealPath
             * invocation will fail if the directory does not already exist. We use Path#toRealPath to follow symlinks and handle issues
             * like unicode normalization or case-insensitivity on some filesystems (e.g., the case-insensitive variant of HFS+ on macOS).
             */
            try {
                final Path realPath = path.toRealPath();
                if (!dataFilesPaths.add(realPath)) {
                    throw new IllegalStateException("path [" + realPath + "] is duplicated by [" + path + "]");
                }
            } catch (final IOException e) {
                throw new IllegalStateException("unable to access [" + path + "]", e);
            }
        }
        for (Path path : environment.repoFiles()) {
            addDirectoryPath(policy, Environment.PATH_REPO_SETTING.getKey(), path, "read,readlink,write,delete", false);
        }
        if (environment.pidFile() != null) {
            // we just need permission to remove the file if its elsewhere.
            addSingleFilePath(policy, environment.pidFile(), "delete");
        }
    }

    /**
     * Add dynamic {@link SocketPermission}s based on HTTP and transport settings.
     *
     * @param policy the {@link Permissions} instance to apply the dynamic {@link SocketPermission}s to.
     * @param settings the {@link Settings} instance to read the HTTP and transport settings from
     */
    private static void addBindPermissions(Permissions policy, Settings settings) {
        addSocketPermissionForHttp(policy, settings);
        addSocketPermissionForTransportProfiles(policy, settings);
        addSocketPermissionForAux(policy, settings);
    }

    /**
     * Add dynamic {@link SocketPermission} based on HTTP settings.
     *
     * @param policy the {@link Permissions} instance to apply the dynamic {@link SocketPermission}s to.
     * @param settings the {@link Settings} instance to read the HTTP settings from
     */
    private static void addSocketPermissionForHttp(final Permissions policy, final Settings settings) {
        // http is simple
        final String httpRange = HttpTransportSettings.SETTING_HTTP_PORT.get(settings).getPortRangeString();
        addSocketPermissionForPortRange(policy, httpRange);
    }

    /**
     * Add dynamic {@link SocketPermission} based on AffixSetting AUX_TRANSPORT_PORTS.
     * If an auxiliary transport type is enabled but has no corresponding port range setting fall back to AUX_PORT_DEFAULTS.
     *
     * @param policy the {@link Permissions} instance to apply the dynamic {@link SocketPermission}s to.
     * @param settings the {@link Settings} instance to read the gRPC settings from
     */
    private static void addSocketPermissionForAux(final Permissions policy, final Settings settings) {
        Set<PortsRange> portsRanges = new HashSet<>();
        for (String auxType : AUX_TRANSPORT_TYPES_SETTING.get(settings)) {
            Setting<PortsRange> auxTypePortSettings = AUX_TRANSPORT_PORTS.getConcreteSettingForNamespace(auxType);
            if (auxTypePortSettings.exists(settings)) {
                portsRanges.add(auxTypePortSettings.get(settings));
            } else {
                portsRanges.add(new PortsRange(AUX_PORT_DEFAULTS));
            }
        }

        for (PortsRange portRange : portsRanges) {
            addSocketPermissionForPortRange(policy, portRange.getPortRangeString());
        }
    }

    /**
     * Add dynamic {@link SocketPermission} based on transport settings. This method will first check if there is a port range specified in
     * the transport profile specified by {@code profileSettings} and will fall back to {@code settings}.
     *
     * @param policy          the {@link Permissions} instance to apply the dynamic {@link SocketPermission}s to
     * @param settings        the {@link Settings} instance to read the transport settings from
     */
    private static void addSocketPermissionForTransportProfiles(final Permissions policy, final Settings settings) {
        // transport is way over-engineered
        Set<TcpTransport.ProfileSettings> profiles = TcpTransport.getProfileSettings(settings);
        Set<String> uniquePortRanges = new HashSet<>();
        // loop through all profiles and add permissions for each one
        for (final TcpTransport.ProfileSettings profile : profiles) {
            if (uniquePortRanges.add(profile.portOrRange)) {
                // profiles fall back to the transport.port if it's not explicit but we want to only add one permission per range
                addSocketPermissionForPortRange(policy, profile.portOrRange);
            }
        }
    }

    /**
     * Add dynamic {@link SocketPermission} for the specified port range.
     *
     * @param policy the {@link Permissions} instance to apply the dynamic {@link SocketPermission} to.
     * @param portRange the port range
     */
    private static void addSocketPermissionForPortRange(final Permissions policy, final String portRange) {
        // listen is always called with 'localhost' but use wildcard to be sure, no name service is consulted.
        // see SocketPermission implies() code
        policy.add(new SocketPermission("*:" + portRange, "listen,resolve"));
    }

    /**
     * Ensures configured directory {@code path} exists.
     * @throws IOException if {@code path} exists, but is not a directory, not accessible, or broken symbolic link.
     */
    static void ensureDirectoryExists(Path path) throws IOException {
        // this isn't atomic, but neither is createDirectories.
        if (Files.isDirectory(path)) {
            // verify access, following links (throws exception if something is wrong)
            // we only check READ as a sanity test
            path.getFileSystem().provider().checkAccess(path.toRealPath(), AccessMode.READ);
        } else {
            // doesn't exist, or not a directory
            try {
                Files.createDirectories(path);
            } catch (FileAlreadyExistsException e) {
                // convert optional specific exception so the context is clear
                IOException e2 = new NotDirectoryException(path.toString());
                e2.addSuppressed(e);
                throw e2;
            }
        }
    }

    /** Simple checks that everything is ok */
    @SuppressForbidden(reason = "accesses jvm default tempdir as a self-test")
    static void selfTest() throws IOException {
        // check we can manipulate temporary files
        try {
            Path p = Files.createTempFile(null, null);
            try {
                Files.delete(p);
            } catch (IOException ignored) {
                // potentially virus scanner
            }
        } catch (SecurityException problem) {
            throw new SecurityException("Security misconfiguration: cannot access java.io.tmpdir", problem);
        }
    }
}
