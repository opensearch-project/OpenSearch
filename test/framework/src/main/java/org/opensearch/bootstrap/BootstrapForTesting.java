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

import com.carrotsearch.randomizedtesting.RandomizedRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.Booleans;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.bootstrap.JarHell;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.network.IfConfig;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.mockito.plugin.PriviledgedMockMaker;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.secure_sm.SecureSM;
import org.junit.Assert;

import java.io.InputStream;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketPermission;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;

/**
 * Initializes natives and installs test security manager
 * (init'd early by base classes to ensure it happens regardless of which
 * test case happens to be first, test ordering, etc).
 * <p>
 * The idea is to mimic as much as possible what happens with ES in production
 * mode (e.g. assign permissions and install security manager the same way)
 */
@SuppressWarnings("removal")
public class BootstrapForTesting {

    // TODO: can we share more code with the non-test side here
    // without making things complex???

    static {
        // make sure java.io.tmpdir exists always (in case code uses it in a static initializer)
        Path javaTmpDir = PathUtils.get(
            Objects.requireNonNull(System.getProperty("java.io.tmpdir"), "please set ${java.io.tmpdir} in pom.xml")
        );
        try {
            Security.ensureDirectoryExists(javaTmpDir);
        } catch (Exception e) {
            throw new RuntimeException("unable to create test temp directory", e);
        }

        // just like bootstrap, initialize natives, then SM
        final boolean memoryLock = BootstrapSettings.MEMORY_LOCK_SETTING.get(Settings.EMPTY); // use the default bootstrap.memory_lock
                                                                                              // setting
        final boolean systemCallFilter = Booleans.parseBoolean(System.getProperty("tests.system_call_filter", "true"));
        Bootstrap.initializeNatives(javaTmpDir, memoryLock, systemCallFilter, true);

        // initialize probes
        Bootstrap.initializeProbes();

        // initialize sysprops
        BootstrapInfo.getSystemProperties();

        // check for jar hell
        try {
            final Logger logger = LogManager.getLogger(JarHell.class);
            JarHell.checkJarHell(logger::debug);
        } catch (Exception e) {
            throw new RuntimeException("found jar hell in test classpath", e);
        }

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();

        // install security manager if requested
        if (systemPropertyAsBoolean("tests.security.manager", true)) {
            try {
                // initialize paths the same exact way as bootstrap
                Permissions perms = new Permissions();
                Security.addClasspathPermissions(perms);
                // java.io.tmpdir
                FilePermissionUtils.addDirectoryPath(perms, "java.io.tmpdir", javaTmpDir, "read,readlink,write,delete", false);
                // custom test config file
                String testConfigFile = System.getProperty("tests.config");
                if (Strings.hasLength(testConfigFile)) {
                    FilePermissionUtils.addSingleFilePath(perms, PathUtils.get(testConfigFile), "read,readlink");
                }
                // intellij hack: intellij test runner wants setIO and will
                // screw up all test logging without it!
                if (System.getProperty("tests.gradle") == null) {
                    perms.add(new RuntimePermission("setIO"));
                }

                // add bind permissions for testing
                // ephemeral ports (note, on java 7 before update 51, this is a different permission)
                // this should really be the only one allowed for tests, otherwise they have race conditions
                perms.add(new SocketPermission("localhost:0", "listen,resolve"));
                // ... but tests are messy. like file permissions, just let them live in a fantasy for now.
                // TODO: cut over all tests to bind to ephemeral ports
                perms.add(new SocketPermission("localhost:1024-", "listen,resolve"));

                // read test-framework permissions
                Map<String, URL> codebases = Security.getCodebaseJarMap(JarHell.parseClassPath());
                // when testing server, the main opensearch code is not yet in a jar, so we need to manually add it
                addClassCodebase(codebases, "opensearch", "org.opensearch.plugins.PluginsService");
                if (System.getProperty("tests.gradle") == null) {
                    // intellij and eclipse don't package our internal libs, so we need to set the codebases for them manually
                    addClassCodebase(codebases, "plugin-classloader", "org.opensearch.plugins.ExtendedPluginsClassLoader");
                    addClassCodebase(codebases, "opensearch-nio", "org.opensearch.nio.ChannelFactory");
                    addClassCodebase(codebases, "opensearch-secure-sm", "org.opensearch.secure_sm.SecureSM");
                    addClassCodebase(codebases, "opensearch-rest-client", "org.opensearch.client.RestClient");
                }
                final Policy testFramework = Security.readPolicy(Bootstrap.class.getResource("test-framework.policy"), codebases);
                // Allow modules to define own test policy in ad-hoc fashion (if needed) that is not really applicable to other modules
                final Optional<Policy> testPolicy = Optional.ofNullable(Bootstrap.class.getResource("test.policy"))
                    .map(policy -> Security.readPolicy(policy, codebases));
                final Policy opensearchPolicy = new OpenSearchPolicy(codebases, perms, getPluginPermissions(), true, new Permissions());
                Policy.setPolicy(new Policy() {
                    @Override
                    public boolean implies(ProtectionDomain domain, Permission permission) {
                        // implements union
                        return opensearchPolicy.implies(domain, permission)
                            || testFramework.implies(domain, permission)
                            || testPolicy.map(policy -> policy.implies(domain, permission)).orElse(false /* no policy */);
                    }
                });
                // Create access control context for mocking
                PriviledgedMockMaker.createAccessControlContext();
                System.setSecurityManager(SecureSM.createTestSecureSM(getTrustedHosts()));
                Security.selfTest();

                // guarantee plugin classes are initialized first, in case they have one-time hacks.
                // this just makes unit testing more realistic
                for (URL url : Collections.list(
                    BootstrapForTesting.class.getClassLoader().getResources(PluginInfo.OPENSEARCH_PLUGIN_PROPERTIES)
                )) {
                    Properties properties = new Properties();
                    try (InputStream stream = FileSystemUtils.openFileURLStream(url)) {
                        properties.load(stream);
                    }
                    String clazz = properties.getProperty("classname");
                    if (clazz != null) {
                        Class.forName(clazz);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("unable to install test security manager", e);
            }
        }
    }

    /** Add the codebase url of the given classname to the codebases map, if the class exists. */
    private static void addClassCodebase(Map<String, URL> codebases, String name, String classname) {
        try {
            Class<?> clazz = BootstrapForTesting.class.getClassLoader().loadClass(classname);
            URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
            if (location.toString().endsWith(".jar") == false) {
                if (codebases.put(name, location) != null) {
                    throw new IllegalStateException("Already added " + name + " codebase for testing");
                }
            }
        } catch (ClassNotFoundException e) {
            // no class, fall through to not add. this can happen for any tests that do not include
            // the given class. eg only core tests include plugin-classloader
        }
    }

    /**
     * we don't know which codesources belong to which plugin, so just remove the permission from key codebases
     * like core, test-framework, etc. this way tests fail if accesscontroller blocks are missing.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Map<String, Policy> getPluginPermissions() throws Exception {
        List<URL> pluginPolicies = Collections.list(
            BootstrapForTesting.class.getClassLoader().getResources(PluginInfo.OPENSEARCH_PLUGIN_POLICY)
        );
        if (pluginPolicies.isEmpty()) {
            return Collections.emptyMap();
        }

        // compute classpath minus obvious places, all other jars will get the permission.
        Set<URL> codebases = new HashSet<>(parseClassPathWithSymlinks());
        Set<URL> excluded = new HashSet<>(
            Arrays.asList(
                // es core
                Bootstrap.class.getProtectionDomain().getCodeSource().getLocation(),
                // es test framework
                BootstrapForTesting.class.getProtectionDomain().getCodeSource().getLocation(),
                // lucene test framework
                LuceneTestCase.class.getProtectionDomain().getCodeSource().getLocation(),
                // randomized runner
                RandomizedRunner.class.getProtectionDomain().getCodeSource().getLocation(),
                // junit library
                Assert.class.getProtectionDomain().getCodeSource().getLocation()
            )
        );
        codebases.removeAll(excluded);

        // parse each policy file, with codebase substitution from the classpath
        final List<Policy> policies = new ArrayList<>(pluginPolicies.size());
        for (URL policyFile : pluginPolicies) {
            policies.add(Security.readPolicy(policyFile, Security.getCodebaseJarMap(codebases)));
        }

        // consult each policy file for those codebases
        Map<String, Policy> map = new HashMap<>();
        for (URL url : codebases) {
            map.put(url.getFile(), new Policy() {
                @Override
                public boolean implies(ProtectionDomain domain, Permission permission) {
                    // implements union
                    for (Policy p : policies) {
                        if (p.implies(domain, permission)) {
                            return true;
                        }
                    }
                    return false;
                }
            });
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * return parsed classpath, but with symlinks resolved to destination files for matching
     * this is for matching the toRealPath() in the code where we have a proper plugin structure
     */
    @SuppressForbidden(reason = "does evil stuff with paths and urls because devs and jenkins do evil stuff with paths and urls")
    static Set<URL> parseClassPathWithSymlinks() throws Exception {
        Set<URL> raw = JarHell.parseClassPath();
        Set<URL> cooked = new HashSet<>(raw.size());
        for (URL url : raw) {
            Path path = PathUtils.get(url.toURI());
            if (Files.exists(path)) {
                boolean added = cooked.add(path.toRealPath().toUri().toURL());
                if (added == false) {
                    throw new IllegalStateException("Duplicate in classpath after resolving symlinks: " + url);
                }
            }
        }
        return raw;
    }

    /**
     * Collect host addresses of all local interfaces so we could check
     * if the network connection is being made only on those.
     * @return host names and addresses of all local interfaces
     */
    private static Set<String> getTrustedHosts() {
        //
        try {
            return Collections.list(NetworkInterface.getNetworkInterfaces())
                .stream()
                .flatMap(iface -> Collections.list(iface.getInetAddresses()).stream())
                .map(address -> NetworkAddress.format(address))
                .collect(Collectors.toSet());
        } catch (final SocketException e) {
            return Collections.emptySet();
        }
    }

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}
}
