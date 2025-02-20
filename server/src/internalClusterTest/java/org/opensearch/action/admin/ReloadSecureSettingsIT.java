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

package org.opensearch.action.admin;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.RemoteTransportException;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class ReloadSecureSettingsIT extends OpenSearchIntegTestCase {

    // Minimal required characters to fulfill the requirement of 112 bit strong passwords
    protected static final int MIN_112_BIT_STRONG = 14;

    public void testMissingKeystoreFile() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
            .stream()
            .findFirst()
            .get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        // keystore file should be missing for this test case
        Files.deleteIfExists(KeyStoreWrapper.keystorePath(environment.configDir()));
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        final CountDownLatch latch = new CountDownLatch(1);
        final SecureString emptyPassword = randomBoolean() ? new SecureString(new char[0]) : null;
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setSecureStorePassword(emptyPassword)
            .setNodesIds(Strings.EMPTY_ARRAY)
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    try {
                        assertThat(nodesReloadResponse, notNullValue());
                        final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                        assertThat(nodesMap.size(), equalTo(cluster().size()));
                        for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                            assertThat(nodeResponse.reloadException(), notNullValue());
                            assertThat(nodeResponse.reloadException(), instanceOf(IllegalStateException.class));
                            assertThat(nodeResponse.reloadException().getMessage(), containsString("Keystore is missing"));
                        }
                    } catch (final AssertionError e) {
                        reloadSettingsError.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                    latch.countDown();
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the missing keystore case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testInvalidKeystoreFile() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
            .stream()
            .findFirst()
            .get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        // invalid "keystore" file should be present in the config dir
        try (InputStream keystore = ReloadSecureSettingsIT.class.getResourceAsStream("invalid.txt.keystore")) {
            if (Files.exists(environment.configDir()) == false) {
                Files.createDirectory(environment.configDir());
            }
            Files.copy(keystore, KeyStoreWrapper.keystorePath(environment.configDir()), StandardCopyOption.REPLACE_EXISTING);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final SecureString emptyPassword = randomBoolean() ? new SecureString(new char[0]) : null;
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setSecureStorePassword(emptyPassword)
            .setNodesIds(Strings.EMPTY_ARRAY)
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    try {
                        assertThat(nodesReloadResponse, notNullValue());
                        final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                        assertThat(nodesMap.size(), equalTo(cluster().size()));
                        for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                            assertThat(nodeResponse.reloadException(), notNullValue());
                        }
                    } catch (final AssertionError e) {
                        reloadSettingsError.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                    latch.countDown();
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the invalid keystore format case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testReloadAllNodesWithPasswordWithoutTLSFails() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
            .stream()
            .findFirst()
            .get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        final char[] password = randomAlphaOfLength(MIN_112_BIT_STRONG).toCharArray();
        writeEmptyKeystore(environment, password);
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            // No filter should try to hit all nodes
            .setNodesIds(Strings.EMPTY_ARRAY)
            .setSecureStorePassword(new SecureString(password))
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    reloadSettingsError.set(new AssertionError("Nodes request succeeded when it should have failed", null));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        if (e instanceof RemoteTransportException) {
                            // transport client was used, so need to unwrap the returned exception
                            assertThat(e.getCause(), instanceOf(Exception.class));
                            e = (Exception) e.getCause();
                        }
                        assertThat(e, instanceOf(OpenSearchException.class));
                        assertThat(
                            e.getMessage(),
                            containsString(
                                "Secure settings cannot be updated cluster wide when TLS for the " + "transport layer is not enabled"
                            )
                        );
                    } finally {
                        latch.countDown();
                    }
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testReloadLocalNodeWithPasswordWithoutTLSSucceeds() throws Exception {
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final char[] password = randomAlphaOfLength(MIN_112_BIT_STRONG).toCharArray();
        writeEmptyKeystore(environment, password);
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setNodesIds("_local")
            .setSecureStorePassword(new SecureString(password))
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    try {
                        assertThat(nodesReloadResponse, notNullValue());
                        final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                        assertThat(nodesMap.size(), equalTo(1));
                        assertThat(nodesReloadResponse.getNodes().size(), equalTo(1));
                        final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse = nodesReloadResponse.getNodes().get(0);
                        assertThat(nodeResponse.reloadException(), nullValue());
                    } catch (final AssertionError e) {
                        reloadSettingsError.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                    latch.countDown();
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
    }

    public void testWrongKeystorePassword() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
            .stream()
            .findFirst()
            .get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        final char[] password = inFipsJvm() ? randomAlphaOfLength(MIN_112_BIT_STRONG).toCharArray() : new char[0];
        // "some" keystore should be present in this case
        writeEmptyKeystore(environment, password);
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setNodesIds("_local")
            .setSecureStorePassword(new SecureString("thewrongkeystorepassword".toCharArray()))
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    try {
                        assertThat(nodesReloadResponse, notNullValue());
                        final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                        assertThat(nodesMap.size(), equalTo(1));
                        for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                            assertThat(nodeResponse.reloadException(), notNullValue());
                            assertThat(nodeResponse.reloadException(), instanceOf(SecurityException.class));
                        }
                    } catch (final AssertionError e) {
                        reloadSettingsError.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                    latch.countDown();
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the wrong password case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testMisbehavingPlugin() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
        final Environment environment = internalCluster().getInstance(Environment.class);
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
            .stream()
            .findFirst()
            .get();
        // make plugins throw on reload
        for (final String nodeName : internalCluster().getNodeNames()) {
            internalCluster().getInstance(PluginsService.class, nodeName)
                .filterPlugins(MisbehavingReloadablePlugin.class)
                .stream()
                .findFirst()
                .get()
                .setShouldThrow(true);
        }
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        // "some" keystore should be present
        final SecureSettings secureSettings = writeEmptyKeystore(environment, new char[0]);
        // read seed setting value from the test case (not from the node)
        final String seedValue = KeyStoreWrapper.SEED_SETTING.get(
            Settings.builder().put(environment.settings()).setSecureSettings(secureSettings).build()
        ).toString();
        final CountDownLatch latch = new CountDownLatch(1);
        final SecureString emptyPassword = randomBoolean() ? new SecureString(new char[0]) : null;
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setSecureStorePassword(emptyPassword)
            .setNodesIds(Strings.EMPTY_ARRAY)
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    try {
                        assertThat(nodesReloadResponse, notNullValue());
                        final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                        assertThat(nodesMap.size(), equalTo(cluster().size()));
                        for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                            assertThat(nodeResponse.reloadException(), notNullValue());
                            assertThat(nodeResponse.reloadException().getMessage(), containsString("If shouldThrow I throw"));
                        }
                    } catch (final AssertionError e) {
                        reloadSettingsError.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                    latch.countDown();
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // even if one plugin fails to reload (throws Exception), others should be
        // unperturbed
        assertThat(mockReloadablePlugin.getReloadCount() - initialReloadCount, equalTo(1));
        // mock plugin should have been reloaded successfully
        assertThat(mockReloadablePlugin.getSeedValue(), equalTo(seedValue));
    }

    public void testReloadWhileKeystoreChanged() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
            .stream()
            .findFirst()
            .get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        for (int i = 0; i < randomIntBetween(4, 8); i++) {
            // write keystore
            final SecureSettings secureSettings = writeEmptyKeystore(environment, new char[0]);
            // read seed setting value from the test case (not from the node)
            final String seedValue = KeyStoreWrapper.SEED_SETTING.get(
                Settings.builder().put(environment.settings()).setSecureSettings(secureSettings).build()
            ).toString();
            // reload call
            successfulReloadCall();
            assertThat(mockReloadablePlugin.getSeedValue(), equalTo(seedValue));
            assertThat(mockReloadablePlugin.getReloadCount() - initialReloadCount, equalTo(i + 1));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = Arrays.asList(MockReloadablePlugin.class, MisbehavingReloadablePlugin.class);
        // shuffle as reload is called in order
        Collections.shuffle(plugins, random());
        return plugins;
    }

    private void successfulReloadCall() throws InterruptedException {
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final SecureString emptyPassword = randomBoolean() ? new SecureString(new char[0]) : null;
        client().admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setSecureStorePassword(emptyPassword)
            .setNodesIds(Strings.EMPTY_ARRAY)
            .execute(new ActionListener<NodesReloadSecureSettingsResponse>() {
                @Override
                public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                    try {
                        assertThat(nodesReloadResponse, notNullValue());
                        final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                        assertThat(nodesMap.size(), equalTo(cluster().size()));
                        for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                            assertThat(nodeResponse.reloadException(), nullValue());
                        }
                    } catch (final AssertionError e) {
                        reloadSettingsError.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                    latch.countDown();
                }
            });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
    }

    @SuppressWarnings("removal")
    private SecureSettings writeEmptyKeystore(Environment environment, char[] password) throws Exception {
        final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
        try {
            keyStoreWrapper.save(environment.configDir(), password);
        } catch (final AccessControlException e) {
            if (e.getPermission() instanceof RuntimePermission && e.getPermission().getName().equals("accessUserInformation")) {
                // this is expected: the save method is extra diligent and wants to make sure
                // the keystore is readable, not relying on umask and whatnot. It's ok, we don't
                // care about this in tests.
            } else {
                throw e;
            }
        }
        return keyStoreWrapper;
    }

    public static class CountingReloadablePlugin extends Plugin implements ReloadablePlugin {

        private volatile int reloadCount;

        public CountingReloadablePlugin() {}

        @Override
        public void reload(Settings settings) throws Exception {
            reloadCount++;
        }

        public int getReloadCount() {
            return reloadCount;
        }

    }

    public static class MockReloadablePlugin extends CountingReloadablePlugin {

        private volatile String seedValue;

        public MockReloadablePlugin() {}

        @Override
        public void reload(Settings settings) throws Exception {
            super.reload(settings);
            this.seedValue = KeyStoreWrapper.SEED_SETTING.get(settings).toString();
        }

        public String getSeedValue() {
            return seedValue;
        }

    }

    public static class MisbehavingReloadablePlugin extends CountingReloadablePlugin {

        private boolean shouldThrow = false;

        public MisbehavingReloadablePlugin() {}

        @Override
        public synchronized void reload(Settings settings) throws Exception {
            super.reload(settings);
            if (shouldThrow) {
                shouldThrow = false;
                throw new Exception("If shouldThrow I throw");
            }
        }

        public synchronized void setShouldThrow(boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }
    }

}
