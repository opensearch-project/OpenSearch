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

package org.opensearch.node;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.bootstrap.BootstrapCheck;
import org.opensearch.bootstrap.BootstrapContext;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine.Searcher;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsProbe;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.NodeRoles;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.NodeRoles.addRoles;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class NodeTests extends OpenSearchTestCase {

    public static class CheckPlugin extends Plugin {
        public static final BootstrapCheck CHECK = context -> BootstrapCheck.BootstrapCheckResult.success();

        @Override
        public List<BootstrapCheck> getBootstrapChecks() {
            return Collections.singletonList(CHECK);
        }
    }

    private List<Class<? extends Plugin>> basePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(getTestTransportPlugin());
        plugins.add(MockHttpTransport.TestPlugin.class);
        return plugins;
    }

    public void testLoadPluginBootstrapChecks() throws IOException {
        final String name = randomBoolean() ? randomAlphaOfLength(10) : null;
        Settings.Builder settings = baseSettings();
        if (name != null) {
            settings.put(Node.NODE_NAME_SETTING.getKey(), name);
        }
        AtomicBoolean executed = new AtomicBoolean(false);
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(CheckPlugin.class);
        try (Node node = new MockNode(settings.build(), plugins) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                BootstrapContext context,
                BoundTransportAddress boundTransportAddress,
                List<BootstrapCheck> bootstrapChecks
            ) throws NodeValidationException {
                assertEquals(1, bootstrapChecks.size());
                assertSame(CheckPlugin.CHECK, bootstrapChecks.get(0));
                executed.set(true);
                throw new NodeValidationException("boom");
            }
        }) {
            expectThrows(NodeValidationException.class, () -> node.start());
            assertTrue(executed.get());
        }
    }

    public void testNodeAttributes() throws IOException {
        String attr = randomAlphaOfLength(5);
        Settings.Builder settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            assertEquals(attr, Node.NODE_ATTRIBUTES.getAsMap(nodeSettings).get("test_attr"));
        }

        // leading whitespace not allowed
        attr = " leading";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            fail("should not allow a node attribute with leading whitespace");
        } catch (IllegalArgumentException e) {
            assertEquals("node.attr.test_attr cannot have leading or trailing whitespace [ leading]", e.getMessage());
        }

        // trailing whitespace not allowed
        attr = "trailing ";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            fail("should not allow a node attribute with trailing whitespace");
        } catch (IllegalArgumentException e) {
            assertEquals("node.attr.test_attr cannot have leading or trailing whitespace [trailing ]", e.getMessage());
        }
    }

    public void testServerNameNodeAttribute() throws IOException {
        String attr = "valid-hostname";
        Settings.Builder settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "server_name", attr);
        int i = 0;
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            assertEquals(attr, Node.NODE_ATTRIBUTES.getAsMap(nodeSettings).get("server_name"));
        }

        // non-LDH hostname not allowed
        attr = "invalid_hostname";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "server_name", attr);
        try (Node node = new MockNode(settings.build(), basePlugins())) {
            fail("should not allow a server_name attribute with an underscore");
        } catch (IllegalArgumentException e) {
            assertEquals("invalid node.attr.server_name [invalid_hostname]", e.getMessage());
        }
    }

    private static Settings.Builder baseSettings() {
        final Path tempDir = createTempDir();
        return Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", randomLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
            .put(dataNode());
    }

    public void testCloseOnOutstandingTask() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        AtomicBoolean shouldRun = new AtomicBoolean(true);
        final CountDownLatch threadRunning = new CountDownLatch(1);
        threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            threadRunning.countDown();
            while (shouldRun.get())
                ;
        });
        threadRunning.await();
        node.close();
        shouldRun.set(false);
        assertTrue(node.awaitClose(10L, TimeUnit.SECONDS));
    }

    public void testCloseRaceWithTaskExecution() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        AtomicBoolean shouldRun = new AtomicBoolean(true);
        final CountDownLatch running = new CountDownLatch(3);
        Thread submitThread = new Thread(() -> {
            running.countDown();
            try {
                running.await();
            } catch (InterruptedException e) {
                throw new AssertionError("interrupted while waiting", e);
            }
            try {
                threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                    while (shouldRun.get())
                        ;
                });
            } catch (RejectedExecutionException e) {
                assertThat(e.getMessage(), containsString("[Terminated,"));
            }
        });
        Thread closeThread = new Thread(() -> {
            running.countDown();
            try {
                running.await();
            } catch (InterruptedException e) {
                throw new AssertionError("interrupted while waiting", e);
            }
            try {
                node.close();
            } catch (IOException e) {
                throw new AssertionError("node close failed", e);
            }
        });
        submitThread.start();
        closeThread.start();
        running.countDown();
        running.await();

        submitThread.join();
        closeThread.join();

        shouldRun.set(false);
        assertTrue(node.awaitClose(10L, TimeUnit.SECONDS));
    }

    public void testAwaitCloseTimeoutsOnNonInterruptibleTask() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        AtomicBoolean shouldRun = new AtomicBoolean(true);
        final CountDownLatch threadRunning = new CountDownLatch(1);
        threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            threadRunning.countDown();
            while (shouldRun.get())
                ;
        });
        threadRunning.await();
        node.close();
        assertFalse(node.awaitClose(0, TimeUnit.MILLISECONDS));
        shouldRun.set(false);
        assertTrue(node.awaitClose(10L, TimeUnit.SECONDS));
    }

    public void testCloseOnInterruptibleTask() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        ThreadPool threadpool = node.injector().getInstance(ThreadPool.class);
        final CountDownLatch threadRunning = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        threadpool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            threadRunning.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            } finally {
                finishLatch.countDown();
            }
        });
        threadRunning.await();
        node.close();
        // close should not interrupt ongoing tasks
        assertFalse(interrupted.get());
        // but awaitClose should
        node.awaitClose(0, TimeUnit.SECONDS);
        finishLatch.await();
        assertTrue(interrupted.get());
    }

    public void testCloseOnLeakedIndexReaderReference() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Searcher searcher = shard.acquireSearcher("test");
        node.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> node.awaitClose(10L, TimeUnit.SECONDS));
        searcher.close();
        assertThat(e.getMessage(), containsString("Something is leaking index readers or store references"));
    }

    public void testCloseOnLeakedStoreReference() throws Exception {
        Node node = new MockNode(baseSettings().build(), basePlugins());
        node.start();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );
        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        shard.store().incRef();
        node.close();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> node.awaitClose(10L, TimeUnit.SECONDS));
        shard.store().decRef();
        assertThat(e.getMessage(), containsString("Something is leaking index readers or store references"));
    }

    public void testCreateWithCircuitBreakerPlugins() throws IOException {
        Settings.Builder settings = baseSettings().put("breaker.test_breaker.limit", "50b");
        List<Class<? extends Plugin>> plugins = basePlugins();
        plugins.add(MockCircuitBreakerPlugin.class);
        try (Node node = new MockNode(settings.build(), plugins)) {
            CircuitBreakerService service = node.injector().getInstance(CircuitBreakerService.class);
            assertThat(service.getBreaker("test_breaker"), is(not(nullValue())));
            assertThat(service.getBreaker("test_breaker").getLimit(), equalTo(50L));
            CircuitBreakerPlugin breakerPlugin = node.getPluginsService().filterPlugins(CircuitBreakerPlugin.class).get(0);
            assertTrue(breakerPlugin instanceof MockCircuitBreakerPlugin);
            assertSame(
                "plugin circuit breaker instance is not the same as breaker service's instance",
                ((MockCircuitBreakerPlugin) breakerPlugin).myCircuitBreaker.get(),
                service.getBreaker("test_breaker")
            );
        }
    }

    public void testCreateWithFileCache() throws Exception {
        Settings searchRoleSettings = addRoles(baseSettings().build(), Set.of(DiscoveryNodeRole.SEARCH_ROLE));
        List<Class<? extends Plugin>> plugins = basePlugins();
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        Settings searchRoleSettingsWithConfig = baseSettings().put(searchRoleSettings)
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .build();
        Settings onlySearchRoleSettings = Settings.builder()
            .put(searchRoleSettingsWithConfig)
            .put(
                NodeRoles.removeRoles(
                    searchRoleSettingsWithConfig,
                    Set.of(
                        DiscoveryNodeRole.DATA_ROLE,
                        DiscoveryNodeRole.CLUSTER_MANAGER_ROLE,
                        DiscoveryNodeRole.INGEST_ROLE,
                        DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE
                    )
                )
            )
            .build();

        // Test exception thrown with configuration missing
        assertThrows(SettingsException.class, () -> new MockNode(searchRoleSettings, plugins));

        // Test file cache is initialized
        try (MockNode mockNode = new MockNode(searchRoleSettingsWithConfig, plugins)) {
            NodeEnvironment.NodePath fileCacheNodePath = mockNode.getNodeEnvironment().fileCacheNodePath();
            assertEquals(cacheSize.getBytes(), fileCacheNodePath.fileCacheReservedSize.getBytes());
        }

        // Test data + search node with defined cache size
        try (MockNode mockNode = new MockNode(searchRoleSettingsWithConfig, plugins)) {
            NodeEnvironment.NodePath fileCacheNodePath = mockNode.getNodeEnvironment().fileCacheNodePath();
            assertEquals(cacheSize.getBytes(), fileCacheNodePath.fileCacheReservedSize.getBytes());
        }

        // Test dedicated search node with no configuration
        try (MockNode mockNode = new MockNode(onlySearchRoleSettings, plugins)) {
            NodeEnvironment.NodePath fileCacheNodePath = mockNode.getNodeEnvironment().fileCacheNodePath();
            assertTrue(fileCacheNodePath.fileCacheReservedSize.getBytes() > 0);
            FsProbe fsProbe = new FsProbe(mockNode.getNodeEnvironment(), mockNode.fileCache());
            FsInfo fsInfo = fsProbe.stats(null);
            FsInfo.Path cachePathInfo = fsInfo.iterator().next();
            assertEquals(cachePathInfo.getFileCacheReserved().getBytes(), fileCacheNodePath.fileCacheReservedSize.getBytes());
        }
    }

    public static class MockCircuitBreakerPlugin extends Plugin implements CircuitBreakerPlugin {

        private SetOnce<CircuitBreaker> myCircuitBreaker = new SetOnce<>();

        public MockCircuitBreakerPlugin() {}

        @Override
        public BreakerSettings getCircuitBreaker(Settings settings) {
            return BreakerSettings.updateFromSettings(
                new BreakerSettings("test_breaker", 100L, 1.0d, CircuitBreaker.Type.MEMORY, CircuitBreaker.Durability.TRANSIENT),
                settings
            );
        }

        @Override
        public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
            assertThat(circuitBreaker.getName(), equalTo("test_breaker"));
            myCircuitBreaker.set(circuitBreaker);
        }
    }
}
