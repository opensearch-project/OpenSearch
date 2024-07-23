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

package org.opensearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.LocalNodeMasterListener;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ContextSwitcher;
import org.opensearch.common.util.concurrent.InternalContextSwitcher;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ClusterApplierServiceTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static ContextSwitcher contextSwitcher;
    private TimedClusterApplierService clusterApplierService;
    private static MetricsRegistry metricsRegistry;
    private static Histogram applierslatencyHistogram;
    private static Histogram listenerslatencyHistogram;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(ClusterApplierServiceTests.class.getName());
        contextSwitcher = new InternalContextSwitcher(threadPool);
        metricsRegistry = mock(MetricsRegistry.class);
        applierslatencyHistogram = mock(Histogram.class);
        listenerslatencyHistogram = mock(Histogram.class);
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(metricsRegistry.createHistogram(anyString(), anyString(), anyString())).thenAnswer(invocationOnMock -> {
            String histogramName = (String) invocationOnMock.getArguments()[0];
            if (histogramName.contains("appliers.latency")) {
                return applierslatencyHistogram;
            }
            return listenerslatencyHistogram;
        });
        clusterApplierService = createTimedClusterService(true, Optional.of(metricsRegistry));
    }

    @After
    public void tearDown() throws Exception {
        clusterApplierService.close();
        super.tearDown();
    }

    private TimedClusterApplierService createTimedClusterService(
        boolean makeClusterManager,
        Optional<MetricsRegistry> metricsRegistryOptional
    ) {
        DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        TimedClusterApplierService timedClusterApplierService;
        if (metricsRegistryOptional != null && metricsRegistryOptional.isPresent()) {
            timedClusterApplierService = new TimedClusterApplierService(
                Settings.builder().put("cluster.name", "ClusterApplierServiceTests").build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                new ClusterManagerMetrics(metricsRegistry)
            );
        } else {
            timedClusterApplierService = new TimedClusterApplierService(
                Settings.builder().put("cluster.name", "ClusterApplierServiceTests").build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            );
        }
        timedClusterApplierService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        timedClusterApplierService.setInitialState(
            ClusterState.builder(new ClusterName("ClusterApplierServiceTests"))
                .nodes(
                    DiscoveryNodes.builder()
                        .add(localNode)
                        .localNodeId(localNode.getId())
                        .clusterManagerNodeId(makeClusterManager ? localNode.getId() : null)
                )
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build()
        );
        timedClusterApplierService.start();
        return timedClusterApplierService;
    }

    @TestLogging(value = "org.opensearch.cluster.service:TRACE", reason = "to ensure that we log cluster state events on TRACE level")
    public void testClusterStateUpdateLogging() throws Exception {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ClusterApplierService.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.DEBUG,
                    "*processing [test1]: took [1s] no change in cluster state"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.TRACE,
                    "*failed to execute cluster state applier in [2s]*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test3",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.DEBUG,
                    "*processing [test3]: took [0s] no change in cluster state*"
                )
            );

            clusterApplierService.currentTimeOverride = threadPool.relativeTimeInMillis();
            clusterApplierService.runOnApplierThread(
                "test1",
                currentState -> clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(1).millis(),
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {}

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                }
            );
            clusterApplierService.runOnApplierThread("test2", currentState -> {
                clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(2).millis();
                throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
            }, new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {}
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            clusterApplierService.runOnApplierThread("test3", currentState -> {}, new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {}

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            assertBusy(mockAppender::assertAllExpectationsMatched);
        }
        verifyNoInteractions(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    @TestLogging(value = "org.opensearch.cluster.service:WARN", reason = "to ensure that we log cluster state events on WARN level")
    public void testLongClusterStateUpdateLogging() throws Exception {
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ClusterApplierService.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "test1 shouldn't see because setting is too low",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "*cluster state applier task [test1] took [*] which is above the warn threshold of *"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test2",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "*cluster state applier task [test2] took [32s] which is above the warn threshold of [*]: "
                        + "[running task [test2]] took [*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test4",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "*cluster state applier task [test3] took [34s] which is above the warn threshold of [*]: "
                        + "[running task [test3]] took [*"
                )
            );

            final CountDownLatch latch = new CountDownLatch(4);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            clusterApplierService.currentTimeOverride = threadPool.relativeTimeInMillis();
            clusterApplierService.runOnApplierThread(
                "test1",
                currentState -> clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(1).millis(),
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        latch.countDown();
                        processedFirstTask.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                }
            );
            processedFirstTask.await();
            clusterApplierService.runOnApplierThread("test2", currentState -> {
                clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(32).millis();
                throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
            }, new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }
            });
            clusterApplierService.runOnApplierThread(
                "test3",
                currentState -> clusterApplierService.currentTimeOverride += TimeValue.timeValueSeconds(34).millis(),
                new ClusterApplyListener() {
                    @Override
                    public void onSuccess(String source) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }
                }
            );
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterApplierService.runOnApplierThread("test4", currentState -> {}, new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            latch.await();
            mockAppender.assertAllExpectationsMatched();
        }
        verifyNoInteractions(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    public void testLocalNodeClusterManagerListenerCallbacks() {
        TimedClusterApplierService timedClusterApplierService = createTimedClusterService(false, Optional.empty());

        AtomicBoolean isClusterManager = new AtomicBoolean();
        timedClusterApplierService.addLocalNodeClusterManagerListener(new LocalNodeClusterManagerListener() {
            @Override
            public void onClusterManager() {
                isClusterManager.set(true);
            }

            @Override
            public void offClusterManager() {
                isClusterManager.set(false);
            }
        });

        ClusterState state = timedClusterApplierService.state();
        DiscoveryNodes nodes = state.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes).clusterManagerNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isClusterManager.get(), is(true));

        nodes = state.nodes();
        nodesBuilder = DiscoveryNodes.builder(nodes).clusterManagerNodeId(null);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_WRITES))
            .nodes(nodesBuilder)
            .build();
        setState(timedClusterApplierService, state);
        assertThat(isClusterManager.get(), is(false));
        nodesBuilder = DiscoveryNodes.builder(nodes).clusterManagerNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isClusterManager.get(), is(true));

        verifyNoInteractions(applierslatencyHistogram, listenerslatencyHistogram);

        timedClusterApplierService.close();
    }

    /* Validate the backwards compatibility of LocalNodeMasterListener remains
     * after making it a subclass of LocalNodeClusterManagerListener.
     * Overriding the methods with non-inclusive words are intentional.
     * To support inclusive language, LocalNodeMasterListener is deprecated in 2.2.
     */
    public void testDeprecatedLocalNodeMasterListenerCallbacks() {
        TimedClusterApplierService timedClusterApplierService = createTimedClusterService(false, Optional.empty());

        AtomicBoolean isClusterManager = new AtomicBoolean();
        timedClusterApplierService.addLocalNodeMasterListener(new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                isClusterManager.set(true);
            }

            @Override
            public void offMaster() {
                isClusterManager.set(false);
            }
        });

        ClusterState state = timedClusterApplierService.state();
        DiscoveryNodes nodes = state.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isClusterManager.get(), is(true));

        nodes = state.nodes();
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(null);
        state = ClusterState.builder(state).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isClusterManager.get(), is(false));

        verifyNoInteractions(applierslatencyHistogram, listenerslatencyHistogram);

        timedClusterApplierService.close();
    }

    public void testClusterStateApplierCantSampleClusterState() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                clusterApplierService.state();
                error.set(new AssertionError("successfully sampled state"));
            } catch (AssertionError e) {
                if (e.getMessage().contains("should not be called by a cluster state applier") == false) {
                    error.set(e);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());

        verify(applierslatencyHistogram, atLeastOnce()).record(anyDouble(), any());
        clearInvocations(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    public void testClusterStateApplierBubblesUpExceptionsInApplier() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        clusterApplierService.addStateApplier(event -> { throw new RuntimeException("dummy exception"); });
        clusterApplierService.allowClusterStateApplicationFailure();

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertTrue(error.compareAndSet(null, e));
                    latch.countDown();
                }
            }
        );

        latch.await();
        assertNotNull(error.get());
        assertThat(error.get().getMessage(), containsString("dummy exception"));

        verifyNoInteractions(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    public void testClusterStateApplierBubblesUpExceptionsInSettingsApplier() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        clusterApplierService.clusterSettings.addSettingsUpdateConsumer(
            EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
            v -> {}
        );
        clusterApplierService.allowClusterStateApplicationFailure();

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state())
                .metadata(
                    Metadata.builder(clusterApplierService.state().metadata())
                        .persistentSettings(
                            Settings.builder()
                                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), false)
                                .build()
                        )
                        .build()
                )
                .build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                    fail("should not be called");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertTrue(error.compareAndSet(null, e));
                    latch.countDown();
                }
            }
        );

        latch.await();
        assertNotNull(error.get());
        assertThat(error.get().getMessage(), containsString("illegal value can't update"));

        verifyNoInteractions(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    public void testClusterStateApplierSwallowsExceptionInListener() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addListener(event -> {
            assertTrue(applierCalled.compareAndSet(false, true));
            throw new RuntimeException("dummy exception");
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());

        verifyNoInteractions(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    public void testClusterStateApplierCanCreateAnObserver() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterApplierService.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                ClusterStateObserver observer = new ClusterStateObserver(
                    event.state(),
                    clusterApplierService,
                    null,
                    logger,
                    threadPool.getThreadContext()
                );
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {

                    }

                    @Override
                    public void onClusterServiceClose() {

                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {

                    }
                });
            } catch (AssertionError e) {
                error.set(e);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState(
            "test",
            () -> ClusterState.builder(clusterApplierService.state()).build(),
            new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    error.compareAndSet(null, e);
                }
            }
        );

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());

        verify(applierslatencyHistogram, atLeastOnce()).record(anyDouble(), any());
        clearInvocations(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    public void testThreadContext() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        try (ThreadContext.StoredContext ignored = contextSwitcher.switchContext()) {
            final Map<String, String> expectedHeaders = Collections.singletonMap("test", "test");
            final Map<String, List<String>> expectedResponseHeaders = Collections.singletonMap(
                "testResponse",
                Collections.singletonList("testResponse")
            );
            threadPool.getThreadContext().putHeader(expectedHeaders);

            clusterApplierService.onNewClusterState("test", () -> {
                assertTrue(threadPool.getThreadContext().isSystemContext());
                assertEquals(Collections.emptyMap(), threadPool.getThreadContext().getHeaders());
                threadPool.getThreadContext().addResponseHeader("testResponse", "testResponse");
                assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                if (randomBoolean()) {
                    return ClusterState.builder(clusterApplierService.state()).build();
                } else {
                    throw new IllegalArgumentException("mock failure");
                }
            }, new ClusterApplyListener() {

                @Override
                public void onSuccess(String source) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    assertFalse(threadPool.getThreadContext().isSystemContext());
                    assertEquals(expectedHeaders, threadPool.getThreadContext().getHeaders());
                    assertEquals(expectedResponseHeaders, threadPool.getThreadContext().getResponseHeaders());
                    latch.countDown();
                }
            });
        }

        latch.await();

        verifyNoInteractions(applierslatencyHistogram);
        verifyNoInteractions(listenerslatencyHistogram);
    }

    static class TimedClusterApplierService extends ClusterApplierService {

        final ClusterSettings clusterSettings;
        volatile Long currentTimeOverride = null;
        boolean applicationMayFail;

        TimedClusterApplierService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
            super("test_node", settings, clusterSettings, threadPool);
            this.clusterSettings = clusterSettings;
        }

        TimedClusterApplierService(
            Settings settings,
            ClusterSettings clusterSettings,
            ThreadPool threadPool,
            ClusterManagerMetrics clusterManagerMetrics
        ) {
            super("test_node", settings, clusterSettings, threadPool, clusterManagerMetrics);
            this.clusterSettings = clusterSettings;
        }

        @Override
        protected long currentTimeInMillis() {
            if (currentTimeOverride != null) {
                return currentTimeOverride;
            }
            return super.currentTimeInMillis();
        }

        @Override
        protected boolean applicationMayFail() {
            return this.applicationMayFail;
        }

        void allowClusterStateApplicationFailure() {
            this.applicationMayFail = true;
        }
    }

}
