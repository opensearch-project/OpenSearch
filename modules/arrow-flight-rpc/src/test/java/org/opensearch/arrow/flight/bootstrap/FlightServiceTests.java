/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.tls.DefaultSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.DisabledSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightServiceTests extends OpenSearchTestCase {

    private Settings settings;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SecureTransportSettingsProvider secureTransportSettingsProvider;
    private final AtomicInteger port = new AtomicInteger(0);
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
        int availablePort = getBaseStreamPort() + port.addAndGet(1);
        settings = Settings.EMPTY;
        localNode = createNode(availablePort);

        // Setup initial cluster state
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.localNodeId(localNode.getId());
        nodesBuilder.add(localNode);
        DiscoveryNodes nodes = nodesBuilder.build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME)).thenReturn(mock(ExecutorService.class));
        when(threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME)).thenReturn(mock(ExecutorService.class));
        secureTransportSettingsProvider = mock(SecureTransportSettingsProvider.class);
    }

    public void testInitializeWithSslEnabled() throws Exception {
        // Configure SSL enabled
        Settings sslSettings = Settings.builder().put("arrow.ssl.enable", true).build();

        try (FlightService sslService = new FlightService(sslSettings)) {
            sslService.setSecureTransportSettingsProvider(secureTransportSettingsProvider);
            sslService.initialize(clusterService, threadPool);
            expectThrows(RuntimeException.class, () -> sslService.onNodeStart(localNode));

            SslContextProvider sslContextProvider = sslService.getSslContextProvider();
            assertNotNull("SSL context provider should not be null", sslContextProvider);
            assertTrue("SSL context provider should be DefaultSslContextProvider", sslContextProvider instanceof DefaultSslContextProvider);
            assertTrue("SSL should be enabled", sslContextProvider.isSslEnabled());
        }
    }

    public void testInitializeWithSslDisabled() throws Exception {
        int testPort = getBaseStreamPort() + port.addAndGet(1);

        Settings noSslSettings = Settings.builder()
            .put("node.attr.transport.stream.port", String.valueOf(testPort))
            .put("arrow.ssl.enable", false)
            .build();

        try (FlightService noSslService = new FlightService(noSslSettings)) {
            noSslService.initialize(clusterService, threadPool);
            noSslService.start();
            noSslService.onNodeStart(localNode);
            // Verify SSL is properly disabled
            SslContextProvider sslContextProvider = noSslService.getSslContextProvider();
            assertNotNull("SSL context provider should not be null", sslContextProvider);
            assertTrue(
                "SSL context provider should be DisabledSslContextProvider",
                sslContextProvider instanceof DisabledSslContextProvider
            );
            assertFalse("SSL should be disabled", sslContextProvider.isSslEnabled());
        }
    }

    public void testStartAndStop() throws Exception {
        try (FlightService testService = new FlightService(Settings.EMPTY)) {
            testService.initialize(clusterService, threadPool);

            testService.start();
            testService.onNodeStart(localNode);
            testService.stop();
            testService.start();
            testService.onNodeStart(localNode);
            assertNotNull(testService.getStreamManager());
        }
    }

    public void testInitializeWithoutSecureTransportSettingsProvider() {
        Settings sslSettings = Settings.builder().put(settings).put("arrow.ssl.enable", true).build();

        try (FlightService sslService = new FlightService(sslSettings)) {
            // Should throw exception when initializing without provider
            expectThrows(RuntimeException.class, () -> {
                sslService.initialize(clusterService, threadPool);
                sslService.onNodeStart(localNode);
            });
        }
    }

    public void testServerStartupFailure() {
        Settings invalidSettings = Settings.builder()
            .put("node.attr.transport.stream.port", "-1") // Invalid port
            .build();
        try (FlightService invalidService = new FlightService(invalidSettings)) {
            expectThrows(RuntimeException.class, () -> { invalidService.onNodeStart(localNode); });
        }
    }

    public void testLifecycleStateTransitions() throws Exception {
        // Find new port for this test
        try (FlightService testService = new FlightService(Settings.EMPTY)) {
            testService.initialize(clusterService, threadPool);

            // Test all state transitions
            testService.start();
            testService.onNodeStart(localNode);
            assertEquals("STARTED", testService.lifecycleState().toString());

            testService.stop();
            assertEquals("STOPPED", testService.lifecycleState().toString());

            testService.close();
            assertEquals("CLOSED", testService.lifecycleState().toString());
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private DiscoveryNode createNode(int port) throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName("127.0.0.1"), port);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("transport.stream.port", String.valueOf(port));
        attributes.put("arrow.streams.enabled", "true");

        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return new DiscoveryNode("local_node", address, attributes, roles, Version.CURRENT);
    }
}
