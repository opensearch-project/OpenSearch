/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightServiceTests extends OpenSearchTestCase {
    FeatureFlags.TestUtils.FlagWriteLock ffLock = null;

    private Settings settings;
    private ClusterService clusterService;
    private NetworkService networkService;
    private ThreadPool threadPool;
    private final AtomicInteger port = new AtomicInteger(0);
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ffLock = new FeatureFlags.TestUtils.FlagWriteLock(ARROW_STREAMS);
        int availablePort = getBasePort(9500) + port.addAndGet(1);
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
        when(threadPool.executor(ServerConfig.GRPC_EXECUTOR_THREAD_POOL_NAME)).thenReturn(mock(ExecutorService.class));

        networkService = new NetworkService(Collections.emptyList());
    }

    public void testInitializeWithSslDisabled() throws Exception {

        Settings noSslSettings = Settings.builder().put("arrow.ssl.enable", false).build();
        ServerConfig.init(noSslSettings);

        try (FlightService noSslService = new FlightService(noSslSettings)) {
            noSslService.setClusterService(clusterService);
            noSslService.setThreadPool(threadPool);
            noSslService.setClient(mock(Client.class));
            noSslService.setNetworkService(networkService);
            noSslService.start();
            SslContextProvider sslContextProvider = noSslService.getSslContextProvider();
            assertNull("SSL context provider should be null", sslContextProvider);
            assertNotNull(noSslService.getFlightClientManager());
            assertNotNull(noSslService.getBoundAddress());
        }
    }

    public void testStartAndStop() throws Exception {
        ServerConfig.init(settings);

        try (FlightService testService = new FlightService(Settings.EMPTY)) {
            testService.setClusterService(clusterService);
            testService.setThreadPool(threadPool);
            testService.setClient(mock(Client.class));
            testService.setNetworkService(networkService);
            testService.start();
            testService.stop();
            testService.start();
            assertNotNull(testService.getStreamManager());
        }
    }

    public void testInitializeWithoutSecureTransportSettingsProvider() {
        Settings sslSettings = Settings.builder().put(settings).put("flight.ssl.enable", true).build();
        ServerConfig.init(sslSettings);
        try (FlightService sslService = new FlightService(sslSettings)) {
            // Should throw exception when initializing without provider
            expectThrows(RuntimeException.class, () -> {
                sslService.setClusterService(clusterService);
                sslService.setThreadPool(threadPool);
                sslService.setClient(mock(Client.class));
                sslService.setNetworkService(networkService);
                sslService.start();
            });
        }
    }

    public void testServerStartupFailure() {
        Settings invalidSettings = Settings.builder()
            .put(ServerComponents.SETTING_FLIGHT_PUBLISH_PORT.getKey(), "-100") // Invalid port
            .build();
        ServerConfig.init(invalidSettings);

        try (FlightService invalidService = new FlightService(invalidSettings)) {
            invalidService.setClusterService(clusterService);
            invalidService.setThreadPool(threadPool);
            invalidService.setClient(mock(Client.class));
            invalidService.setNetworkService(networkService);
            expectThrows(RuntimeException.class, () -> { invalidService.doStart(); });
        }
    }

    public void testLifecycleStateTransitions() throws Exception {
        ServerConfig.init(Settings.EMPTY);
        // Find new port for this test
        try (FlightService testService = new FlightService(Settings.EMPTY)) {
            testService.setClusterService(clusterService);
            testService.setThreadPool(threadPool);
            testService.setClient(mock(Client.class));
            testService.setNetworkService(networkService);
            // Test all state transitions
            testService.start();
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
        ffLock.close();
    }

    private DiscoveryNode createNode(int port) throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName("127.0.0.1"), port);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("arrow.streams.enabled", "true");

        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return new DiscoveryNode("local_node", address, attributes, roles, Version.CURRENT);
    }
}
