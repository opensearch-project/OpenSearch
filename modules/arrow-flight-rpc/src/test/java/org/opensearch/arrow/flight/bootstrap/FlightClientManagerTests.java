/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.netty.GrpcSslContexts;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.NettyRuntime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightClientManagerTests extends OpenSearchTestCase {

    private static BufferAllocator allocator;
    private static EventLoopGroup elg;
    private static ExecutorService executorService;
    private static final AtomicInteger port = new AtomicInteger(0);

    private ClusterService clusterService;
    private ClusterState state;
    private FlightClientManager clientManager;

    @BeforeClass
    public static void setupClass() throws Exception {
        ServerConfig.init(Settings.EMPTY);
        allocator = new RootAllocator();
        elg = ServerConfig.createELG("test-grpc-worker-elg", NettyRuntime.availableProcessors() * 2);
        executorService = ServerConfig.createELG("test-grpc-worker", NettyRuntime.availableProcessors() * 2);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
        clusterService = mock(ClusterService.class);
        state = getDefaultState();
        when(clusterService.state()).thenReturn(state);
        SslContextProvider sslContextProvider = mock(SslContextProvider.class);
        // Create a proper gRPC client SSL context with ALPN and HTTP/2 support
        SslContext clientSslContext = GrpcSslContexts.configure(SslContextBuilder.forClient()).build();
        when(sslContextProvider.isSslEnabled()).thenReturn(true);
        when(sslContextProvider.getClientSslContext()).thenReturn(clientSslContext);
        clientManager = new FlightClientManager(allocator, clusterService, sslContextProvider, elg, executorService);
        clientManager.updateFlightClients();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clientManager.close();
    }

    private ClusterState getDefaultState() throws Exception {
        int testPort = getBasePort() + port.addAndGet(2);

        DiscoveryNode localNode = createNode("local_node", "127.0.0.1", testPort);
        DiscoveryNode remoteNode = createNode("remote_node", "127.0.0.2", testPort + 1);

        // Setup initial cluster state
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(remoteNode);
        nodesBuilder.add(localNode);
        nodesBuilder.localNodeId(localNode.getId());
        DiscoveryNodes nodes = nodesBuilder.build();

        return ClusterState.builder(new ClusterName("test")).nodes(nodes).build();
    }

    private DiscoveryNode createNode(String nodeId, String host, int port) throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName(host), port);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("transport.stream.port", String.valueOf(port));
        attributes.put("arrow.streams.enabled", "true");
        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return new DiscoveryNode(nodeId, address, attributes, roles, Version.CURRENT);
    }

    @AfterClass
    public static void tearClass() {
        allocator.close();
    }

    public void testGetFlightClientForExistingNode() {
        validateNodes();
    }

    public void testGetFlightClientLocation() {
        for (DiscoveryNode node : state.nodes()) {
            Location location = clientManager.getFlightClientLocation(node.getId());
            assertNotNull("Flight client location should be returned", location);
            assertEquals("Location host should match", node.getHostAddress(), location.getUri().getHost());
        }
    }

    public void testGetFlightClientForNonExistentNode() throws Exception {
        assertNull(clientManager.getFlightClient("non_existent_node"));
    }

    public void testClusterChangedWithNodesChanged() throws Exception {
        DiscoveryNode newNode = createNode("new_node", "127.0.0.3", getBasePort() + port.addAndGet(1));
        DiscoveryNodes.Builder newNodesBuilder = DiscoveryNodes.builder();

        for (DiscoveryNode node : state.nodes()) {
            newNodesBuilder.add(node);
        }
        newNodesBuilder.localNodeId("local_node");
        // Update cluster state with new node
        newNodesBuilder.add(newNode);
        DiscoveryNodes newNodes = newNodesBuilder.build();

        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(newNodes).build();

        when(clusterService.state()).thenReturn(newState);

        clientManager.updateFlightClients();

        for (DiscoveryNode node : state.nodes()) {
            assertNotNull(clientManager.getFlightClient(node.getId()));
        }
    }

    public void testClusterChangedWithNoNodesChanged() throws Exception {
        ClusterChangedEvent event = new ClusterChangedEvent("test", state, state);
        clientManager.clusterChanged(event);

        // Verify original client still exists
        for (DiscoveryNode node : state.nodes()) {
            assertNotNull(clientManager.getFlightClient(node.getId()));
        }
    }

    public void testGetLocalNodeId() throws Exception {
        assertEquals("Local node ID should match", "local_node", clientManager.getLocalNodeId());
    }

    public void testNodeWithoutStreamPort() throws Exception {
        DiscoveryNode invalidNode = new DiscoveryNode(
            "invalid_node",
            new TransportAddress(InetAddress.getByName("127.0.0.4"), getBasePort() + port.addAndGet(1)),
            Map.of("arrow.streams.enabled", "true"),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(invalidNode);
        nodesBuilder.localNodeId("local_node");
        DiscoveryNodes nodes = nodesBuilder.build();
        ClusterState invalidState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(invalidState);

        expectThrows(NumberFormatException.class, () -> { clientManager.getFlightClient(invalidNode.getId()); });
    }

    public void testCloseWithActiveClients() throws Exception {
        for (DiscoveryNode node : state.nodes()) {
            OSFlightClient client = clientManager.getFlightClient(node.getId());
            assertNotNull(client);
        }

        clientManager.close();
        assertEquals(0, clientManager.getFlightClients().size());
    }

    public void testIncompatibleNodeVersion() throws Exception {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("transport.stream.port", String.valueOf(getBasePort() + port.addAndGet(1)));
        attributes.put("arrow.streams.enabled", "true");
        DiscoveryNode oldVersionNode = new DiscoveryNode(
            "old_version_node",
            new TransportAddress(InetAddress.getByName("127.0.0.3"), getBasePort() + port.addAndGet(1)),
            attributes,
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.fromString("2.18.0")  // Version before Arrow Flight introduction
        );

        // Update cluster state with old version node
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(oldVersionNode);
        nodesBuilder.localNodeId("local_node");
        DiscoveryNodes nodes = nodesBuilder.build();
        ClusterState oldVersionState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(oldVersionState);

        assertNull(clientManager.getFlightClient(oldVersionNode.getId()));
    }

    private void validateNodes() {
        for (DiscoveryNode node : state.nodes()) {
            OSFlightClient client = clientManager.getFlightClient(node.getId());
            assertNotNull("Flight client should be created for existing node", client);
        }
    }
}
