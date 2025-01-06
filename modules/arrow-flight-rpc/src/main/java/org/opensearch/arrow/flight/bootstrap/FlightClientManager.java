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
import org.apache.arrow.util.VisibleForTesting;
import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.FeatureFlags;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import io.netty.channel.EventLoopGroup;

import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS_SETTING;

/**
 * Manages Flight client connections to OpenSearch nodes in a cluster.
 * This class maintains a pool of Flight clients for internode communication,
 * handles client lifecycle, and responds to cluster state changes.
 *
 * <p>The manager implements ClusterStateListener to automatically update
 * client connections when nodes join or leave the cluster. </p>
 */
public class FlightClientManager implements ClusterStateListener, AutoCloseable {
    private static final Version MIN_SUPPORTED_VERSION = Version.fromString("3.0.0");

    private final ClientPool clientPool;
    private final ClientConfiguration clientConfig;

    /**
     * Creates a new FlightClientManager instance.
     *
     * @param allocator Supplier for buffer allocation
     * @param clusterService Service for cluster state management
     * @param sslContextProvider Provider for SSL/TLS context configuration
     * @param elg Event loop group for network operations
     * @param grpcExecutor Executor service for gRPC operations
     */
    public FlightClientManager(
        BufferAllocator allocator,
        ClusterService clusterService,
        SslContextProvider sslContextProvider,
        EventLoopGroup elg,
        ExecutorService grpcExecutor
    ) {
        this.clientConfig = new ClientConfiguration(
            Objects.requireNonNull(allocator, "BufferAllocator cannot be null"),
            Objects.requireNonNull(clusterService, "ClusterService cannot be null"),
            Objects.requireNonNull(sslContextProvider, "SslContextProvider cannot be null"),
            Objects.requireNonNull(elg, "EventLoopGroup cannot be null"),
            Objects.requireNonNull(grpcExecutor, "ExecutorService cannot be null")
        );
        this.clientPool = new ClientPool();
        clusterService.addListener(this);
    }

    /**
     * Returns a Flight client for a given node ID.
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
    public OSFlightClient getFlightClient(String nodeId) {
        FlightClientHolder clientHolder = clientPool.getOrCreateClient(nodeId, this::buildFlightClient);
        return clientHolder == null ? null : clientHolder.flightClient;
    }

    /**
     * Returns the location of a Flight client for a given node ID.
     * @param nodeId The ID of the node for which to retrieve the location
     * @return The Location of the Flight client for the specified node
     */
    public Location getFlightClientLocation(String nodeId) {
        FlightClientHolder clientHolder = clientPool.getOrCreateClient(nodeId, this::buildFlightClient);
        return clientHolder == null ? null : clientHolder.location;
    }

    /**
     * Returns the ID of the local node in the cluster.
     *
     * @return String representing the local node ID
     */
    public String getLocalNodeId() {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().getLocalNodeId();
    }

    /**
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        clientPool.close();
    }

    private FlightClientHolder buildFlightClient(String nodeId) {
        DiscoveryNode node = getNodeFromCluster(nodeId);
        if (!isValidNode(node)) {
            return null;
        }

        Location location = createClientLocation(node);
        OSFlightClient client = buildClient(location);
        return new FlightClientHolder(client, location);
    }

    private DiscoveryNode getNodeFromCluster(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

    private boolean isValidNode(DiscoveryNode node) {
        return node != null && !node.getVersion().before(MIN_SUPPORTED_VERSION) && FeatureFlags.isEnabled(ARROW_STREAMS_SETTING);
    }

    private Location createClientLocation(DiscoveryNode node) {
        String address = node.getAddress().getAddress();
        int clientPort = Integer.parseInt(node.getAttributes().get("transport.stream.port"));
        return clientConfig.sslContextProvider.isSslEnabled()
            ? Location.forGrpcTls(address, clientPort)
            : Location.forGrpcInsecure(address, clientPort);
    }

    private OSFlightClient buildClient(Location location) {
        return OSFlightClient.builder(
            clientConfig.allocator,
            location,
            ServerConfig.clientChannelType(),
            clientConfig.grpcExecutor,
            clientConfig.workerELG,
            clientConfig.sslContextProvider.getClientSslContext()
        ).build();
    }

    /**
     * Initializes the Flight clients for the current cluster state.
     * @param event The ClusterChangedEvent containing the current cluster state
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            updateFlightClients();
        }
    }

    @VisibleForTesting
    void updateFlightClients() {
        Set<String> currentNodes = getCurrentClusterNodes();
        clientPool.removeStaleClients(currentNodes);
        initializeFlightClients();
    }

    private Set<String> getCurrentClusterNodes() {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().getNodes().keySet();
    }

    private void initializeFlightClients() {
        for (DiscoveryNode node : Objects.requireNonNull(clientConfig.clusterService).state().nodes()) {
            getFlightClient(node.getId());
        }
    }

    @VisibleForTesting
    Map<String, FlightClientHolder> getFlightClients() {
        return clientPool.getClients();
    }

    /**
     * Holds configuration for Flight client creation
     */
    private static class ClientConfiguration {
        private final BufferAllocator allocator;
        private final ClusterService clusterService;
        private final SslContextProvider sslContextProvider;
        private final EventLoopGroup workerELG;
        private final ExecutorService grpcExecutor;

        ClientConfiguration(
            BufferAllocator allocator,
            ClusterService clusterService,
            SslContextProvider sslContextProvider,
            EventLoopGroup workerELG,
            ExecutorService grpcExecutor
        ) {
            this.allocator = allocator;
            this.clusterService = clusterService;
            this.sslContextProvider = sslContextProvider;
            this.workerELG = workerELG;
            this.grpcExecutor = grpcExecutor;
        }
    }

    /**
     * Manages the pool of Flight clients
     */
    private static class ClientPool implements AutoCloseable {
        private final Map<String, FlightClientHolder> flightClients = new ConcurrentHashMap<>();

        FlightClientHolder getOrCreateClient(String nodeId, Function<String, FlightClientHolder> clientBuilder) {
            return flightClients.computeIfAbsent(nodeId, clientBuilder);
        }

        void removeStaleClients(Set<String> currentNodes) {
            flightClients.keySet().removeIf(nodeId -> !currentNodes.contains(nodeId));
        }

        Map<String, FlightClientHolder> getClients() {
            return flightClients;
        }

        @Override
        public void close() throws Exception {
            for (FlightClientHolder clientHolder : flightClients.values()) {
                clientHolder.flightClient.close();
            }
            flightClients.clear();
        }
    }

    /**
     * Holds a Flight client and its associated location
     */
    private static class FlightClientHolder {
        final OSFlightClient flightClient;
        final Location location;

        FlightClientHolder(OSFlightClient flightClient, Location location) {
            this.flightClient = Objects.requireNonNull(flightClient, "FlightClient cannot be null");
            this.location = Objects.requireNonNull(location, "Location cannot be null");
        }
    }
}
