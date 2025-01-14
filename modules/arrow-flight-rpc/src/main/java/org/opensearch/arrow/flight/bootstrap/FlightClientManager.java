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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.arrow.flight.api.NodeFlightInfo;
import org.opensearch.arrow.flight.api.NodesFlightInfoAction;
import org.opensearch.arrow.flight.api.NodesFlightInfoRequest;
import org.opensearch.arrow.flight.api.NodesFlightInfoResponse;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private static final Logger logger = LogManager.getLogger(FlightClientManager.class);
    static final int LOCATION_TIMEOUT_MS = 1000;
    private final ClientPool clientPool;
    private final ClientConfiguration clientConfig;
    private final Client client;
    private final Map<String, Location> nodeLocations = new ConcurrentHashMap<>();
    private final ExecutorService grpcExecutor;

    /**
     * Creates a new FlightClientManager instance.
     *
     * @param allocator          Supplier for buffer allocation
     * @param clusterService     Service for cluster state management
     * @param sslContextProvider Provider for SSL/TLS context configuration
     * @param elg                Event loop group for network operations
     * @param client             OpenSearch client
     */
    public FlightClientManager(
        BufferAllocator allocator,
        ClusterService clusterService,
        SslContextProvider sslContextProvider,
        EventLoopGroup elg,
        ThreadPool threadPool,
        Client client
    ) {
        grpcExecutor = threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME);
        this.clientConfig = new ClientConfiguration(
            Objects.requireNonNull(allocator, "BufferAllocator cannot be null"),
            Objects.requireNonNull(clusterService, "ClusterService cannot be null"),
            Objects.requireNonNull(sslContextProvider, "SslContextProvider cannot be null"),
            Objects.requireNonNull(elg, "EventLoopGroup cannot be null"),
            Objects.requireNonNull(grpcExecutor, "ExecutorService cannot be null")
        );
        this.client = Objects.requireNonNull(client, "Client cannot be null");
        this.clientPool = new ClientPool();
        clusterService.addListener(this);
    }

    /**
     * Returns a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
    public OSFlightClient getFlightClient(String nodeId) {
        return clientPool.getOrCreateClient(nodeId, this::buildFlightClient);
    }

    /**
     * Returns the location of a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the location
     * @return The Location of the Flight client for the specified node
     */
    public Location getFlightClientLocation(String nodeId) {
        return nodeLocations.get(nodeId);
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
        nodeLocations.clear();
        clientPool.close();
        grpcExecutor.shutdown();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            updateNodeLocations(event.state().nodes());
        }
    }

    private void updateNodeLocations(DiscoveryNodes nodes) {
        nodeLocations.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
        for (DiscoveryNode node : nodes) {
            if (!nodeLocations.containsKey(node.getId()) && isValidNode(node)) {
                CompletableFuture<Location> locationFuture = new CompletableFuture<>();
                requestNodeLocation(node, locationFuture);
                locationFuture.thenAccept(location -> { nodeLocations.put(node.getId(), location); }).exceptionally(throwable -> {
                    logger.error("Failed to get Flight server location for node: {}", node.getId(), throwable);
                    return null;
                });
            }
        }
    }

    private OSFlightClient buildFlightClient(String nodeId) {
        DiscoveryNode node = getNodeFromCluster(nodeId);
        if (!isValidNode(node)) {
            return null;
        }

        Location location = nodeLocations.get(nodeId);
        if (location != null) {
            return buildClient(location);
        }

        // If location is not available, request it
        CompletableFuture<Location> locationFuture = new CompletableFuture<>();

        requestNodeLocation(node, locationFuture);

        try {
            // Wait for a limited time to get the location
            location = locationFuture.get(LOCATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return buildClient(location);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timeout waiting for Flight server location for node: " + nodeId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Flight server location", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error getting Flight server location for node: " + nodeId, e.getCause());
        }
    }

    private void requestNodeLocation(DiscoveryNode node, CompletableFuture<Location> future) {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest(node.getId());
        client.execute(NodesFlightInfoAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(NodesFlightInfoResponse response) {
                NodeFlightInfo nodeInfo = response.getNodesMap().get(node.getId());
                if (nodeInfo != null) {
                    TransportAddress publishAddress = nodeInfo.getBoundAddress().publishAddress();
                    String address = node.getAddress().getAddress();
                    int flightPort = publishAddress.address().getPort();
                    Location location = clientConfig.sslContextProvider.isSslEnabled()
                        ? Location.forGrpcTls(address, flightPort)
                        : Location.forGrpcInsecure(address, flightPort);

                    future.complete(location);
                } else {
                    future.completeExceptionally(new IllegalStateException("No Flight info received for node: " + node.getId()));
                }
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
                logger.error("Failed to get Flight server info for node: {}", node.getId(), e);
            }
        });
    }

    private DiscoveryNode getNodeFromCluster(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

    private static boolean isValidNode(DiscoveryNode node) {
        return node != null && !node.getVersion().before(MIN_SUPPORTED_VERSION) && FeatureFlags.isEnabled(ARROW_STREAMS_SETTING);
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
    Map<String, OSFlightClient> getFlightClients() {
        return clientPool.getClients();
    }

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
        private final Map<String, OSFlightClient> flightClients = new ConcurrentHashMap<>();

        OSFlightClient getOrCreateClient(String nodeId, Function<String, OSFlightClient> clientBuilder) {
            return flightClients.computeIfAbsent(nodeId, clientBuilder);
        }

        void removeStaleClients(Set<String> currentNodes) {
            flightClients.keySet().removeIf(nodeId -> !currentNodes.contains(nodeId));
        }

        Map<String, OSFlightClient> getClients() {
            return flightClients;
        }

        @Override
        public void close() throws Exception {
            for (OSFlightClient flightClient : flightClients.values()) {
                flightClient.close();
            }
            flightClients.clear();
        }
    }
}
