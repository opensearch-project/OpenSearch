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
import org.opensearch.arrow.flight.api.flightinfo.NodeFlightInfo;
import org.opensearch.arrow.flight.api.flightinfo.NodesFlightInfoAction;
import org.opensearch.arrow.flight.api.flightinfo.NodesFlightInfoRequest;
import org.opensearch.arrow.flight.api.flightinfo.NodesFlightInfoResponse;
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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;

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
    private static final Version MIN_SUPPORTED_VERSION = Version.fromString("2.19.0");
    private static final Logger logger = LogManager.getLogger(FlightClientManager.class);
    static final int LOCATION_TIMEOUT_MS = 1000;
    private final ExecutorService grpcExecutor;
    private final ClientConfiguration clientConfig;
    private final Map<String, ClientHolder> flightClients = new ConcurrentHashMap<>();
    private final Client client;
    private static final long CLIENT_BUILD_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);

    /**
     * Creates a new FlightClientManager instance.
     *
     * @param allocator          Supplier for buffer allocation
     * @param clusterService     Service for cluster state management
     * @param sslContextProvider Provider for SSL/TLS context configuration
     * @param elg                Event loop group for network operations
     * @param threadPool         Thread pool for executing tasks asynchronously
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
        clusterService.addListener(this);
    }

    /**
     * Returns the location of a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the location
     * @return The Location of the Flight client for the specified node
     */
    public Location getFlightClientLocation(String nodeId) {
        ClientHolder clientHolder = flightClients.get(nodeId);
        if (clientHolder != null && clientHolder.location != null) {
            return clientHolder.location;
        }
        buildClientAsync(nodeId);
        return null;
    }

    /**
     * Returns a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
    public Optional<OSFlightClient> getFlightClient(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        ClientHolder holder = flightClients.get(nodeId);

        if (holder == null) {
            buildClientAsync(nodeId);
            return Optional.empty();
        }

        if (holder.state == BuildState.COMPLETE) {
            return Optional.ofNullable(holder.flightClient);
        }

        if (holder.isStale()) {
            logger.warn("Detected stale building state for node [{}], triggering rebuild", nodeId);
            if (flightClients.remove(nodeId, holder)) {
                try {
                    holder.close();
                } catch (Exception e) {
                    logger.warn("Error closing stale client holder for node [{}]. {}", nodeId, e.getMessage());
                }
                buildClientAsync(nodeId);
            }
        }

        return Optional.empty();
    }

    /**
     * Represents the state and metadata of a Flight client
     */
    private record ClientHolder(OSFlightClient flightClient, Location location, long buildStartTime, BuildState state)
        implements
            AutoCloseable {

        private static ClientHolder building() {
            return new ClientHolder(null, null, System.currentTimeMillis(), BuildState.BUILDING);
        }

        private static ClientHolder complete(OSFlightClient client, Location location) {
            return new ClientHolder(client, location, System.currentTimeMillis(), BuildState.COMPLETE);
        }

        boolean isStale() {
            return state == BuildState.BUILDING && (System.currentTimeMillis() - buildStartTime) > CLIENT_BUILD_TIMEOUT_MS;
        }

        /**
         * Closes the client holder and logs the operation
         * @param nodeId The ID of the node this holder belongs to
         * @param reason The reason for closing
         */
        public void close(String nodeId, String reason) {
            try {
                if (flightClient != null) {
                    flightClient.close();
                }
                if (state == BuildState.BUILDING) {
                    logger.info("Cleaned up building state for node [{}]: {}", nodeId, reason);
                } else {
                    logger.info("Closed client for node [{}]: {}", nodeId, reason);
                }
            } catch (Exception e) {
                logger.error("Failed to close client for node [{}] ({}): {}", nodeId, reason, e.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            if (flightClient != null) {
                flightClient.close();
            }
        }
    }

    private enum BuildState {
        BUILDING,
        COMPLETE
    }

    /**
     * Initiates async build of a flight client for the given node
     */
    void buildClientAsync(String nodeId) {
        // Try to put a building placeholder
        ClientHolder placeholder = ClientHolder.building();
        if (flightClients.putIfAbsent(nodeId, placeholder) != null) {
            return; // Another thread is already handling this node
        }

        CompletableFuture<Location> locationFuture = new CompletableFuture<>();
        locationFuture.thenAccept(location -> {
            try {
                DiscoveryNode node = getNodeFromClusterState(nodeId);
                if (!isValidNode(node)) {
                    logger.warn("Node [{}] is not valid for client creation", nodeId);
                    flightClients.remove(nodeId, placeholder);
                    return;
                }

                OSFlightClient flightClient = buildClient(location);
                ClientHolder newHolder = ClientHolder.complete(flightClient, location);

                if (!flightClients.replace(nodeId, placeholder, newHolder)) {
                    // Something changed while we were building
                    logger.warn("Failed to store new client for node [{}], state changed during build", nodeId);
                    flightClient.close();
                }
            } catch (Exception e) {
                logger.error("Failed to build Flight client for node [{}]. {}", nodeId, e);
                flightClients.remove(nodeId, placeholder);
                throw new RuntimeException(e);
            }
        }).exceptionally(throwable -> {
            flightClients.remove(nodeId, placeholder);
            logger.error("Failed to get Flight server location for node [{}] {}", nodeId, throwable);
            throw new CompletionException(throwable);
        });

        requestNodeLocation(nodeId, locationFuture);
    }

    Collection<ClientHolder> getClients() {
        return flightClients.values();
    }

    private void requestNodeLocation(String nodeId, CompletableFuture<Location> future) {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest(nodeId);
        client.execute(NodesFlightInfoAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(NodesFlightInfoResponse response) {
                NodeFlightInfo nodeInfo = response.getNodesMap().get(nodeId);
                if (nodeInfo != null) {
                    TransportAddress publishAddress = nodeInfo.getBoundAddress().publishAddress();
                    String address = publishAddress.getAddress();
                    int flightPort = publishAddress.address().getPort();
                    Location location = clientConfig.sslContextProvider.isSslEnabled()
                        ? Location.forGrpcTls(address, flightPort)
                        : Location.forGrpcInsecure(address, flightPort);

                    future.complete(location);
                } else {
                    future.completeExceptionally(new IllegalStateException("No Flight info received for node: [" + nodeId + "]"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
                logger.error("Failed to get Flight server info for node: [{}] {}", nodeId, e);
            }
        });
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

    private DiscoveryNode getNodeFromClusterState(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

    /**
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        for (ClientHolder clientHolder : flightClients.values()) {
            clientHolder.close();
        }
        flightClients.clear();
        grpcExecutor.shutdown();
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
     * Handles cluster state changes by updating node locations and managing client connections.
     *
     * @param event The ClusterChangedEvent containing information about the cluster state change
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.nodesChanged()) {
            return;
        }

        final DiscoveryNodes nodes = event.state().nodes();

        cleanupStaleBuilding();
        removeStaleClients(nodes);
        updateExistingClients(nodes);
    }

    private void removeStaleClients(DiscoveryNodes nodes) {
        flightClients.entrySet().removeIf(entry -> {
            String nodeId = entry.getKey();
            ClientHolder holder = entry.getValue();

            if (!nodes.nodeExists(nodeId)) {
                holder.close(nodeId, "node no longer exists");
                return true;
            }

            if (holder.state == BuildState.BUILDING && holder.isStale()) {
                holder.close(nodeId, "client build state is stale");
                return true;
            }

            return false;
        });
    }

    /**
     * Updates clients for existing nodes based on their validity
     */
    private void updateExistingClients(DiscoveryNodes nodes) {
        for (DiscoveryNode node : nodes) {
            String nodeId = node.getId();

            if (isValidNode(node)) {
                ClientHolder existingHolder = flightClients.get(nodeId);

                if (existingHolder == null) {
                    buildClientAsync(nodeId);
                } else if (existingHolder.state == BuildState.BUILDING && existingHolder.isStale()) {
                    if (flightClients.remove(nodeId, existingHolder)) {
                        existingHolder.close(nodeId, "rebuilding stale client");
                        buildClientAsync(nodeId);
                    }
                }
            } else {
                ClientHolder holder = flightClients.remove(nodeId);
                if (holder != null) {
                    holder.close(nodeId, "node is no longer valid");
                }
            }
        }
    }

    /**
     * Cleans up any clients that are in a stale BUILDING state
     */
    private void cleanupStaleBuilding() {
        flightClients.entrySet().removeIf(entry -> {
            ClientHolder holder = entry.getValue();
            if (holder.state == BuildState.BUILDING && holder.isStale()) {
                holder.close(entry.getKey(), "cleaning up stale building state");
                return true;
            }
            return false;
        });
    }

    private static boolean isValidNode(DiscoveryNode node) {
        return node != null && !node.getVersion().before(MIN_SUPPORTED_VERSION) && FeatureFlags.isEnabled(ARROW_STREAMS_SETTING);
    }

    @VisibleForTesting
    Map<String, ClientHolder> getFlightClients() {
        return flightClients;
    }

    private record ClientConfiguration(BufferAllocator allocator, ClusterService clusterService, SslContextProvider sslContextProvider,
        EventLoopGroup workerELG, ExecutorService grpcExecutor) {
    }
}
