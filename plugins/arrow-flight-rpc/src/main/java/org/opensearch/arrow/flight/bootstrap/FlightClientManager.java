/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

import org.apache.arrow.flight.FlightClient;
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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    private static final Version MIN_SUPPORTED_VERSION = Version.V_3_0_0;
    private static final Logger logger = LogManager.getLogger(FlightClientManager.class);
    static final int LOCATION_TIMEOUT_MS = 1000;
    private final ExecutorService grpcExecutor;
    private final ClientConfiguration clientConfig;
    private final Map<String, ClientHolder> flightClients = new ConcurrentHashMap<>();
    private final Client client;
    private volatile boolean closed = false;

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
        @Nullable SslContextProvider sslContextProvider,
        EventLoopGroup elg,
        ThreadPool threadPool,
        Client client
    ) {
        grpcExecutor = threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME);
        this.clientConfig = new ClientConfiguration(
            Objects.requireNonNull(allocator, "BufferAllocator cannot be null"),
            Objects.requireNonNull(clusterService, "ClusterService cannot be null"),
            sslContextProvider,
            Objects.requireNonNull(elg, "EventLoopGroup cannot be null"),
            Objects.requireNonNull(grpcExecutor, "ExecutorService cannot be null")
        );
        this.client = Objects.requireNonNull(client, "Client cannot be null");
        clusterService.addListener(this);
    }

    /**
     * Returns a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
    public Optional<FlightClient> getFlightClient(String nodeId) {
        ClientHolder clientHolder = flightClients.get(nodeId);
        return clientHolder == null ? Optional.empty() : Optional.of(clientHolder.flightClient);
    }

    /**
     * Returns the location of a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the location
     * @return The Location of the Flight client for the specified node
     */
    public Optional<Location> getFlightClientLocation(String nodeId) {
        ClientHolder clientHolder = flightClients.get(nodeId);
        return clientHolder == null ? Optional.empty() : Optional.of(clientHolder.location);
    }

    /**
     * Builds a client for a given nodeId in asynchronous manner
     * @param nodeId nodeId of the node to build client for
     */
    public void buildClientAsync(String nodeId) {
        CompletableFuture<Location> locationFuture = new CompletableFuture<>();
        locationFuture.thenAccept(location -> {
            DiscoveryNode node = getNodeFromClusterState(nodeId);
            buildClientAndAddToPool(location, node);
        }).exceptionally(throwable -> {
            logger.error("Failed to get Flight server location for node: [{}] {}", nodeId, throwable);
            throw new RuntimeException(throwable);
        });
        requestNodeLocation(nodeId, locationFuture);
    }

    private void buildClientAndAddToPool(Location location, DiscoveryNode node) {
        if (!isValidNode(node)) {
            logger.warn(
                "Unable to build FlightClient for node [{}] with role [{}] on version [{}]",
                node.getId(),
                node.getRoles(),
                node.getVersion()
            );
            return;
        }
        if (closed) {
            return;
        }
        flightClients.computeIfAbsent(node.getId(), nodeId -> new ClientHolder(location, buildClient(location)));
    }

    private void requestNodeLocation(String nodeId, CompletableFuture<Location> future) {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest(nodeId);
        try {
            client.execute(NodesFlightInfoAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(NodesFlightInfoResponse response) {
                    NodeFlightInfo nodeInfo = response.getNodesMap().get(nodeId);
                    if (nodeInfo != null) {
                        TransportAddress publishAddress = nodeInfo.getBoundAddress().publishAddress();
                        String address = publishAddress.getAddress();
                        int flightPort = publishAddress.address().getPort();
                        Location location = clientConfig.sslContextProvider != null
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
        } catch (final Exception ex) {
            future.completeExceptionally(ex);
        }
    }

    private FlightClient buildClient(Location location) {
        return OSFlightClient.builder()
            .allocator(clientConfig.allocator)
            .location(location)
            .channelType(ServerConfig.clientChannelType())
            .eventLoopGroup(clientConfig.workerELG)
            .sslContext(clientConfig.sslContextProvider != null ? clientConfig.sslContextProvider.getClientSslContext() : null)
            .executor(clientConfig.grpcExecutor)
            .build();
    }

    private DiscoveryNode getNodeFromClusterState(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

    /**
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        for (ClientHolder clientHolder : flightClients.values()) {
            clientHolder.flightClient.close();
        }
        flightClients.clear();
        grpcExecutor.shutdown();
        if (grpcExecutor.awaitTermination(5, TimeUnit.SECONDS) == false) {
            grpcExecutor.shutdownNow();
        }
    }

    private record ClientHolder(Location location, FlightClient flightClient) {
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
        if (event.nodesChanged()) {
            DiscoveryNodes nodes = event.state().nodes();
            flightClients.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
            for (DiscoveryNode node : nodes) {
                if (!flightClients.containsKey(node.getId()) && isValidNode(node)) {
                    buildClientAsync(node.getId());
                }
            }
        }
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
        private ClientConfiguration(
            BufferAllocator allocator,
            ClusterService clusterService,
            @Nullable SslContextProvider sslContextProvider,
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
}
