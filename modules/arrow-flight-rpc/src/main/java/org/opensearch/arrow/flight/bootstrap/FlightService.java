/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.OSFlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.bootstrap.tls.DefaultSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.DisabledSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.arrow.spi.StreamTicketFactory;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.Future;

/**
 * FlightService manages the Arrow Flight server and client for OpenSearch.
 * It handles the initialization, startup, and shutdown of the Flight server and client,
 * as well as managing the stream operations through a FlightStreamManager.
 */
public class FlightService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(FlightService.class);

    // Constants
    private static final String GRPC_WORKER_ELG = "os-grpc-worker-ELG";
    private static final String GRPC_BOSS_ELG = "os-grpc-boss-ELG";
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final String TRANSPORT_STREAM_PORT = "transport.stream.port";

    private final ServerComponents serverComponents;
    private final NetworkResources networkResources;

    /**
     * Constructor for FlightService.
     * @param settings The settings for the FlightService.
     */
    public FlightService(Settings settings) {
        Objects.requireNonNull(settings, "Settings cannot be null");
        this.serverComponents = new ServerComponents();
        this.networkResources = new NetworkResources();
        initializeServerConfig(settings);
    }

    private void initializeServerConfig(Settings settings) {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                ServerConfig.init(settings);
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Arrow Flight server", e);
        }
    }

    /**
     * Initializes the FlightService with the provided ClusterService and ThreadPool.
     * It sets up the SSL context provider, client manager, and stream manager.
     * @param clusterService The ClusterService instance.
     * @param threadPool The ThreadPool instance.
     */
    public void initialize(ClusterService clusterService, ThreadPool threadPool) {
        serverComponents.setClusterService(Objects.requireNonNull(clusterService, "ClusterService cannot be null"));
        serverComponents.setThreadPool(Objects.requireNonNull(threadPool, "ThreadPool cannot be null"));
    }

    /**
     * Sets the SecureTransportSettingsProvider for the FlightService.
     * @param secureTransportSettingsProvider The SecureTransportSettingsProvider instance.
     */
    public void setSecureTransportSettingsProvider(SecureTransportSettingsProvider secureTransportSettingsProvider) {
        serverComponents.setSecureTransportSettingsProvider(
            Objects.requireNonNull(secureTransportSettingsProvider, "SecureTransportSettingsProvider cannot be null")
        );
    }

    /**
     * Starts the FlightService by initializing the stream manager.
     */
    @Override
    protected void doStart() {
        serverComponents.initializeStreamManager();
    }

    /**
     * Stops the FlightService by closing the server components and network resources.
     */
    @Override
    protected void doStop() {
        serverComponents.close();
        networkResources.close();
    }

    /**
     * doStop() ensures all resources are cleaned up and resources are recreated
     * onNodeStart()
     */
    @Override
    protected void doClose() {

    }

    /**
     * Lazily instantiates the server and networks resources and starts the FlightServer.
     * Cluster services is started and node is part of the cluster when  this method is called.
     * If the node is a dedicated cluster manager node, its a no-op as this feature isn't valid on dedicated
     * cluster manager nodes.
     * @param localNode The local node
     */
    public void onNodeStart(DiscoveryNode localNode) {
        Objects.requireNonNull(localNode, "LocalNode cannot be null");

        if (isDedicatedClusterManagerNode(localNode)) {
            doClose();
            return;
        }

        try {
            serverComponents.initialize();
            networkResources.initialize(serverComponents);
            startFlightServer(localNode);
        } catch (Exception e) {
            logger.error("Failed to start Flight server", e);
            cleanup();
            throw new RuntimeException("Failed to start Flight server", e);
        }
    }

    private void cleanup() {
        try {
            doClose();
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }

    private void startFlightServer(DiscoveryNode localNode) {
        Location serverLocation = createServerLocation(localNode);
        FlightProducer producer = serverComponents.createFlightProducer();

        try {
            OSFlightServer server = buildAndStartServer(serverLocation, producer);
            serverComponents.setServer(server);
            logger.info("Arrow Flight server started. Listening at {}", serverLocation);
        } catch (Exception e) {
            String errorMsg = "Failed to start Arrow Flight server at " + serverLocation;
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private Location createServerLocation(DiscoveryNode localNode) {
        String host = localNode.getAddress().getAddress();
        int port = Integer.parseInt(localNode.getAttributes().get(TRANSPORT_STREAM_PORT));
        return ServerConfig.getLocation(host, port);
    }

    private OSFlightServer buildAndStartServer(Location location, FlightProducer producer) throws IOException {
        OSFlightServer server = OSFlightServer.builder(
            serverComponents.getAllocator(),
            location,
            producer,
            serverComponents.getSslContextProvider().getServerSslContext(),
            ServerConfig.serverChannelType(),
            networkResources.getBossEventLoopGroup(),
            networkResources.getWorkerEventLoopGroup(),
            networkResources.getServerExecutor()
        ).build();

        server.start();
        return server;
    }

    private static boolean isDedicatedClusterManagerNode(DiscoveryNode node) {
        Set<DiscoveryNodeRole> nodeRoles = node.getRoles();
        return nodeRoles.size() == 1
            && (nodeRoles.contains(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE) || nodeRoles.contains(DiscoveryNodeRole.MASTER_ROLE));
    }

    /**
     * Retrieves the FlightClientManager used by the FlightService.
     * @return The FlightClientManager instance.
     */
    public FlightClientManager getFlightClientManager() {
        return serverComponents.getClientManager();
    }

    /**
     * Retrieves the StreamManager used by the FlightService.
     * @return The StreamManager instance.
     */
    public StreamManager getStreamManager() {
        return serverComponents.getStreamManager();
    }

    @VisibleForTesting
    SslContextProvider getSslContextProvider() {
        return serverComponents.getSslContextProvider();
    }

    @VisibleForTesting
    BufferAllocator getAllocator() {
        return serverComponents.getAllocator();
    }

    private static class ServerComponents implements AutoCloseable {
        private static final Logger logger = LogManager.getLogger(ServerComponents.class);

        private OSFlightServer server;
        private BufferAllocator allocator;
        private StreamManager streamManager;
        private FlightClientManager clientManager;
        private ClusterService clusterService;
        private ThreadPool threadPool;
        private SecureTransportSettingsProvider secureTransportSettingsProvider;
        private SslContextProvider sslContextProvider;

        void initialize() throws Exception {
            initializeAllocator();
            initializeSslContext();
        }

        void initializeStreamManager() {
            streamManager = new StreamManager() {
                @Override
                public StreamTicket registerStream(StreamProducer producer, TaskId parentTaskId) {
                    return null;
                }

                @Override
                public StreamReader getStreamReader(StreamTicket ticket) {
                    return null;
                }

                @Override
                public StreamTicketFactory getStreamTicketFactory() {
                    return null;
                }

                @Override
                public void close() {

                }
            };
        }

        private void initializeAllocator() throws Exception {
            allocator = AccessController.doPrivileged(
                (PrivilegedExceptionAction<BufferAllocator>) () -> new RootAllocator(Integer.MAX_VALUE)
            );
        }

        private void initializeSslContext() {
            sslContextProvider = ServerConfig.isSslEnabled()
                ? new DefaultSslContextProvider(secureTransportSettingsProvider)
                : new DisabledSslContextProvider();
        }

        FlightProducer createFlightProducer() {
            return new NoOpFlightProducer();
        }

        @Override
        public void close() {
            try {
                AutoCloseables.close(server, clientManager, allocator);
            } catch (Exception e) {
                logger.error("Error while closing server components", e);
            }
        }

        public BufferAllocator getAllocator() {
            return allocator;
        }

        public StreamManager getStreamManager() {
            return streamManager;
        }

        public FlightClientManager getClientManager() {
            return clientManager;
        }

        public void setClientManager(FlightClientManager clientManager) {
            this.clientManager = Objects.requireNonNull(clientManager);
        }

        public ClusterService getClusterService() {
            return clusterService;
        }

        public void setClusterService(ClusterService clusterService) {
            this.clusterService = Objects.requireNonNull(clusterService);
        }

        public ThreadPool getThreadPool() {
            return threadPool;
        }

        public void setThreadPool(ThreadPool threadPool) {
            this.threadPool = Objects.requireNonNull(threadPool);
        }

        public void setSecureTransportSettingsProvider(SecureTransportSettingsProvider provider) {
            this.secureTransportSettingsProvider = Objects.requireNonNull(provider);
        }

        public void setServer(OSFlightServer server) {
            this.server = Objects.requireNonNull(server);
        }

        public SslContextProvider getSslContextProvider() {
            return sslContextProvider;
        }
    }

    private static class NetworkResources implements AutoCloseable {
        private static final Logger logger = LogManager.getLogger(NetworkResources.class);

        private EventLoopGroup bossEventLoopGroup;
        private EventLoopGroup workerEventLoopGroup;
        private ExecutorService serverExecutor;
        private ExecutorService clientExecutor;

        void initialize(ServerComponents components) {
            initializeEventLoopGroups();
            initializeExecutors(components.getThreadPool());
            initializeClientManager(components);
        }

        private void initializeEventLoopGroups() {
            bossEventLoopGroup = ServerConfig.createELG(GRPC_BOSS_ELG, 1);
            workerEventLoopGroup = ServerConfig.createELG(GRPC_WORKER_ELG, NettyRuntime.availableProcessors() * 2);
        }

        private void initializeExecutors(ThreadPool threadPool) {
            Objects.requireNonNull(threadPool, "ThreadPool cannot be null");
            serverExecutor = threadPool.executor(ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME);
            clientExecutor = threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME);
        }

        private void initializeClientManager(ServerComponents components) {
            Objects.requireNonNull(components, "ServerComponents cannot be null");
            FlightClientManager clientManager = new FlightClientManager(
                components.getAllocator(),
                components.getClusterService(),
                components.getSslContextProvider(),
                workerEventLoopGroup,
                clientExecutor
            );
            components.setClientManager(clientManager);
        }

        @Override
        public void close() {
            closeEventLoopGroups();
            closeExecutors();
        }

        private void closeEventLoopGroups() {
            gracefullyShutdownEventLoopGroup(bossEventLoopGroup, GRPC_BOSS_ELG);
            gracefullyShutdownEventLoopGroup(workerEventLoopGroup, GRPC_WORKER_ELG);
        }

        private void gracefullyShutdownEventLoopGroup(EventLoopGroup group, String groupName) {
            if (group != null) {
                Future<?> shutdownFuture = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                shutdownFuture.awaitUninterruptibly();
                if (!shutdownFuture.isSuccess()) {
                    logger.warn("Error closing {} netty event loop group {}", groupName, shutdownFuture.cause());
                }
            }
        }

        private void closeExecutors() {
            shutdownExecutor(serverExecutor);
            shutdownExecutor(clientExecutor);
        }

        private void shutdownExecutor(ExecutorService executor) {
            if (executor != null) {
                executor.shutdown();
            }
        }

        public EventLoopGroup getBossEventLoopGroup() {
            return bossEventLoopGroup;
        }

        public EventLoopGroup getWorkerEventLoopGroup() {
            return workerEventLoopGroup;
        }

        public ExecutorService getServerExecutor() {
            return serverExecutor;
        }
    }
}
