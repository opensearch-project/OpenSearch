/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OSFlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.netty.channel.EventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.Future;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;
import static org.opensearch.transport.AuxTransport.AUX_TRANSPORT_PORT;
import static org.opensearch.transport.Transport.resolveTransportPublishPort;

/**
 * Server components for Arrow Flight RPC integration with OpenSearch.
 * Manages the lifecycle of Flight server instances and their configuration.
 * @opensearch.internal
 */
@SuppressWarnings("removal")
public final class ServerComponents implements AutoCloseable {

    /**
     * Setting for Arrow Flight host addresses.
     */
    public static final Setting<List<String>> SETTING_FLIGHT_HOST = listSetting(
        "arrow.flight.host",
        emptyList(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Setting for Arrow Flight bind host addresses.
     */
    public static final Setting<List<String>> SETTING_FLIGHT_BIND_HOST = listSetting(
        "arrow.flight.bind_host",
        SETTING_FLIGHT_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Setting for Arrow Flight publish host addresses.
     */
    public static final Setting<List<String>> SETTING_FLIGHT_PUBLISH_HOST = listSetting(
        "arrow.flight.publish_host",
        SETTING_FLIGHT_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Setting for Arrow Flight publish port.
     */
    public static final Setting<Integer> SETTING_FLIGHT_PUBLISH_PORT = intSetting(
        "arrow.flight.publish_port",
        -1,
        -1,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(ServerComponents.class);

    private static final String GRPC_WORKER_ELG = "os-grpc-worker-ELG";
    private static final String GRPC_BOSS_ELG = "os-grpc-boss-ELG";
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    /**
     * The setting key for Flight transport configuration.
     */
    public static final String FLIGHT_TRANSPORT_SETTING_KEY = "transport-flight";

    /**
     * Setting for Arrow Flight port range.
     */
    public static final Setting<PortsRange> SETTING_FLIGHT_PORTS = AUX_TRANSPORT_PORT.getConcreteSettingForNamespace(
        FLIGHT_TRANSPORT_SETTING_KEY
    );

    private final Settings settings;
    private final PortsRange port;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private volatile BoundTransportAddress boundAddress;

    private FlightServer server;
    private BufferAllocator allocator;
    ClusterService clusterService;
    private NetworkService networkService;
    private ThreadPool threadPool;
    private SslContextProvider sslContextProvider;
    private FlightProducer flightProducer;

    private EventLoopGroup bossEventLoopGroup;
    EventLoopGroup workerEventLoopGroup;
    private ExecutorService serverExecutor;
    private ExecutorService grpcExecutor;

    ServerComponents(Settings settings) {
        this.settings = settings;
        this.port = SETTING_FLIGHT_PORTS.get(settings);

        List<String> bindHosts = SETTING_FLIGHT_BIND_HOST.get(settings);
        this.bindHosts = bindHosts.toArray(new String[0]);

        List<String> publishHosts = SETTING_FLIGHT_PUBLISH_HOST.get(settings);
        this.publishHosts = publishHosts.toArray(new String[0]);
    }

    void setAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    void setClusterService(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    void setNetworkService(NetworkService networkService) {
        this.networkService = Objects.requireNonNull(networkService);
    }

    void setThreadPool(ThreadPool threadPool) {
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    void setSslContextProvider(@Nullable SslContextProvider sslContextProvider) {
        this.sslContextProvider = sslContextProvider;
    }

    void setFlightProducer(FlightProducer flightProducer) {
        this.flightProducer = Objects.requireNonNull(flightProducer);
    }

    private FlightServer buildAndStartServer(Location location, FlightProducer producer) throws IOException {
        FlightServer server = OSFlightServer.builder()
            .allocator(allocator)
            .location(location)
            .producer(producer)
            .sslContext(sslContextProvider != null ? sslContextProvider.getServerSslContext() : null)
            .channelType(ServerConfig.serverChannelType())
            .bossEventLoopGroup(bossEventLoopGroup)
            .workerEventLoopGroup(workerEventLoopGroup)
            .executor(grpcExecutor)
            .build();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                server.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        return server;
    }

    SslContextProvider getSslContextProvider() {
        return sslContextProvider;
    }

    BoundTransportAddress getBoundAddress() {
        return boundAddress;
    }

    void start() {
        InetAddress[] hostAddresses;
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<TransportAddress> boundAddresses = new ArrayList<>(hostAddresses.length);
        for (InetAddress address : hostAddresses) {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                boundAddresses.add(bindAddress(address, port));
                return null;
            });
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolveTransportPublishPort(SETTING_FLIGHT_PUBLISH_PORT.get(settings), boundAddresses, publishInetAddress);

        if (publishPort < 0) {
            throw new BindTransportException(
                "Failed to auto-resolve flight publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). Please specify a unique port by setting "
                    + SETTING_FLIGHT_PUBLISH_PORT.getKey()
            );
        }

        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), publishAddress);
    }

    void initComponents() throws Exception {
        bossEventLoopGroup = ServerConfig.createELG(GRPC_BOSS_ELG, 1);
        workerEventLoopGroup = ServerConfig.createELG(GRPC_WORKER_ELG, NettyRuntime.availableProcessors() * 2);
        serverExecutor = threadPool.executor(ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME);
        grpcExecutor = threadPool.executor(ServerConfig.GRPC_EXECUTOR_THREAD_POOL_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            AutoCloseables.close(server);
            gracefullyShutdownELG(bossEventLoopGroup, GRPC_BOSS_ELG);
            gracefullyShutdownELG(workerEventLoopGroup, GRPC_WORKER_ELG);
            if (serverExecutor != null) {
                serverExecutor.shutdown();
            }
            if (grpcExecutor != null) {
                grpcExecutor.shutdown();
            }
        } catch (Exception e) {
            logger.error("Error while closing server components", e);
        }
    }

    private TransportAddress bindAddress(final InetAddress hostAddress, final PortsRange portsRange) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        final TransportAddress[] address = new TransportAddress[1];
        boolean success = portsRange.iterate(portNumber -> {
            boundSocket.set(new InetSocketAddress(hostAddress, portNumber));
            address[0] = new TransportAddress(boundSocket.get());
            try {
                return startFlightServer(address[0]);
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
        });

        if (!success) {
            throw new BindTransportException("Failed to bind to [" + hostAddress + "]", lastException.get());
        }
        return address[0];
    }

    private boolean startFlightServer(TransportAddress transportAddress) {
        InetSocketAddress address = transportAddress.address();
        Location serverLocation = sslContextProvider != null
            ? Location.forGrpcTls(address.getHostString(), address.getPort())
            : Location.forGrpcInsecure(address.getHostString(), address.getPort());
        try {
            this.server = buildAndStartServer(serverLocation, flightProducer);
            logger.info("Arrow Flight server started. Listening at {}", serverLocation);
            return true;
        } catch (Exception e) {
            String errorMsg = "Failed to start Arrow Flight server at " + serverLocation;
            logger.debug(errorMsg, e);
            return false;
        }
    }

    private void gracefullyShutdownELG(EventLoopGroup group, String groupName) {
        if (group != null) {
            Future<?> shutdownFuture = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            shutdownFuture.awaitUninterruptibly();
            if (!shutdownFuture.isSuccess()) {
                logger.warn("Error closing {} netty event loop group {}", groupName, shutdownFuture.cause());
            }
        }
    }
}
