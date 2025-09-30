/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.flight.OSFlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.BindTransportException;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.InboundHandler;
import org.opensearch.transport.OutboundHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpServerChannel;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportHandshaker;
import org.opensearch.transport.TransportKeepAlive;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import static org.opensearch.arrow.flight.bootstrap.ServerComponents.SETTING_FLIGHT_BIND_HOST;
import static org.opensearch.arrow.flight.bootstrap.ServerComponents.SETTING_FLIGHT_PORTS;
import static org.opensearch.arrow.flight.bootstrap.ServerComponents.SETTING_FLIGHT_PUBLISH_HOST;
import static org.opensearch.arrow.flight.bootstrap.ServerComponents.SETTING_FLIGHT_PUBLISH_PORT;

@SuppressWarnings("removal")
class FlightTransport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(FlightTransport.class);
    private static final String DEFAULT_PROFILE = "stream_profile";

    private final PortsRange portRange;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private volatile BoundTransportAddress boundAddress;
    private volatile FlightServer flightServer;
    private final SslContextProvider sslContextProvider;
    private FlightProducer flightProducer;
    private final ConcurrentMap<String, ClientHolder> flightClients = new ConcurrentHashMap<>();
    private final EventLoopGroup bossEventLoopGroup;
    private final EventLoopGroup workerEventLoopGroup;
    private final ExecutorService serverExecutor;
    private final ExecutorService clientExecutor;
    private final ExecutorService[] flightEventLoopGroup;
    private final AtomicInteger nextExecutorIndex = new AtomicInteger(0);

    private final ThreadPool threadPool;
    private RootAllocator rootAllocator;
    private BufferAllocator serverAllocator;
    private BufferAllocator clientAllocator;

    private final NamedWriteableRegistry namedWriteableRegistry;
    private final FlightStatsCollector statsCollector;
    private final FlightTransportConfig config = new FlightTransportConfig();

    final FlightServerMiddleware.Key<ServerHeaderMiddleware> SERVER_HEADER_KEY = FlightServerMiddleware.Key.of(
        "flight-server-header-middleware"
    );

    private record ClientHolder(Location location, FlightClient flightClient, HeaderContext context) {
    }

    public FlightTransport(
        Settings settings,
        Version version,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        Tracer tracer,
        SslContextProvider sslContextProvider,
        FlightStatsCollector statsCollector
    ) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService, tracer);
        this.portRange = SETTING_FLIGHT_PORTS.get(settings);
        this.bindHosts = SETTING_FLIGHT_BIND_HOST.get(settings).toArray(new String[0]);
        this.publishHosts = SETTING_FLIGHT_PUBLISH_HOST.get(settings).toArray(new String[0]);
        this.sslContextProvider = sslContextProvider;
        this.statsCollector = statsCollector;
        this.bossEventLoopGroup = createEventLoopGroup("os-grpc-boss-ELG", 1);
        this.workerEventLoopGroup = createEventLoopGroup("os-grpc-worker-ELG", Runtime.getRuntime().availableProcessors() * 2);
        this.serverExecutor = threadPool.executor(ServerConfig.GRPC_EXECUTOR_THREAD_POOL_NAME);
        this.clientExecutor = threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME);
        this.threadPool = threadPool;
        this.namedWriteableRegistry = namedWriteableRegistry;

        // Create Flight event loop group for request processing
        int eventLoopCount = ServerConfig.getEventLoopThreads();
        this.flightEventLoopGroup = new ExecutorService[eventLoopCount];
        for (int i = 0; i < eventLoopCount; i++) {
            int finalI = i;
            flightEventLoopGroup[i] = Executors.newSingleThreadExecutor(r -> new Thread(r, "flight-eventloop-" + finalI));
        }
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            rootAllocator = AccessController.doPrivileged((PrivilegedAction<RootAllocator>) () -> new RootAllocator(Integer.MAX_VALUE));
            serverAllocator = rootAllocator.newChildAllocator("server", 0, rootAllocator.getLimit());
            clientAllocator = rootAllocator.newChildAllocator("client", 0, rootAllocator.getLimit());
            if (statsCollector != null) {
                statsCollector.setBufferAllocator(rootAllocator);
                statsCollector.setThreadPool(threadPool);
            }
            flightProducer = new ArrowFlightProducer(this, rootAllocator, SERVER_HEADER_KEY, statsCollector);
            bindServer();
            success = true;
            if (statsCollector != null) {
                statsCollector.incrementServerChannelsActive();
            }
        } finally {
            if (!success) {
                doStop();
            }
        }
    }

    private void bindServer() {
        InetAddress[] hostAddresses;
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<InetSocketAddress> boundAddresses = bindToPort(hostAddresses);
        List<TransportAddress> transportAddresses = boundAddresses.stream().map(TransportAddress::new).collect(Collectors.toList());

        InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        int publishPort = Transport.resolveTransportPublishPort(
            SETTING_FLIGHT_PUBLISH_PORT.get(settings),
            transportAddresses,
            publishInetAddress
        );
        if (publishPort < 0) {
            throw new BindTransportException(
                "Failed to auto-resolve flight publish port, multiple bound addresses "
                    + transportAddresses
                    + " with distinct ports and none matched the publish address ("
                    + publishInetAddress
                    + ")."
            );
        }

        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(transportAddresses.toArray(new TransportAddress[0]), publishAddress);
    }

    private List<InetSocketAddress> bindToPort(InetAddress[] hostAddresses) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final List<InetSocketAddress> boundAddresses = new ArrayList<>();
        final List<Location> locations = new ArrayList<>();

        boolean success = portRange.iterate(portNumber -> {
            try {
                boundAddresses.clear();
                locations.clear();

                // Try to bind all addresses on the same port
                for (InetAddress hostAddress : hostAddresses) {
                    InetSocketAddress socketAddress = new InetSocketAddress(hostAddress, portNumber);
                    boundAddresses.add(socketAddress);

                    Location location = sslContextProvider != null
                        ? Location.forGrpcTls(NetworkAddress.format(hostAddress), portNumber)
                        : Location.forGrpcInsecure(NetworkAddress.format(hostAddress), portNumber);
                    locations.add(location);
                }

                // Create single FlightServer with all locations
                ServerHeaderMiddleware.Factory factory = new ServerHeaderMiddleware.Factory();
                OSFlightServer.Builder builder = OSFlightServer.builder()
                    .allocator(serverAllocator)
                    .producer(flightProducer)
                    .sslContext(sslContextProvider != null ? sslContextProvider.getServerSslContext() : null)
                    .channelType(ServerConfig.serverChannelType())
                    .bossEventLoopGroup(bossEventLoopGroup)
                    .workerEventLoopGroup(workerEventLoopGroup)
                    .executor(serverExecutor)
                    .middleware(SERVER_HEADER_KEY, factory);

                builder.location(locations.get(0));
                for (int i = 1; i < locations.size(); i++) {
                    builder.addListenAddress(locations.get(i));
                }

                FlightServer server = builder.build();
                server.start();
                this.flightServer = server;
                logger.info("Arrow Flight server started. Listening at {}", locations);
                return true;
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
        });

        if (!success) {
            throw new BindTransportException("Failed to bind to " + Arrays.toString(hostAddresses) + ":" + portRange, lastException.get());
        }

        return new ArrayList<>(boundAddresses);
    }

    @Override
    protected void stopInternal() {
        try {
            if (flightServer != null) {
                flightServer.shutdown();
                flightServer.awaitTermination();
                flightServer.close();
                flightServer = null;
            }
            serverAllocator.close();
            for (ClientHolder holder : flightClients.values()) {
                holder.flightClient().close();
            }
            flightClients.clear();
            clientAllocator.close();
            rootAllocator.close();
            gracefullyShutdownELG(bossEventLoopGroup, "os-grpc-boss-ELG");
            gracefullyShutdownELG(workerEventLoopGroup, "os-grpc-worker-ELG");

            for (ExecutorService executor : flightEventLoopGroup) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            if (statsCollector != null) {
                statsCollector.decrementServerChannelsActive();
            }
        } catch (Exception e) {
            logger.error("Error stopping FlightTransport", e);
        }
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

    @Override
    protected TcpServerChannel bind(String name, InetSocketAddress address) {
        return null; // we don't need to bind anything here
    }

    @Override
    protected TcpChannel initiateChannel(DiscoveryNode node) throws IOException {
        String nodeId = node.getId();
        ClientHolder holder = flightClients.computeIfAbsent(nodeId, id -> {
            TransportAddress publishAddress = node.getStreamAddress();
            String address = publishAddress.getAddress();
            int flightPort = publishAddress.address().getPort();
            // TODO: check feasibility of GRPC_DOMAIN_SOCKET for local connections
            // This would require server to addListener on GRPC_DOMAIN_SOCKET
            Location location = sslContextProvider != null
                ? Location.forGrpcTls(address, flightPort)
                : Location.forGrpcInsecure(address, flightPort);
            HeaderContext context = new HeaderContext();
            ClientHeaderMiddleware.Factory factory = new ClientHeaderMiddleware.Factory(context, getVersion());
            FlightClient client = OSFlightClient.builder()
                // TODO configure initial and max reservation setting per client
                .allocator(clientAllocator)
                .location(location)
                .channelType(ServerConfig.clientChannelType())
                .eventLoopGroup(workerEventLoopGroup)
                .sslContext(sslContextProvider != null ? sslContextProvider.getClientSslContext() : null)
                .executor(clientExecutor)
                .intercept(factory)
                .build();
            return new ClientHolder(location, client, context);
        });
        FlightClientChannel channel = new FlightClientChannel(
            boundAddress,
            holder.flightClient(),
            node,
            holder.location(),
            holder.context(),
            DEFAULT_PROFILE,
            getResponseHandlers(),
            threadPool,
            this.inboundHandler.getMessageListener(),
            namedWriteableRegistry,
            statsCollector,
            config
        );

        return channel;
    }

    @Override
    public void setSlowLogThreshold(TimeValue slowLogThreshold) {
        super.setSlowLogThreshold(slowLogThreshold);
        config.setSlowLogThreshold(slowLogThreshold);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener) {
        try {
            ensureOpen();
            TcpChannel channel = initiateChannel(node);
            List<TcpChannel> channels = Collections.singletonList(channel);
            NodeChannels nodeChannels = new NodeChannels(node, channels, profile, getVersion());
            listener.onResponse(nodeChannels);
        } catch (Exception e) {
            listener.onFailure(new ConnectTransportException(node, "Failed to open Flight connection", e));
        }
    }

    @Override
    protected InboundHandler createInboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        TransportKeepAlive keepAlive,
        RequestHandlers requestHandlers,
        ResponseHandlers responseHandlers,
        Tracer tracer
    ) {
        return new FlightInboundHandler(
            nodeName,
            version,
            features,
            statsTracker,
            threadPool,
            bigArrays,
            outboundHandler,
            namedWriteableRegistry,
            handshaker,
            keepAlive,
            requestHandlers,
            responseHandlers,
            tracer
        );
    }

    private EventLoopGroup createEventLoopGroup(String name, int threads) {
        return new NioEventLoopGroup(threads);
    }

    private void gracefullyShutdownELG(EventLoopGroup group, String name) {
        if (group != null) {
            group.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
    }

    /**
     * Gets the next executor for round-robin distribution
     */
    public ExecutorService getNextFlightExecutor() {
        return flightEventLoopGroup[nextExecutorIndex.getAndIncrement() % flightEventLoopGroup.length];
    }
}
