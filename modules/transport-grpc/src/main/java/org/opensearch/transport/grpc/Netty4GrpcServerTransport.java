/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;
import org.opensearch.transport.BindTransportException;
import org.opensearch.transport.grpc.interceptor.GrpcInterceptorChain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;
import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;
import static org.opensearch.transport.Transport.resolveTransportPublishPort;
import static org.opensearch.transport.grpc.GrpcPlugin.GRPC_THREAD_POOL_NAME;

/**
 * Netty4 gRPC server implemented as a LifecycleComponent.
 * Services injected through BindableService list.
 */
public class Netty4GrpcServerTransport extends AuxTransport {
    private static final Logger logger = LogManager.getLogger(Netty4GrpcServerTransport.class);

    /**
     * Type key for configuring settings of this auxiliary transport.
     */
    public static final String GRPC_TRANSPORT_SETTING_KEY = "transport-grpc";

    /**
     * Port range on which to bind.
     * Note this setting is configured through AffixSetting AUX_TRANSPORT_PORT where the aux transport type matches the GRPC_TRANSPORT_SETTING_KEY.
     */
    public static final Setting<PortsRange> SETTING_GRPC_PORT = AUX_TRANSPORT_PORT.getConcreteSettingForNamespace(
        GRPC_TRANSPORT_SETTING_KEY
    );

    /**
     * Port published to peers for this server.
     */
    public static final Setting<Integer> SETTING_GRPC_PUBLISH_PORT = intSetting("grpc.publish_port", -1, -1, Setting.Property.NodeScope);

    /**
     * Host list to bind and publish.
     * For distinct bind/publish hosts configure SETTING_GRPC_BIND_HOST + SETTING_GRPC_PUBLISH_HOST separately.
     */
    public static final Setting<List<String>> SETTING_GRPC_HOST = listSetting(
        "grpc.host",
        emptyList(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Host list to bind.
     */
    public static final Setting<List<String>> SETTING_GRPC_BIND_HOST = listSetting(
        "grpc.bind_host",
        SETTING_GRPC_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Host list published to peers.
     */
    public static final Setting<List<String>> SETTING_GRPC_PUBLISH_HOST = listSetting(
        "grpc.publish_host",
        SETTING_GRPC_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * Configure size of thread pool backing this transport server.
     */
    public static final Setting<Integer> SETTING_GRPC_WORKER_COUNT = new Setting<>(
        "grpc.netty.worker_count",
        (s) -> Integer.toString(OpenSearchExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "grpc.netty.worker_count"),
        Setting.Property.NodeScope
    );

    /**
     * Configure size of executor thread pool for handling gRPC calls.
     */
    public static final Setting<Integer> SETTING_GRPC_EXECUTOR_COUNT = new Setting<>(
        "grpc.netty.executor_count",
        (s) -> Integer.toString(OpenSearchExecutors.allocatedProcessors(s) * 2),
        (s) -> Setting.parseInt(s, 1, "grpc.netty.executor_count"),
        Setting.Property.NodeScope
    );

    /**
     * Controls the number of allowed simultaneous in flight requests a single client connection may send.
     */
    public static final Setting<Integer> SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS = Setting.intSetting(
        "grpc.netty.max_concurrent_connection_calls",
        100,
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope
    );

    /**
     * Configure maximum inbound message size in bytes.
     */
    public static final Setting<ByteSizeValue> SETTING_GRPC_MAX_MSG_SIZE = Setting.byteSizeSetting(
        "grpc.netty.max_msg_size",
        new ByteSizeValue(10, ByteSizeUnit.MB),
        new ByteSizeValue(0, ByteSizeUnit.MB),
        new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES),
        Setting.Property.NodeScope
    );

    /**
     * Connections lasting longer than configured age will be gracefully terminated.
     * No max connection age by default.
     */
    public static final Setting<TimeValue> SETTING_GRPC_MAX_CONNECTION_AGE = Setting.timeSetting(
        "grpc.netty.max_connection_age",
        new TimeValue(Long.MAX_VALUE),
        new TimeValue(0),
        Setting.Property.NodeScope
    );

    /**
     * Idle connections lasting longer than configured value will be gracefully terminated.
     * No max idle time by default.
     */
    public static final Setting<TimeValue> SETTING_GRPC_MAX_CONNECTION_IDLE = Setting.timeSetting(
        "grpc.netty.max_connection_idle",
        new TimeValue(Long.MAX_VALUE),
        new TimeValue(0),
        Setting.Property.NodeScope
    );

    /**
     * Timeout for keepalive ping requests of an established connection.
     */
    public static final Setting<TimeValue> SETTING_GRPC_KEEPALIVE_TIMEOUT = Setting.timeSetting(
        "grpc.netty.keepalive_timeout",
        new TimeValue(Long.MAX_VALUE),
        new TimeValue(0),
        Setting.Property.NodeScope
    );

    /**
     * Port range on which servers bind.
     */
    protected PortsRange port;

    /**
     * Port settings are set using the transport type, in this case GRPC_TRANSPORT_SETTING_KEY.
     * Child classes have distinct transport type keys and need to override these settings.
     */
    protected String portSettingKey;

    /**
     * Settings.
     */
    protected final Settings settings;

    private final NetworkService networkService;
    private final ThreadPool threadPool;
    private final List<BindableService> services;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private final int nettyEventLoopThreads;
    private final int executorThreads;
    private final long maxInboundMessageSize;
    private final long maxConcurrentConnectionCalls;
    private final ServerInterceptor serverInterceptor;
    private final TimeValue maxConnectionAge;
    private final TimeValue maxConnectionIdle;
    private final TimeValue keepAliveTimeout;
    private final CopyOnWriteArrayList<Server> servers = new CopyOnWriteArrayList<>();
    private final List<UnaryOperator<NettyServerBuilder>> serverBuilderConfigs = new ArrayList<>();

    private volatile BoundTransportAddress boundAddress;
    private volatile EventLoopGroup bossEventLoopGroup;
    private volatile EventLoopGroup workerEventLoopGroup;
    private volatile ExecutorService grpcExecutor;

    /**
     * Creates a new Netty4GrpcServerTransport instance.
     * @param settings the configured settings.
     * @param services the gRPC compatible services to be registered with the server.
     * @param networkService the bind/publish addresses.
     * @param threadPool the thread pool for gRPC request processing.
     * @param serverInterceptor the gRPC server interceptor to be applied.
     */
    public Netty4GrpcServerTransport(
        Settings settings,
        List<BindableService> services,
        NetworkService networkService,
        ThreadPool threadPool,
        ServerInterceptor serverInterceptor
    ) {
        logger.debug("Initializing Netty4GrpcServerTransport with settings = {}", settings);
        this.settings = Objects.requireNonNull(settings);
        this.services = Objects.requireNonNull(services);
        this.serverInterceptor = Objects.requireNonNull(serverInterceptor);
        this.networkService = Objects.requireNonNull(networkService);
        this.threadPool = Objects.requireNonNull(threadPool);
        final List<String> grpcBindHost = SETTING_GRPC_BIND_HOST.get(settings);
        this.bindHosts = (grpcBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : grpcBindHost).toArray(
            Strings.EMPTY_ARRAY
        );
        final List<String> grpcPublishHost = SETTING_GRPC_PUBLISH_HOST.get(settings);
        this.publishHosts = (grpcPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings) : grpcPublishHost)
            .toArray(Strings.EMPTY_ARRAY);
        this.port = SETTING_GRPC_PORT.get(settings);
        this.nettyEventLoopThreads = SETTING_GRPC_WORKER_COUNT.get(settings);
        this.executorThreads = SETTING_GRPC_EXECUTOR_COUNT.get(settings);
        this.maxInboundMessageSize = SETTING_GRPC_MAX_MSG_SIZE.get(settings).getBytes();
        this.maxConcurrentConnectionCalls = SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS.get(settings);
        this.maxConnectionAge = SETTING_GRPC_MAX_CONNECTION_AGE.get(settings);
        this.maxConnectionIdle = SETTING_GRPC_MAX_CONNECTION_IDLE.get(settings);
        this.keepAliveTimeout = SETTING_GRPC_KEEPALIVE_TIMEOUT.get(settings);
        this.portSettingKey = SETTING_GRPC_PORT.getKey();
    }

    /**
     * Creates a new Netty4GrpcServerTransport instance.
     * @param settings the configured settings.
     * @param services the gRPC compatible services to be registered with the server.
     * @param networkService the bind/publish addresses.
     * @param threadPool the thread pool for gRPC request processing.
     */
    public Netty4GrpcServerTransport(
        Settings settings,
        List<BindableService> services,
        NetworkService networkService,
        ThreadPool threadPool
    ) {
        this(settings, services, networkService, threadPool, new GrpcInterceptorChain(threadPool.getThreadContext()));
    }

    /**
     * Returns the setting key used to identify this transport type.
     *
     * @return the gRPC transport setting key
     */
    @Override
    public String settingKey() {
        return GRPC_TRANSPORT_SETTING_KEY;
    }

    /**
     * Returns the bound transport addresses for this gRPC server.
     * This method is public for testing purposes.
     *
     * @return the bound transport address containing all bound addresses and publish address
     */
    @Override
    public BoundTransportAddress getBoundAddress() {
        return this.boundAddress;
    }

    /**
     * Inject a NettyServerBuilder configuration to be applied at server bind and start.
     * @param configModifier builder configuration to set.
     */
    protected void addServerConfig(UnaryOperator<NettyServerBuilder> configModifier) {
        serverBuilderConfigs.add(configModifier);
    }

    /**
     * Starts the gRPC server transport.
     * Initializes the event loop group and binds the server to the configured addresses.
     */
    @Override
    protected void doStart() {
        boolean success = false;
        try {
            // Create separate boss and worker event loop groups for better isolation
            this.bossEventLoopGroup = new NioEventLoopGroup(1, daemonThreadFactory(settings, "grpc_boss"));
            this.workerEventLoopGroup = new NioEventLoopGroup(nettyEventLoopThreads, daemonThreadFactory(settings, "grpc_worker"));

            // Use OpenSearch's managed thread pool for gRPC request processing
            this.grpcExecutor = threadPool.executor(GRPC_THREAD_POOL_NAME);

            bindServer();
            success = true;
            logger.info("Started gRPC server on port {} with {} executor threads", port, executorThreads);
        } finally {
            if (!success) {
                doStop();
            }
        }
    }

    /**
     * Stops the gRPC server transport.
     * Shuts down all running servers and the event loop group.
     */
    @Override
    protected void doStop() {
        for (Server server : servers) {
            if (server != null) {
                server.shutdown();
                try {
                    server.awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while shutting down gRPC server");
                } finally {
                    server.shutdownNow();
                }
            }
        }

        // Note: grpcExecutor is managed by OpenSearch's ThreadPool, so we don't shut it down here

        // Shutdown event loop groups
        if (bossEventLoopGroup != null) {
            try {
                bossEventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Failed to shut down boss event loop group");
            }
        }

        if (workerEventLoopGroup != null) {
            try {
                workerEventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Failed to shut down worker event loop group");
            }
        }
    }

    /**
     * Closes the gRPC server transport.
     * Performs any necessary cleanup after stopping the transport.
     */
    @Override
    protected void doClose() {
        if (bossEventLoopGroup != null) {
            bossEventLoopGroup.close();
        }
        if (workerEventLoopGroup != null) {
            workerEventLoopGroup.close();
        }
    }

    private void bindServer() {
        InetAddress[] hostAddresses;
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<TransportAddress> boundAddresses = new ArrayList<>(hostAddresses.length);
        for (InetAddress address : hostAddresses) {
            boundAddresses.add(bindAddress(address, port));
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolveTransportPublishPort(SETTING_GRPC_PUBLISH_PORT.get(settings), boundAddresses, publishInetAddress);
        if (publishPort < 0) {
            throw new BindTransportException(
                "Failed to auto-resolve grpc publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). "
                    + "Please specify a unique port by setting "
                    + portSettingKey
                    + " or "
                    + SETTING_GRPC_PUBLISH_PORT.getKey()
            );
        }

        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), publishAddress);
        logger.info("{}", boundAddress);
    }

    private TransportAddress bindAddress(InetAddress hostAddress, PortsRange portRange) {
        AtomicReference<Exception> lastException = new AtomicReference<>();
        AtomicReference<TransportAddress> addr = new AtomicReference<>();

        boolean success = portRange.iterate(portNumber -> {
            try {
                final InetSocketAddress address = new InetSocketAddress(hostAddress, portNumber);
                final NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(address)
                    .executor(grpcExecutor)
                    .bossEventLoopGroup(bossEventLoopGroup)
                    .workerEventLoopGroup(workerEventLoopGroup)
                    .maxInboundMessageSize((int) maxInboundMessageSize)
                    .maxConcurrentCallsPerConnection((int) maxConcurrentConnectionCalls)
                    .maxConnectionAge(maxConnectionAge.duration(), maxConnectionAge.timeUnit())
                    .maxConnectionIdle(maxConnectionIdle.duration(), maxConnectionIdle.timeUnit())
                    .keepAliveTimeout(keepAliveTimeout.duration(), keepAliveTimeout.timeUnit())
                    .channelType(NioServerSocketChannel.class)
                    .addService(new HealthStatusManager().getHealthService())
                    .addService(ProtoReflectionService.newInstance())
                    .intercept(serverInterceptor);

                for (UnaryOperator<NettyServerBuilder> op : serverBuilderConfigs) {
                    op.apply(serverBuilder);
                }

                services.forEach(serverBuilder::addService);

                Server srv = serverBuilder.build().start();
                servers.add(srv);
                addr.set(new TransportAddress(hostAddress, portNumber));
                logger.debug("Bound gRPC to address {{}}", address);
                return true;
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
        });

        if (!success) {
            throw new RuntimeException("Failed to bind to " + hostAddress + " on ports " + portRange, lastException.get());
        }

        return addr.get();
    }

    // Package-private methods for testing
    ExecutorService getGrpcExecutorForTesting() {
        return grpcExecutor;
    }

    EventLoopGroup getBossEventLoopGroupForTesting() {
        return bossEventLoopGroup;
    }

    EventLoopGroup getWorkerEventLoopGroupForTesting() {
        return workerEventLoopGroup;
    }
}
