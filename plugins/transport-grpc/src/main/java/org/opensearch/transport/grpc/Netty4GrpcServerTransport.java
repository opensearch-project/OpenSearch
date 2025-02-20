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
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import io.grpc.BindableService;
import io.grpc.Server;
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

/**
 * Netty4 gRPC server implemented as a LifecycleComponent.
 * Services injected through BindableService list.
 */
public class Netty4GrpcServerTransport extends NetworkPlugin.AuxTransport {
    private static final Logger logger = LogManager.getLogger(Netty4GrpcServerTransport.class);

    /**
     * Type key for configuring settings of this auxiliary transport.
     */
    public static final String GRPC_TRANSPORT_SETTING_KEY = "experimental-transport-grpc";

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

    protected PortsRange port;
    protected final Settings settings;

    private final NetworkService networkService;
    private final List<BindableService> services;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private final int nettyEventLoopThreads;
    private final CopyOnWriteArrayList<Server> servers = new CopyOnWriteArrayList<>();
    private final List<UnaryOperator<NettyServerBuilder>> serverBuilderConfigs = new ArrayList<>();

    private volatile BoundTransportAddress boundAddress;
    private volatile EventLoopGroup eventLoopGroup;

    /**
     * Creates a new Netty4GrpcServerTransport instance.
     * @param settings the configured settings.
     * @param services the gRPC compatible services to be registered with the server.
     * @param networkService the bind/publish addresses.
     */
    public Netty4GrpcServerTransport(Settings settings, List<BindableService> services, NetworkService networkService) {
        logger.debug("Initializing Netty4GrpcServerTransport with settings = {}", settings);
        this.settings = Objects.requireNonNull(settings);
        this.services = Objects.requireNonNull(services);
        this.networkService = Objects.requireNonNull(networkService);

        final List<String> grpcBindHost = SETTING_GRPC_BIND_HOST.get(settings);
        this.bindHosts = (grpcBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : grpcBindHost).toArray(
            Strings.EMPTY_ARRAY
        );

        final List<String> grpcPublishHost = SETTING_GRPC_PUBLISH_HOST.get(settings);
        this.publishHosts = (grpcPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings) : grpcPublishHost)
            .toArray(Strings.EMPTY_ARRAY);

        this.port = SETTING_GRPC_PORT.get(settings);
        this.nettyEventLoopThreads = SETTING_GRPC_WORKER_COUNT.get(settings);
    }

    // public for tests
    @Override
    public BoundTransportAddress getBoundAddress() {
        return this.boundAddress;
    }

    protected void addServerConfig(UnaryOperator<NettyServerBuilder> configModifier) {
        serverBuilderConfigs.add(configModifier);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            this.eventLoopGroup = new NioEventLoopGroup(nettyEventLoopThreads, daemonThreadFactory(settings, "grpc_event_loop"));
            bindServer();
            success = true;
            logger.info("Started gRPC server on port {}", port);
        } finally {
            if (!success) {
                doStop();
            }
        }
    }

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
        if (eventLoopGroup != null) {
            try {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Failed to shut down event loop group");
            }
        }
    }

    @Override
    protected void doClose() {
        eventLoopGroup.close();
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
                    + SETTING_GRPC_PORT.getKey()
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
                    .directExecutor()
                    .bossEventLoopGroup(eventLoopGroup)
                    .workerEventLoopGroup(eventLoopGroup)
                    .channelType(NioServerSocketChannel.class)
                    .addService(new HealthStatusManager().getHealthService())
                    .addService(ProtoReflectionService.newInstance());

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
}
