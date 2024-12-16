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
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
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

import io.grpc.BindableService;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;

import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_BIND_HOST;
import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_PORT;
import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_PUBLISH_HOST;
import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_PUBLISH_PORT;
import static org.opensearch.common.network.NetworkService.resolvePublishPort;
import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;
import static org.opensearch.transport.grpc.GrpcModulePlugin.SETTING_GRPC_WORKER_COUNT;

public class Netty4GrpcServerTransport extends AbstractLifecycleComponent implements LifecycleComponent {
    private static final Logger logger = LogManager.getLogger(Netty4GrpcServerTransport.class);

    private final Settings settings;
    private final NetworkService networkService;
    private final List<BindableService> services;
    private final CopyOnWriteArrayList<Server> servers = new CopyOnWriteArrayList<>();
    private final String[] bindHosts;
    private final String[] publishHosts;
    private final PortsRange port;
    private final int nettyEventLoopThreads;

    private volatile BoundTransportAddress boundAddress;
    private volatile EventLoopGroup eventLoopGroup;

    public Netty4GrpcServerTransport(Settings settings, List<BindableService> services, NetworkService networkService) {
        this.settings = Objects.requireNonNull(settings);
        this.services = Objects.requireNonNull(services);
        this.networkService = Objects.requireNonNull(networkService);

        final List<String> httpBindHost = SETTING_AUX_BIND_HOST.get(settings);
        this.bindHosts = (httpBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : httpBindHost).toArray(
            Strings.EMPTY_ARRAY
        );

        final List<String> httpPublishHost = SETTING_AUX_PUBLISH_HOST.get(settings);
        this.publishHosts = (httpPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings) : httpPublishHost)
            .toArray(Strings.EMPTY_ARRAY);

        this.port = SETTING_AUX_PORT.get(settings);
        this.nettyEventLoopThreads = SETTING_GRPC_WORKER_COUNT.get(settings);
    }

    BoundTransportAddress boundAddress() {
        return this.boundAddress;
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

        final int publishPort = resolvePublishPort(SETTING_AUX_PUBLISH_PORT.get(settings), boundAddresses, publishInetAddress);
        if (publishPort < 0) {
            throw new BindTransportException(
                "Failed to auto-resolve grpc publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). "
                    + "Please specify a unique port by setting "
                    + SETTING_AUX_PORT.getKey()
                    + " or "
                    + SETTING_AUX_PUBLISH_PORT.getKey()
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
                final NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(address, InsecureServerCredentials.create())
                    .bossEventLoopGroup(eventLoopGroup)
                    .workerEventLoopGroup(eventLoopGroup)
                    .channelType(NioServerSocketChannel.class)
                    .addService(new HealthStatusManager().getHealthService())
                    .addService(ProtoReflectionService.newInstance());

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
