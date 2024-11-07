/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc.netty4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.grpc.AbstractGrpcServerTransport;
import org.opensearch.grpc.GrpcStats;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.SharedGroupFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;

public class Netty4GrpcServerTransport extends AbstractGrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(Netty4GrpcServerTransport.class);

    public static final Setting<Integer> SETTING_GRPC_WORKER_COUNT = Setting.intSetting("grpc.worker_count", 1, Setting.Property.NodeScope);

    private final SharedGroupFactory sharedGroupFactory;
    private final CopyOnWriteArrayList<Server> servers = new CopyOnWriteArrayList<>();
    private volatile SharedGroupFactory.SharedGroup sharedGroup;
    private final ServerStatsInterceptor sharedServerStatsInterceptor;
    private final AtomicLong currentOpen = new AtomicLong(0);
    private final AtomicLong totalOpened = new AtomicLong(0);

    public Netty4GrpcServerTransport(Settings settings, NetworkService networkService, SharedGroupFactory sharedGroupFactory) {
        super(settings, networkService);
        this.sharedGroupFactory = sharedGroupFactory;
        this.sharedServerStatsInterceptor = new ServerStatsInterceptor(currentOpen, totalOpened);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            sharedGroup = sharedGroupFactory.getGRPCGroup();
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
    protected TransportAddress bindAddress(InetAddress hostAddress, PortsRange portRange) {
        AtomicReference<Exception> lastException = new AtomicReference<>();
        AtomicReference<TransportAddress> addr = new AtomicReference<>();

        boolean success = portRange.iterate(portNumber -> {
            try {
                InetSocketAddress address = new InetSocketAddress(hostAddress, portNumber);
                NettyServerBuilder srvBuilder = NettyServerBuilder.forAddress(address)
                    .bossEventLoopGroup(sharedGroup.getLowLevelGroup())
                    .workerEventLoopGroup(sharedGroup.getLowLevelGroup())
                    .channelType(NettyAllocator.getServerChannelType())
                    .intercept(this.sharedServerStatsInterceptor)
                    .addService(new HealthStatusManager().getHealthService())
                    .addService(ProtoReflectionService.newInstance());

                // TODO: INJECT SERVICE DEFINITIONS // .addService(new GrpcQueryServiceImpl(this))

                Server srv = srvBuilder.build().start();
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

            if (sharedGroup != null) {
                sharedGroup.shutdown();
                sharedGroup = null;
            }
        }
    }

    @Override
    protected void doClose() {}

    @Override
    public GrpcStats stats() {
        return new GrpcStats(totalOpened.get(), currentOpen.get());
    }

    static class ServerStatsInterceptor implements ServerInterceptor {
        private final AtomicLong currentOpen;
        private final AtomicLong totalOpened;

        ServerStatsInterceptor(AtomicLong currentOpen, AtomicLong totalOpened) {
            this.currentOpen = currentOpen;
            this.totalOpened = totalOpened;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
        ) {
            logger.debug("Intercepted call - Method: {}, Authority: {}, Headers: {}",
                call.getMethodDescriptor().getFullMethodName(),
                call.getAuthority(),
                headers);

            currentOpen.incrementAndGet();
            totalOpened.incrementAndGet();

            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        currentOpen.decrementAndGet();
                        super.close(status, trailers);
                    }
                }, headers)
            ) {
            };
        }
    }
}
