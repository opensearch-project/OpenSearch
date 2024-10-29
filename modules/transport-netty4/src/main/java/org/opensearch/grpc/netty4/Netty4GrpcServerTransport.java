package org.opensearch.grpc.netty4;

import io.grpc.netty.NettyServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.grpc.AbstractGrpcServerTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.SharedGroupFactory;
import io.netty.channel.EventLoopGroup;

import io.grpc.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_PORT;
import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_BIND_HOST;

public class Netty4GrpcServerTransport extends AbstractGrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(Netty4GrpcServerTransport.class);

    public static final Setting<Integer> SETTING_GRPC_WORKER_COUNT =
        Setting.intSetting("grpc.worker_count", 1, Setting.Property.NodeScope);

    private final SharedGroupFactory sharedGroupFactory;
    private final int port;
    private final String host;
    private volatile Server grpcServer;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;

    public Netty4GrpcServerTransport(
        Settings settings,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory
    ) {
        this.sharedGroupFactory = sharedGroupFactory;
        this.port = SETTING_GRPC_PORT.get(settings).ports()[0]; // TODO: HANDLE PORT RANGE
        this.host = SETTING_GRPC_BIND_HOST.get(settings).get(0); // TODO: HANDLE BIND_HOST LIST
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            sharedGroup = sharedGroupFactory.getGRPCGroup();
            EventLoopGroup eventLoopGroup = sharedGroup.getLowLevelGroup();

            grpcServer = NettyServerBuilder
                .forAddress(new InetSocketAddress(host, port))
                .bossEventLoopGroup(eventLoopGroup)
                .workerEventLoopGroup(eventLoopGroup)
                .channelType(NettyAllocator.getServerChannelType())
                // TODO: INJECT SERVICE DEFINITIONS // .addService(new GrpcQueryServiceImpl(this))
                .build();

            grpcServer.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (grpcServer != null) {
                    grpcServer.shutdown();
                }
            }));

            success = true;
            logger.info("Started gRPC server on port {}", port);

        } catch (IOException e) {
            logger.error("Failed to start gRPC server", e);
            throw new RuntimeException("Failed to start gRPC server", e);
        } finally {
            if (!success) {
                doStop();
            }
        }
    }

    @Override
    protected void doStop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while shutting down gRPC server");
            } finally {
                grpcServer.shutdownNow();
            }
        }

        if (sharedGroup != null) {
            sharedGroup.shutdown();
            sharedGroup = null;
        }
    }

    @Override
    protected void doClose() {
        grpcServer.shutdown();
    }
}
