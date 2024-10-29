package org.opensearch.grpc.netty4;

import io.grpc.netty.NettyServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.grpc.AbstractGrpcServerTransport;
import org.opensearch.grpc.GrpcInfo;
import org.opensearch.grpc.GrpcStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import io.netty.channel.EventLoopGroup;

import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Netty4GrpcServerTransport extends AbstractGrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(Netty4GrpcServerTransport.class);

    public static final Setting<Integer> SETTING_GRPC_PORT =
        Setting.intSetting("grpc.port", 9400, Property.NodeScope);

    public static final Setting<ByteSizeValue> SETTING_GRPC_MAX_MESSAGE_SIZE =
        Setting.byteSizeSetting("grpc.max_message_size", new ByteSizeValue(100 * 1024 * 1024), Property.NodeScope);

    public static final Setting<Integer> SETTING_GRPC_WORKER_COUNT =
        Setting.intSetting("grpc.worker_count", 1, Property.NodeScope);

    private final SharedGroupFactory sharedGroupFactory;
    private final int maxMessageSize;
    private final int port;
    private volatile Server grpcServer;
    private volatile SharedGroupFactory.SharedGroup sharedGroup;

    public Netty4GrpcServerTransport(
        Settings settings,
        NetworkService networkService,
        BigArrays bigArrays,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        SharedGroupFactory sharedGroupFactory
    ) {
        this.sharedGroupFactory = sharedGroupFactory;
        this.maxMessageSize = Math.toIntExact(SETTING_GRPC_MAX_MESSAGE_SIZE.get(settings).getBytes());
        this.port = SETTING_GRPC_PORT.get(settings);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            sharedGroup = sharedGroupFactory.getGRPCGroup();
            EventLoopGroup workerGroup = sharedGroup.getLowLevelGroup();

            grpcServer = NettyServerBuilder.forPort(port)
                .workerEventLoopGroup(workerGroup)
                .maxInboundMessageSize(maxMessageSize)
                // gRPC service definitions need to be injected here?
                // .addService(new GrpcQueryServiceImpl(this))
                .build();

            // Start the server
            grpcServer.start();

            // Register shutdown hook
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

    }
}
