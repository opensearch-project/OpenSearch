/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.transport.TransportAddress;

import javax.net.ssl.SSLException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.reflection.v1alpha.ServiceResponse;
import io.grpc.stub.StreamObserver;

import static org.opensearch.plugin.transport.grpc.ssl.SecureSettingsHelpers.CLIENT_KEYSTORE;
import static org.opensearch.plugin.transport.grpc.ssl.SecureSettingsHelpers.getTestKeyManagerFactory;
import static io.grpc.internal.GrpcUtil.NOOP_PROXY_DETECTOR;

public class NettyGrpcClient implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(NettyGrpcClient.class);
    private final ManagedChannel channel;
    private final HealthGrpc.HealthBlockingStub healthStub;
    private final ServerReflectionGrpc.ServerReflectionStub reflectionStub;

    public NettyGrpcClient(NettyChannelBuilder channelBuilder) {
        channel = channelBuilder.build();
        healthStub = HealthGrpc.newBlockingStub(channel);
        reflectionStub = ServerReflectionGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow(); // forced shutdown
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Unable to shutdown the managed channel gracefully");
            }
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }

    /**
     * List available gRPC services available on server.
     * Note: ProtoReflectionService only implements a streaming interface and has no blocking stub.
     * @return services registered on the server.
     */
    public CompletableFuture<List<ServiceResponse>> listServices() {
        CompletableFuture<List<ServiceResponse>> respServices = new CompletableFuture<>();

        StreamObserver<ServerReflectionResponse> responseObserver = new StreamObserver<>() {
            final List<ServiceResponse> services = new ArrayList<>();

            @Override
            public void onNext(ServerReflectionResponse response) {
                if (response.hasListServicesResponse()) {
                    services.addAll(response.getListServicesResponse().getServiceList());
                }
            }

            @Override
            public void onError(Throwable t) {
                respServices.completeExceptionally(t);
                throw new RuntimeException(t);
            }

            @Override
            public void onCompleted() {
                respServices.complete(services);
            }
        };

        StreamObserver<ServerReflectionRequest> requestObserver = reflectionStub.serverReflectionInfo(responseObserver);
        requestObserver.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        requestObserver.onCompleted();
        return respServices;
    }

    /**
     * Request server status.
     * @return HealthCheckResponse.ServingStatus.
     */
    public HealthCheckResponse.ServingStatus checkHealth() {
        return healthStub.check(HealthCheckRequest.newBuilder().build()).getStatus();
    }

    public static class Builder {
        private Boolean clientAuth = false;
        private Boolean insecure = false;
        private TransportAddress addr;

        private static final ApplicationProtocolConfig CLIENT_ALPN = new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            ApplicationProtocolNames.HTTP_2
        );

        public Builder() {}

        public NettyGrpcClient build() throws SSLException {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(addr.getAddress(), addr.getPort())
                .proxyDetector(NOOP_PROXY_DETECTOR);

            if (clientAuth || insecure) {
                SslContextBuilder builder = SslContextBuilder.forClient();
                builder.sslProvider(SslProvider.JDK);
                builder.applicationProtocolConfig(CLIENT_ALPN);
                if (clientAuth) {
                    builder.keyManager(getTestKeyManagerFactory(CLIENT_KEYSTORE));
                }
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                channelBuilder.sslContext(builder.build());
            } else {
                channelBuilder.usePlaintext();
            }

            return new NettyGrpcClient(channelBuilder);
        }

        public Builder setAddress(TransportAddress addr) {
            this.addr = addr;
            return this;
        }

        /**
         * Enable clientAuth - load client keystore.
         */
        public Builder clientAuth(boolean enable) {
            this.clientAuth = enable;
            return this;
        }

        /**
         * Enable insecure TLS client.
         */
        public Builder insecure(boolean enable) {
            this.insecure = enable;
            return this;
        }
    }
}
