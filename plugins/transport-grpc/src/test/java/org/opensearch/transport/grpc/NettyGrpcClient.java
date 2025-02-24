/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;

import javax.net.ssl.SSLException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ProxyDetector;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.reflection.v1alpha.ServiceResponse;
import io.grpc.stub.StreamObserver;

import static org.opensearch.transport.grpc.SecureNetty4GrpcServerTransportTests.createSettings;
import static io.grpc.internal.GrpcUtil.NOOP_PROXY_DETECTOR;

public class NettyGrpcClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final HealthGrpc.HealthBlockingStub healthStub;
    private final ServerReflectionGrpc.ServerReflectionStub reflectionStub;

    public NettyGrpcClient(TransportAddress addr, NettyChannelBuilder channelBuilder) {
        channel = channelBuilder.build();
        healthStub = HealthGrpc.newBlockingStub(channel);
        reflectionStub = ServerReflectionGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow();
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
    public List<ServiceResponse> listServices() {
        List<ServiceResponse> respServices = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<ServerReflectionResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(ServerReflectionResponse response) {
                if (response.hasListServicesResponse()) {
                    respServices.addAll(response.getListServicesResponse().getServiceList());
                }
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
                throw new RuntimeException(t);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        StreamObserver<ServerReflectionRequest> requestObserver = reflectionStub.serverReflectionInfo(responseObserver);
        requestObserver.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        requestObserver.onCompleted();

        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new RuntimeException(NettyGrpcClient.class.getSimpleName() + " timed out waiting for response.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(NettyGrpcClient.class.getSimpleName() + " interrupted waiting for response: " + e.getMessage());
        }

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
        private SecureAuxTransportSettingsProvider settingsProvider = null;
        private TransportAddress addr;
        private final ProxyDetector proxyDetector = NOOP_PROXY_DETECTOR; // No proxy detection for test client

        Builder() {}

        public NettyGrpcClient build() throws SSLException {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(addr.getAddress(), addr.getPort())
                .proxyDetector(proxyDetector);

            if (settingsProvider == null) {
                channelBuilder.usePlaintext();
            } else {
                SecureAuxTransportSettingsProvider.SecureTransportParameters params = settingsProvider.parameters(createSettings()).get();
                SslContext ctxt = SslContextBuilder.forClient()
                    .trustManager(params.trustManagerFactory().get())
                    .sslProvider(SslProvider.valueOf(params.sslProvider().get().toUpperCase(Locale.ROOT)))
                    .clientAuth(ClientAuth.valueOf(params.clientAuth().get().toUpperCase(Locale.ROOT)))
                    .protocols(params.protocols())
                    .ciphers(params.cipherSuites())
                    .applicationProtocolConfig(
                        new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2
                        )
                    )
                    .build();
                channelBuilder.sslContext(ctxt);
            }

            return new NettyGrpcClient(addr, channelBuilder);
        }

        public Builder setSecureSettingsProvider(SecureAuxTransportSettingsProvider settingsProvider) {
            this.settingsProvider = settingsProvider;
            return this;
        }

        public Builder setAddress(TransportAddress addr) {
            this.addr = addr;
            return this;
        }
    }
}
