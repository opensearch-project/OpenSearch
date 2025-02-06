/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ProxyDetector;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.reflection.v1alpha.ServiceResponse;
import io.grpc.stub.StreamObserver;
import org.opensearch.core.common.transport.TransportAddress;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.grpc.internal.GrpcUtil.NOOP_PROXY_DETECTOR;

public class NettyGrpcClient {
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
    }

    /**
     * ProtoReflectionService only implements a streaming interface and has no blocking stub.
     * @return List<ServiceResponse> services reported
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
        requestObserver.onNext(ServerReflectionRequest.newBuilder()
            .setListServices("")
            .build());
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

    public HealthCheckResponse.ServingStatus checkHealth() {
        return healthStub.check(HealthCheckRequest.newBuilder().build()).getStatus();
    }

    public static class Builder {
        private SslContext sslCtxt = null;
        private TransportAddress addr = new TransportAddress(new InetSocketAddress("localhost", 9300));
        private final ProxyDetector proxyDetector = NOOP_PROXY_DETECTOR; // No proxy detection for test client

        Builder () {}

        public NettyGrpcClient build() {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress(addr.getAddress(), addr.getPort())
                .proxyDetector(proxyDetector);

            if (sslCtxt == null) {
                channelBuilder.usePlaintext();
            } else {
                channelBuilder.sslContext(sslCtxt);
            }

            return new NettyGrpcClient(addr, channelBuilder);
        }

        public Builder setSslContext(SslContext sslCtxt) {
            this.sslCtxt = sslCtxt;
            return this;
        }

        public Builder setAddress(TransportAddress addr) {
            this.addr = addr;
            return this;
        }
    }
}
