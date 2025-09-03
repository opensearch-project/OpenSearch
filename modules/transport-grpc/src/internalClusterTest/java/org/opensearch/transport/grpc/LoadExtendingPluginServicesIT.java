/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import com.google.protobuf.StringValue;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.reflection.v1alpha.ServiceResponse;
import io.grpc.stub.ServerCalls;
import org.opensearch.plugins.Plugin;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LoadExtendingPluginServicesIT extends GrpcTransportBaseIT {

    /**
     * Mock service which echoes a string.
     */
    private static class MockEchoService implements BindableService {
        @Override
        public ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder("MockEchoService")
                .addMethod(
                    MethodDescriptor.<StringValue, StringValue>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName("MockEchoService/Echo")
                        .setRequestMarshaller(ProtoLiteUtils.marshaller(StringValue.getDefaultInstance()))
                        .setResponseMarshaller(ProtoLiteUtils.marshaller(StringValue.getDefaultInstance()))
                        .build(),
                    ServerCalls.asyncUnaryCall((request, responseObserver) -> {
                        responseObserver.onNext(request);
                        responseObserver.onCompleted();
                    }))
                .build();
        }
    }

    public static final class MockExtendingPlugin extends Plugin {

        public static class MockServiceProvider implements GrpcPlugin.GrpcServiceProvider {
            @Override
            public BindableService get() {
                return new MockEchoService();
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GrpcPlugin.class, MockExtendingPlugin.class);
    }

    public void testListInjectedService() throws Exception {
        System.out.println("PRINTING DISCOVERED SERVICES: ");

        try (NettyGrpcClient client = createGrpcClient()) {
            List<ServiceResponse> servicesResp = client.listServices().get();
            for (ServiceResponse resp : servicesResp) {
                System.out.println(resp.getName());
            }
        }
    }
}
