/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.Version;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.spi.GrpcServiceFactory;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.grpc.BindableService;
import io.grpc.channelz.v1.ChannelzGrpc;
import io.grpc.channelz.v1.GetChannelRequest;
import io.grpc.channelz.v1.GetChannelResponse;
import io.grpc.reflection.v1alpha.ServiceResponse;
import io.grpc.stub.StreamObserver;

public class LoadExtendingPluginServicesIT extends GrpcTransportBaseIT {

    private static class MockChannelzService extends ChannelzGrpc.ChannelzImplBase {
        @Override
        public void getChannel(GetChannelRequest request, StreamObserver<GetChannelResponse> responseObserver) {
            GetChannelResponse response = GetChannelResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static final class MockExtendingPlugin extends Plugin {

        public MockExtendingPlugin() {}

        public static class MockServiceProvider implements GrpcServiceFactory {
            public MockServiceProvider() {}

            @Override
            public String plugin() {
                return "MockExtendingPlugin";
            }

            @Override
            public GrpcServiceFactory initClient(Client client) {
                return this;
            }

            @Override
            public GrpcServiceFactory initSettings(Settings settings) {
                return GrpcServiceFactory.super.initSettings(settings);
            }

            @Override
            public GrpcServiceFactory initClusterSettings(ClusterSettings clusterSettings) {
                return GrpcServiceFactory.super.initClusterSettings(clusterSettings);
            }

            @Override
            public GrpcServiceFactory initThreadPool(ThreadPool threadPool) {
                return GrpcServiceFactory.super.initThreadPool(threadPool);
            }

            @Override
            public List<BindableService> build() {
                return List.of(new MockChannelzService());
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                GrpcPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "21",
                GrpcPlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            new PluginInfo(
                MockExtendingPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "21",
                MockExtendingPlugin.class.getName(),
                null,
                List.of(GrpcPlugin.class.getName()),
                false
            )
        );
    }

    public void testListInjectedService() throws Exception {
        try (NettyGrpcClient client = createGrpcClient()) {
            List<ServiceResponse> servicesResp = client.listServices().get();
            boolean foundMockService = servicesResp.stream().anyMatch(service -> service.getName().contains("grpc.channelz.v1.Channelz"));
            assertTrue("Failed to discover plugin provided service: grpc.channelz.v1.Channelz", foundMockService);
        }
    }
}
