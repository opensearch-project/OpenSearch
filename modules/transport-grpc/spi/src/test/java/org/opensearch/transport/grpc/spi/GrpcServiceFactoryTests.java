/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.spi;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;

import io.grpc.BindableService;
import io.grpc.channelz.v1.ChannelzGrpc;
import io.grpc.channelz.v1.GetChannelRequest;
import io.grpc.channelz.v1.GetChannelResponse;
import io.grpc.stub.StreamObserver;

import static org.mockito.Mockito.mock;

public class GrpcServiceFactoryTests extends OpenSearchTestCase {

    private static class MockChannelzService extends ChannelzGrpc.ChannelzImplBase {
        @Override
        public void getChannel(GetChannelRequest request, StreamObserver<GetChannelResponse> responseObserver) {
            GetChannelResponse response = GetChannelResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static class MockServiceProvider implements GrpcServiceFactory {
        private Client client;

        public MockServiceProvider() {}

        @Override
        public String plugin() {
            return "MockTestPlugin";
        }

        @Override
        public GrpcServiceFactory initClient(Client client) {
            this.client = client;
            return this;
        }

        @Override
        public List<BindableService> build() {
            return List.of(new MockChannelzService());
        }

        public Client getClient() {
            return client;
        }
    }

    public void testGrpcServiceFactoryPlugin() {
        MockServiceProvider factory = new MockServiceProvider();
        assertEquals("MockTestPlugin", factory.plugin());
    }

    public void testGrpcServiceFactoryBuild() {
        MockServiceProvider factory = new MockServiceProvider();
        List<BindableService> services = factory.build();

        assertNotNull(services);
        assertEquals(1, services.size());
        assertTrue(services.get(0) instanceof MockChannelzService);
    }

    public void testGrpcServiceFactoryInitClient() {
        MockServiceProvider factory = new MockServiceProvider();
        Client mockClient = mock(Client.class);

        GrpcServiceFactory result = factory.initClient(mockClient);

        assertSame(factory, result);
        assertSame(mockClient, factory.getClient());
    }
}
