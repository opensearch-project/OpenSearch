/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.server.spi;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import io.grpc.BindableService;
import io.grpc.channelz.v1.ChannelzGrpc;
import io.grpc.channelz.v1.GetChannelRequest;
import io.grpc.channelz.v1.GetChannelResponse;
import io.grpc.stub.StreamObserver;

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
        public MockServiceProvider() {}

        @Override
        public GrpcServiceFactory initClient(Client client) {
            return null;
        }

        @Override
        public BindableService build() {
            return new MockChannelzService();
        }
    }

    public void testGrcpServiceFactory() {
        GrpcServiceFactory grpcServiceFactory = new MockServiceProvider();
        assert (true);
    }
}
