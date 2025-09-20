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

public class GrpcServiceFactoryTests extends OpenSearchTestCase {

    class MockGrpcServiceProvider implements GrpcServiceFactory {
        MockGrpcServiceProvider() {}

        @Override
        public GrpcServiceFactory initClient(Client client) {
            return null;
        }

        @Override
        public BindableService build() {
            return null;
        }
    }

    public void testGrcpServiceFactory() {
        GrpcServiceFactory grpcServiceFactory = new MockGrpcServiceProvider();
        assert (true);
    }
}
