/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class GrpcTransportIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(GrpcPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SETTING_GRPC_PORT.getKey(), "0")
            .put(AUX_TRANSPORT_TYPES_KEY, GRPC_TRANSPORT_SETTING_KEY)
            .build();
    }

    public void testGrpcTransportStarted() {
        // Verify that the gRPC transport is started on all nodes
        for (String nodeName : internalCluster().getNodeNames()) {
            Netty4GrpcServerTransport transport = internalCluster().getInstance(Netty4GrpcServerTransport.class, nodeName);
            assertNotNull("gRPC transport should be started on node " + nodeName, transport);

            // Verify that the transport is bound to an address
            TransportAddress[] boundAddresses = transport.boundAddress().boundAddresses();
            assertTrue("gRPC transport should be bound to at least one address", boundAddresses.length > 0);

            // Log the bound addresses for debugging
            for (TransportAddress address : boundAddresses) {
                InetSocketAddress inetAddress = address.address();
                logger.info("Node {} gRPC transport bound to {}", nodeName, NetworkAddress.format(inetAddress));
            }
        }
    }
}
