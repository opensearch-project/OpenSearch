/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugin.transport.grpc.ssl.NettyGrpcClient;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.grpc.health.v1.HealthCheckResponse;

import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;

public class Netty4GrpcServerTransportIT extends OpenSearchIntegTestCase {

    private TransportAddress randomNetty4GrpcServerTransportAddr() {
        List<TransportAddress> addresses = new ArrayList<>();
        for (Netty4GrpcServerTransport transport : internalCluster().getInstances(Netty4GrpcServerTransport.class)) {
            TransportAddress tAddr = new TransportAddress(transport.getBoundAddress().publishAddress().address());
            addresses.add(tAddr);
        }
        return randomFrom(addresses);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(AUX_TRANSPORT_TYPES_KEY, GRPC_TRANSPORT_SETTING_KEY).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GrpcPlugin.class);
    }

    public void testGrpcTransportStarted() {
        // Verify that the gRPC transport is started on all nodes
        for (String nodeName : internalCluster().getNodeNames()) {
            Netty4GrpcServerTransport transport = internalCluster().getInstance(Netty4GrpcServerTransport.class, nodeName);
            assertNotNull("gRPC transport should be started on node " + nodeName, transport);

            // Verify that the transport is bound to an address
            TransportAddress[] boundAddresses = transport.getBoundAddress().boundAddresses();
            assertTrue("gRPC transport should be bound to at least one address", boundAddresses.length > 0);

            // Log the bound addresses for debugging
            for (TransportAddress address : boundAddresses) {
                InetSocketAddress inetAddress = address.address();
                logger.info("Node {} gRPC transport bound to {}", nodeName, NetworkAddress.format(inetAddress));
            }
        }
    }

    public void testStartGrpcTransportClusterHealth() throws Exception {
        // REST api cluster health
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().get();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());

        // gRPC transport service health
        try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build()) {
            assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
        }
    }
}
