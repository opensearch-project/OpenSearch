/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import io.grpc.health.v1.HealthCheckResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_PORT;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;

public class GrpcTransportIT extends OpenSearchIntegTestCase {
    private final PortsRange TEST_AUX_PORTS = new PortsRange("9400-9500");
    private final TransportAddress PLACEHOLDER_ADDR = new TransportAddress(new InetSocketAddress("127.0.0.1", 9401));

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AUX_TRANSPORT_TYPES_KEY, GRPC_TRANSPORT_SETTING_KEY)
            .put(AUX_TRANSPORT_PORT.getConcreteSettingForNamespace(GRPC_TRANSPORT_SETTING_KEY).getKey(), TEST_AUX_PORTS.getPortRangeString())
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GrpcPlugin.class);
    }

    public void testStartGrpcTransportClusterHealth() throws Exception {
        // REST api cluster health
        ClusterHealthResponse healthResponse = client().admin().cluster()
            .prepareHealth()
            .get();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());

        // gRPC transport service health
        try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(PLACEHOLDER_ADDR).build()) {
            assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
        }
    }
}
