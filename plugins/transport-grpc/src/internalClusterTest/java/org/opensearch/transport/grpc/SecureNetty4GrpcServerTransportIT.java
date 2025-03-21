/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckResponse;

import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;
import static org.opensearch.transport.grpc.SecureNetty4GrpcServerTransportTests.getNoTrustClientAuthNoneTLSSettingsProvider;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;

public class SecureNetty4GrpcServerTransportIT extends OpenSearchIntegTestCase {

    // public for plugin service
    public static class MockSecurityPlugin extends Plugin implements NetworkPlugin {

        public MockSecurityPlugin() {}

        @Override
        public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) {
            return Optional.of(new SecureSettingsFactory() {
                @Override
                public Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings) {
                    return Optional.empty();
                }

                @Override
                public Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings) {
                    return Optional.empty();
                }

                @Override
                public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) {
                    return Optional.of(getNoTrustClientAuthNoneTLSSettingsProvider());
                }
            });
        }
    }

    private TransportAddress randomNetty4GrpcServerTransportAddr() {
        List<TransportAddress> addresses = new ArrayList<>();
        for (SecureNetty4GrpcServerTransport transport : internalCluster().getInstances(SecureNetty4GrpcServerTransport.class)) {
            TransportAddress tAddr = new TransportAddress(transport.getBoundAddress().publishAddress().address());
            addresses.add(tAddr);
        }
        return randomFrom(addresses);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AUX_TRANSPORT_TYPES_KEY, GRPC_SECURE_TRANSPORT_SETTING_KEY)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GrpcPlugin.class, MockSecurityPlugin.class);
    }

    public void testSecureGrpcTransportClusterHealth() throws Exception {
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().get();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());
        try (
            NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr())
                .setSecureSettingsProvider(getNoTrustClientAuthNoneTLSSettingsProvider())
                .build()
        ) {
            assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
        }
    }

    public void testSecureGrpcTransportPlaintextClient() throws Exception {
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().get();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());
        try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(randomNetty4GrpcServerTransportAddr()).build()) {
            assertThrows(StatusRuntimeException.class, client::checkHealth);
        }
    }
}
