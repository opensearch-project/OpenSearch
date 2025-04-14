/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_HOST;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT;
import static org.opensearch.plugin.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT;
import static org.opensearch.plugin.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthNone;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT;

public class GrpcPluginTests extends OpenSearchTestCase {

    private GrpcPlugin plugin;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private CircuitBreakerService circuitBreakerService;

    @Mock
    private Client client;

    private NetworkService networkService;

    private ClusterSettings clusterSettings;

    @Mock
    private Tracer tracer;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        // Use real instances instead of mocks for final classes
        networkService = new NetworkService(List.of());

        // Create a real ClusterSettings instance with the plugin's settings
        plugin = new GrpcPlugin();

        // Set the client in the plugin
        plugin.createComponents(
            client,
            null, // ClusterService
            null, // ThreadPool
            null, // ResourceWatcherService
            null, // ScriptService
            null, // NamedXContentRegistry
            null, // Environment
            null, // NodeEnvironment
            null, // NamedWriteableRegistry
            null, // IndexNameExpressionResolver
            null  // Supplier<RepositoriesService>
        );

        clusterSettings = new ClusterSettings(Settings.EMPTY, plugin.getSettings().stream().collect(java.util.stream.Collectors.toSet()));
    }

    public void testGetSettings() {
        List<Setting<?>> settings = plugin.getSettings();

        // Verify that all expected settings are returned
        assertTrue("SETTING_GRPC_PORT should be included", settings.contains(SETTING_GRPC_PORT));
        assertTrue("SETTING_GRPC_SECURE_PORT should be included", settings.contains(SETTING_GRPC_SECURE_PORT));
        assertTrue("SETTING_GRPC_HOST should be included", settings.contains(SETTING_GRPC_HOST));
        assertTrue("SETTING_GRPC_PUBLISH_HOST should be included", settings.contains(SETTING_GRPC_PUBLISH_HOST));
        assertTrue("SETTING_GRPC_BIND_HOST should be included", settings.contains(SETTING_GRPC_BIND_HOST));
        assertTrue("SETTING_GRPC_WORKER_COUNT should be included", settings.contains(SETTING_GRPC_WORKER_COUNT));
        assertTrue("SETTING_GRPC_PUBLISH_PORT should be included", settings.contains(SETTING_GRPC_PUBLISH_PORT));

        // Verify the number of settings
        assertEquals("Should return 7 settings", 7, settings.size());
    }

    public void testGetAuxTransports() {
        Settings settings = Settings.builder().put(SETTING_GRPC_PORT.getKey(), "9200-9300").build();

        Map<String, Supplier<NetworkPlugin.AuxTransport>> transports = plugin.getAuxTransports(
            settings,
            threadPool,
            circuitBreakerService,
            networkService,
            clusterSettings,
            tracer
        );

        // Verify that the transport map contains the expected key
        assertTrue("Should contain GRPC_TRANSPORT_SETTING_KEY", transports.containsKey(GRPC_TRANSPORT_SETTING_KEY));

        // Verify that the supplier returns a Netty4GrpcServerTransport instance
        NetworkPlugin.AuxTransport transport = transports.get(GRPC_TRANSPORT_SETTING_KEY).get();
        assertTrue("Should return a Netty4GrpcServerTransport instance", transport instanceof Netty4GrpcServerTransport);
    }

    public void testGetSecureAuxTransports() {
        Settings settings = Settings.builder().put(SETTING_GRPC_SECURE_PORT.getKey(), "9200-9300").build();

        Map<String, Supplier<NetworkPlugin.AuxTransport>> transports = plugin.getSecureAuxTransports(
            settings,
            threadPool,
            circuitBreakerService,
            networkService,
            clusterSettings,
            getServerClientAuthNone(),
            tracer
        );

        // Verify that the transport map contains the expected key
        assertTrue("Should contain GRPC_SECURE_TRANSPORT_SETTING_KEY", transports.containsKey(GRPC_SECURE_TRANSPORT_SETTING_KEY));

        // Verify that the supplier returns a Netty4GrpcServerTransport instance
        NetworkPlugin.AuxTransport transport = transports.get(GRPC_SECURE_TRANSPORT_SETTING_KEY).get();
        assertTrue("Should return a SecureNetty4GrpcServerTransport instance", transport instanceof SecureNetty4GrpcServerTransport);
    }
}
