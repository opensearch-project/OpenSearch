/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.spi.GrpcServiceFactory;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.grpc.BindableService;
import io.grpc.protobuf.services.HealthStatusManager;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_EXECUTOR_COUNT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_KEEPALIVE_TIMEOUT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONNECTION_AGE;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONNECTION_IDLE;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_MSG_SIZE;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT;
import static org.opensearch.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;
import static org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT;
import static org.opensearch.transport.grpc.ssl.SecureSettingsHelpers.getServerClientAuthNone;
import static org.mockito.Mockito.when;

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

    @Mock
    private ExtensiblePlugin.ExtensionLoader extensionLoader;

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
        assertTrue("SETTING_GRPC_EXECUTOR_COUNT should be included", settings.contains(SETTING_GRPC_EXECUTOR_COUNT));
        assertTrue("SETTING_GRPC_PUBLISH_PORT should be included", settings.contains(SETTING_GRPC_PUBLISH_PORT));
        assertTrue(
            "SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS should be included",
            settings.contains(SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS)
        );
        assertTrue("SETTING_GRPC_MAX_MSG_SIZE should be included", settings.contains(SETTING_GRPC_MAX_MSG_SIZE));
        assertTrue("SETTING_GRPC_MAX_CONNECTION_AGE should be included", settings.contains(SETTING_GRPC_MAX_CONNECTION_AGE));
        assertTrue("SETTING_GRPC_MAX_CONNECTION_IDLE should be included", settings.contains(SETTING_GRPC_MAX_CONNECTION_IDLE));
        assertTrue("SETTING_GRPC_KEEPALIVE_TIMEOUT should be included", settings.contains(SETTING_GRPC_KEEPALIVE_TIMEOUT));

        // Verify the number of settings
        assertEquals("Should return 13 settings", 13, settings.size());
    }

    private static class LoadableMockServiceFactory implements GrpcServiceFactory {

        public LoadableMockServiceFactory() {}

        @Override
        public String plugin() {
            return "MockHealthServicePluginServiceFactory";
        }

        @Override
        public List<BindableService> build() {
            return List.of(new HealthStatusManager().getHealthService());
        }
    };

    public void testGetQueryUtilsBeforeCreateComponents() {
        // Create a new plugin instance without calling createComponents
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Test that getQueryUtils throws IllegalStateException when queryUtils is not initialized
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> newPlugin.getQueryUtils());

        assertEquals("Query utils not initialized. Make sure createComponents has been called.", exception.getMessage());
    }

    public void testGetQueryUtilsAfterCreateComponents() {
        // Test that getQueryUtils returns the queryUtils instance after createComponents is called
        assertNotNull("QueryUtils should not be null after createComponents", plugin.getQueryUtils());
    }

    public void testGetAuxTransports() {
        Settings settings = Settings.builder().put(SETTING_GRPC_PORT.getKey(), "9200-9300").build();

        Map<String, Supplier<AuxTransport>> transports = plugin.getAuxTransports(
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
        AuxTransport transport = transports.get(GRPC_TRANSPORT_SETTING_KEY).get();
        assertTrue("Should return a Netty4GrpcServerTransport instance", transport instanceof Netty4GrpcServerTransport);
    }

    public void testGetSecureAuxTransports() {
        Settings settings = Settings.builder().put(SETTING_GRPC_SECURE_PORT.getKey(), "9200-9300").build();

        Map<String, Supplier<AuxTransport>> transports = plugin.getSecureAuxTransports(
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
        AuxTransport transport = transports.get(GRPC_SECURE_TRANSPORT_SETTING_KEY).get();
        assertTrue("Should return a SecureNetty4GrpcServerTransport instance", transport instanceof SecureNetty4GrpcServerTransport);
    }

    public void testGetAuxTransportsWithNullClient() {
        Settings settings = Settings.builder().put(SETTING_GRPC_PORT.getKey(), "9200-9300").build();

        // Create a new plugin instance without initializing the client
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Expect a RuntimeException when client is null
        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> newPlugin.getAuxTransports(settings, threadPool, circuitBreakerService, networkService, clusterSettings, tracer)
        );

        assertEquals("createComponents must be called first to initialize server provided resources.", exception.getMessage());
    }

    public void testGetSecureAuxTransportsWithNullClient() {
        Settings settings = Settings.builder().put(SETTING_GRPC_SECURE_PORT.getKey(), "9200-9300").build();

        // Create a new plugin instance without initializing the client
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Expect a RuntimeException when client is null
        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> newPlugin.getSecureAuxTransports(
                settings,
                threadPool,
                circuitBreakerService,
                networkService,
                clusterSettings,
                getServerClientAuthNone(),
                tracer
            )
        );

        assertEquals("createComponents must be called first to initialize server provided resources.", exception.getMessage());
    }

    public void testGetAuxTransportsWithServiceFactories() {
        GrpcPlugin newPlugin = new GrpcPlugin();
        newPlugin.createComponents(Mockito.mock(Client.class), null, null, null, null, null, null, null, null, null, null);
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(GrpcServiceFactory.class)).thenReturn(List.of(new LoadableMockServiceFactory()));
        plugin.loadExtensions(mockLoader);
        Map<String, Supplier<AuxTransport>> transports = plugin.getAuxTransports(
            Settings.EMPTY,
            threadPool,
            circuitBreakerService,
            networkService,
            clusterSettings,
            tracer
        );
        assertTrue("Should contain GRPC_TRANSPORT_SETTING_KEY", transports.containsKey(GRPC_TRANSPORT_SETTING_KEY));
        AuxTransport transport = transports.get(GRPC_TRANSPORT_SETTING_KEY).get();
        assertTrue(transport instanceof Netty4GrpcServerTransport);
    }

    public void testGetSecureAuxTransportsWithServiceFactories() {
        GrpcPlugin newPlugin = new GrpcPlugin();
        newPlugin.createComponents(Mockito.mock(Client.class), null, null, null, null, null, null, null, null, null, null);
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(GrpcServiceFactory.class)).thenReturn(List.of(new LoadableMockServiceFactory()));
        plugin.loadExtensions(mockLoader);
        Map<String, Supplier<AuxTransport>> transports = plugin.getSecureAuxTransports(
            Settings.EMPTY,
            threadPool,
            circuitBreakerService,
            networkService,
            clusterSettings,
            Mockito.mock(SecureAuxTransportSettingsProvider.class),
            tracer
        );
        assertTrue("Should contain GRPC_SECURE_TRANSPORT_SETTING_KEY", transports.containsKey(GRPC_SECURE_TRANSPORT_SETTING_KEY));
        AuxTransport transport = transports.get(GRPC_SECURE_TRANSPORT_SETTING_KEY).get();
        assertTrue(transport instanceof SecureNetty4GrpcServerTransport);
    }

    public void testLoadExtensions() {
        // Create a new plugin instance
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Create a mock extension loader
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);

        // Create a list of mock converters
        List<QueryBuilderProtoConverter> mockConverters = new ArrayList<>();
        mockConverters.add(Mockito.mock(QueryBuilderProtoConverter.class));
        mockConverters.add(Mockito.mock(QueryBuilderProtoConverter.class));

        // Set up the mock loader to return the mock converters
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(mockConverters);

        // Call loadExtensions
        newPlugin.loadExtensions(mockLoader);

        // Verify that the converters were loaded
        assertEquals(2, newPlugin.getQueryConverters().size());
    }

    public void testLoadExtensionsWithNullExtensions() {
        // Create a new plugin instance
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Create a mock extension loader
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);

        // Set up the mock loader to return null
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(null);

        // Call loadExtensions
        newPlugin.loadExtensions(mockLoader);

        // Verify that no converters were loaded
        assertEquals(0, newPlugin.getQueryConverters().size());
    }

    public void testCreateComponents() {
        // Create a new plugin instance
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Create a mock converter
        QueryBuilderProtoConverter mockConverter = Mockito.mock(QueryBuilderProtoConverter.class);

        // Add the mock converter to the plugin
        newPlugin.loadExtensions(extensionLoader);
        when(extensionLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(List.of(mockConverter));

        // Call createComponents
        Collection<Object> components = newPlugin.createComponents(
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

        // Verify that the queryUtils instance was created and is available
        assertNotNull("QueryUtils should be initialized after createComponents", newPlugin.getQueryUtils());
    }

    public void testCreateComponentsWithExternalConverters() {
        // Create a new plugin instance
        GrpcPlugin newPlugin = new GrpcPlugin();

        // Create a mock converter that will be registered
        QueryBuilderProtoConverter mockConverter = Mockito.mock(QueryBuilderProtoConverter.class);
        when(mockConverter.getHandledQueryCase()).thenReturn(QueryContainer.QueryContainerCase.MATCH_ALL);

        // Create a mock extension loader that returns the converter
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(List.of(mockConverter));

        // Load the extensions first
        newPlugin.loadExtensions(mockLoader);

        // Verify the converter was added to the queryConverters list
        assertEquals("Should have 1 query converter loaded", 1, newPlugin.getQueryConverters().size());

        // Call createComponents to trigger registration of external converters
        Collection<Object> components = newPlugin.createComponents(
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

        // Verify that the queryUtils instance was created and is available
        assertNotNull("QueryUtils should be initialized after createComponents", newPlugin.getQueryUtils());

        // Verify that the external converter was registered by checking it was called
        // Note: getHandledQueryCase() is called 3 times:
        // 1. In loadExtensions() for logging
        // 2. In createComponents() for logging
        // 3. In QueryBuilderProtoConverterSpiRegistry.registerConverter() for registration
        Mockito.verify(mockConverter, Mockito.times(3)).getHandledQueryCase();

        // Verify that setRegistry was called to inject the registry
        // Note: setRegistry is called 2 times:
        // 1. In createComponents() when processing external converters
        // 2. In updateRegistryOnAllConverters() to ensure all converters have the complete registry
        Mockito.verify(mockConverter, Mockito.times(2)).setRegistry(Mockito.any());
    }
}
