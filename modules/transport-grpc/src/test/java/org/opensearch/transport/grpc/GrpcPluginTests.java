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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.interceptor.GrpcInterceptorChain;
import org.opensearch.transport.grpc.spi.GrpcInterceptorProvider;
import org.opensearch.transport.grpc.spi.GrpcInterceptorProvider.OrderedGrpcInterceptor;
import org.opensearch.transport.grpc.spi.GrpcServiceFactory;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.grpc.BindableService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
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
import static org.mockito.Mockito.doAnswer;
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

        // Mock ThreadPool and ThreadContext
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        // Set the client in the plugin
        plugin.createComponents(
            client,
            null, // ClusterService
            threadPool, // ThreadPool (now properly mocked)
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
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        newPlugin.createComponents(Mockito.mock(Client.class), null, mockThreadPool, null, null, null, null, null, null, null, null);
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
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        newPlugin.createComponents(Mockito.mock(Client.class), null, mockThreadPool, null, null, null, null, null, null, null, null);
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

        // Mock ThreadPool for createComponents
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        // Call createComponents
        Collection<Object> components = newPlugin.createComponents(
            client,
            null, // ClusterService
            mockThreadPool, // ThreadPool
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

        // Mock ThreadPool for createComponents
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        // Call createComponents to trigger registration of external converters
        Collection<Object> components = newPlugin.createComponents(
            client,
            null, // ClusterService
            mockThreadPool, // ThreadPool
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

    // Test cases for gRPC interceptor functionality

    public void testLoadExtensionsWithGrpcInterceptors() {
        testInterceptorLoading(List.of(1, 2), null);
    }

    public void testLoadExtensionsWithGrpcInterceptorsOrdering() {
        testInterceptorLoading(List.of(3, 1, 2), null); // Out of order - should be sorted
    }

    public void testLoadExtensionsWithDuplicateGrpcInterceptorOrder() {
        GrpcPlugin plugin = new GrpcPlugin();
        ExtensiblePlugin.ExtensionLoader mockLoader = createMockLoader(List.of(1, 1));

        assertDoesNotThrow(() -> plugin.loadExtensions(mockLoader));

        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> plugin.createComponents(client, null, mockThreadPool, null, null, null, null, null, null, null, null)
        );

        String errorMessage = exception.getMessage();
        assertTrue(errorMessage.contains("Multiple gRPC interceptors have the same order value [1]"));
        assertTrue(errorMessage.contains("ServerInterceptor")); // Mock class name will contain this
        assertTrue(errorMessage.contains("Each interceptor must have a unique order value"));
    }

    public void testLoadExtensionsWithMultipleProvidersAndDuplicateOrder() {
        GrpcPlugin plugin = new GrpcPlugin();
        ExtensiblePlugin.ExtensionLoader mockLoader = createMockLoaderWithMultipleProviders(List.of(List.of(5), List.of(5)));

        // loadExtensions should succeed
        assertDoesNotThrow(() -> plugin.loadExtensions(mockLoader));

        // createComponents should fail with duplicate order
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> plugin.createComponents(client, null, mockThreadPool, null, null, null, null, null, null, null, null)
        );

        String errorMessage = exception.getMessage();
        assertTrue(errorMessage.contains("Multiple gRPC interceptors have the same order value [5]"));
        assertTrue(errorMessage.contains("ServerInterceptor"));
        assertTrue(errorMessage.contains("Each interceptor must have a unique order value"));
    }

    public void testLoadExtensionsWithNullGrpcInterceptorProviders() {
        testInterceptorLoading(null, null);
    }

    public void testLoadExtensionsWithEmptyGrpcInterceptorList() {
        testInterceptorLoading(List.of(), null);
    }

    public void testLoadExtensionsWithSameExplicitOrderInterceptors() {
        GrpcPlugin plugin = new GrpcPlugin();
        ExtensiblePlugin.ExtensionLoader mockLoader = createMockLoader(List.of(5, 5));

        assertDoesNotThrow(() -> plugin.loadExtensions(mockLoader));

        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> plugin.createComponents(client, null, mockThreadPool, null, null, null, null, null, null, null, null)
        );

        String errorMessage = exception.getMessage();
        assertTrue(errorMessage.contains("Multiple gRPC interceptors have the same order value [5]"));
        assertTrue(errorMessage.contains("ServerInterceptor"));
        assertTrue(errorMessage.contains("Each interceptor must have a unique order value"));
    }

    // Test cases for interceptor chain failure handling

    public void testInterceptorExceptionDuringRequestPhase() {
        testInterceptorLoading(List.of(1, 2), null); // Runtime failures don't affect loading
    }

    // Test cases for actual request processing with interceptors

    public void testInterceptorRequestProcessingWithSuccess() {
        testRequestProcessing(
            List.of(new TestRequestInterceptor(1, true, "Auth check passed")),
            true,
            "Request should succeed when interceptor succeeds"
        );
    }

    public void testInterceptorRequestProcessingWithFailure() {
        testRequestProcessing(
            List.of(new TestRequestInterceptor(1, false, "Auth failed")),
            false,
            "Request should fail when interceptor fails"
        );
    }

    public void testMultipleInterceptorsRequestProcessing() {
        testRequestProcessing(
            List.of(
                new TestRequestInterceptor(1, true, "Auth passed"),
                new TestRequestInterceptor(2, true, "Rate limit passed"),
                new TestRequestInterceptor(3, true, "Validation passed")
            ),
            true,
            "Request should succeed when all interceptors succeed"
        );
    }

    public void testMultipleInterceptorsWithEarlyFailure() {
        testRequestProcessing(
            List.of(
                new TestRequestInterceptor(1, true, "Auth passed"),
                new TestRequestInterceptor(2, false, "Rate limit exceeded"), // Fails here
                new TestRequestInterceptor(3, true, "Validation passed")     // Should not execute
            ),
            false,
            "Request should fail when any interceptor fails"
        );
    }

    public void testInterceptorExceptionHandling() {
        testRequestProcessingWithException(
            new TestExceptionThrowingInterceptor(1, "Security check failed"),
            SecurityException.class,
            "Security check failed"
        );
    }

    public void testInterceptorChainExceptionPropagation() {
        // Test that exceptions in interceptor chain stop processing
        @SuppressWarnings("unchecked")
        ServerCall<String, String> mockCall = Mockito.mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<String, String> mockHandler = Mockito.mock(ServerCallHandler.class);
        Metadata headers = new Metadata();

        TestExceptionThrowingInterceptor throwingInterceptor = new TestExceptionThrowingInterceptor(2, "Rate limit service down");

        Exception thrown = expectThrows(
            RuntimeException.class,
            () -> { throwingInterceptor.interceptCall(mockCall, headers, mockHandler); }
        );

        assertEquals("Rate limit service down", thrown.getMessage());
    }

    // Generic helper methods for cleaner, more maintainable tests

    /**
     * Generic interceptor loading test helper
     * @param orders List of interceptor orders (null for no interceptors)
     * @param expectedException Expected exception class (null if no exception expected)
     */
    private void testInterceptorLoading(List<Integer> orders, Class<? extends Exception> expectedException) {
        GrpcPlugin plugin = new GrpcPlugin();
        ExtensiblePlugin.ExtensionLoader mockLoader = createMockLoader(orders);

        if (expectedException != null) {
            expectThrows(expectedException, () -> plugin.loadExtensions(mockLoader));
        } else {
            assertDoesNotThrow(() -> plugin.loadExtensions(mockLoader));
        }
    }

    /**
     * Test interceptor loading with multiple providers
     */
    private void testInterceptorLoadingWithMultipleProviders(
        List<List<Integer>> providerOrders,
        Class<? extends Exception> expectedException
    ) {
        GrpcPlugin plugin = new GrpcPlugin();
        ExtensiblePlugin.ExtensionLoader mockLoader = createMockLoaderWithMultipleProviders(providerOrders);

        if (expectedException != null) {
            expectThrows(expectedException, () -> plugin.loadExtensions(mockLoader));
        } else {
            assertDoesNotThrow(() -> plugin.loadExtensions(mockLoader));
        }
    }

    /**
     * Creates a mock extension loader with interceptors of given orders
     */
    private ExtensiblePlugin.ExtensionLoader createMockLoader(List<Integer> orders) {
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(null);

        if (orders == null) {
            when(mockLoader.loadExtensions(GrpcInterceptorProvider.class)).thenReturn(null);
        } else if (orders.isEmpty()) {
            GrpcInterceptorProvider mockProvider = Mockito.mock(GrpcInterceptorProvider.class);
            when(mockProvider.getOrderedGrpcInterceptors(Mockito.any())).thenReturn(new ArrayList<>());
            when(mockLoader.loadExtensions(GrpcInterceptorProvider.class)).thenReturn(List.of(mockProvider));
        } else {
            List<OrderedGrpcInterceptor> interceptors = orders.stream().map(order -> createMockInterceptor(order)).toList();

            GrpcInterceptorProvider mockProvider = Mockito.mock(GrpcInterceptorProvider.class);
            when(mockProvider.getOrderedGrpcInterceptors(Mockito.any())).thenReturn(interceptors);
            when(mockLoader.loadExtensions(GrpcInterceptorProvider.class)).thenReturn(List.of(mockProvider));
        }

        return mockLoader;
    }

    /**
     * Creates a mock extension loader with multiple providers
     */
    private ExtensiblePlugin.ExtensionLoader createMockLoaderWithMultipleProviders(List<List<Integer>> providerOrders) {
        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(null);

        List<GrpcInterceptorProvider> providers = providerOrders.stream().map(orders -> {
            List<OrderedGrpcInterceptor> interceptors = orders.stream().map(this::createMockInterceptor).toList();
            GrpcInterceptorProvider provider = Mockito.mock(GrpcInterceptorProvider.class);
            when(provider.getOrderedGrpcInterceptors(Mockito.any())).thenReturn(interceptors);
            return provider;
        }).toList();

        when(mockLoader.loadExtensions(GrpcInterceptorProvider.class)).thenReturn(providers);
        return mockLoader;
    }

    /**
     * Test actual request processing with interceptors
     */
    private void testRequestProcessing(List<TestRequestInterceptor> interceptors, boolean shouldSucceed, String description) {
        // Create mock server call and handler
        @SuppressWarnings("unchecked")
        ServerCall<String, String> mockCall = Mockito.mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<String, String> mockHandler = Mockito.mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<String> mockListener = Mockito.mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();

        when(mockHandler.startCall(Mockito.any(), Mockito.any())).thenReturn(mockListener);

        // Track if call.close() was called (indicates failure)
        AtomicBoolean callClosed = new AtomicBoolean(false);
        doAnswer(invocation -> {
            callClosed.set(true);
            return null;
        }).when(mockCall).close(Mockito.any(Status.class), Mockito.any(Metadata.class));

        // Execute the interceptors in chain (simulating real gRPC behavior)
        ServerCallHandler<String, String> currentHandler = mockHandler;

        for (TestRequestInterceptor interceptor : interceptors) {
            final ServerCallHandler<String, String> nextHandler = currentHandler;
            ServerCall.Listener<String> result = interceptor.interceptCall(mockCall, headers, nextHandler);

            // Check if the call was closed (indicating failure)
            if (callClosed.get()) {
                // A failure occurred - this is expected for failure test cases
                if (!shouldSucceed) {
                    // This is expected - test passes
                    return;
                } else {
                    fail("Unexpected failure in success test case");
                }
            }

            // If we reach here, the interceptor succeeded
            assertNotNull(result);

            // Create a new handler for the next interceptor that continues the chain
            currentHandler = new ServerCallHandler<String, String>() {
                @Override
                public ServerCall.Listener<String> startCall(ServerCall<String, String> call, Metadata headers) {
                    return result;
                }
            };
        }

        // If we reach here and shouldSucceed is false, the test failed to fail as expected
        if (!shouldSucceed) {
            fail("Expected failure but all interceptors succeeded");
        }

        // For success case, ensure call was not closed
        assertFalse("Call should not be closed for success case", callClosed.get());
    }

    /**
     * Test request processing with exception throwing interceptor
     */
    private void testRequestProcessingWithException(
        TestExceptionThrowingInterceptor interceptor,
        Class<? extends Exception> expectedExceptionType,
        String expectedMessage
    ) {
        @SuppressWarnings("unchecked")
        ServerCall<String, String> mockCall = Mockito.mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<String, String> mockHandler = Mockito.mock(ServerCallHandler.class);
        Metadata headers = new Metadata();

        Exception thrown = expectThrows(Exception.class, () -> { interceptor.interceptCall(mockCall, headers, mockHandler); });

        assertTrue(expectedExceptionType.isInstance(thrown));
        assertTrue(thrown.getMessage().contains(expectedMessage));
    }

    /**
     * Creates a mock interceptor with given order
     */
    private OrderedGrpcInterceptor createMockInterceptor(int order) {
        OrderedGrpcInterceptor mock = Mockito.mock(OrderedGrpcInterceptor.class);
        when(mock.order()).thenReturn(order);
        when(mock.getInterceptor()).thenReturn(Mockito.mock(ServerInterceptor.class));
        return mock;
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            fail("Expected no exception, but got: " + e.getMessage());
        }
    }

    /**
     * Test interceptor that simulates request processing with success/failure
     */
    private static class TestRequestInterceptor implements ServerInterceptor {
        private final int order;
        private final boolean shouldSucceed;
        private final String message;

        public TestRequestInterceptor(int order, boolean shouldSucceed, String message) {
            this.order = order;
            this.shouldSucceed = shouldSucceed;
            this.message = message;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
        ) {

            // Simulate request processing logic
            if (!shouldSucceed) {
                // Fail the request with proper gRPC status
                call.close(Status.PERMISSION_DENIED.withDescription(message), new Metadata());
                return new ServerCall.Listener<ReqT>() {
                }; // Empty listener for failed call
            }

            // Success case - continue to next interceptor/handler
            return next.startCall(call, headers);
        }

        public int getOrder() {
            return order;
        }
    }

    /**
     * Test interceptor that throws exceptions during request processing
     */
    private static class TestExceptionThrowingInterceptor implements ServerInterceptor {
        private final int order;
        private final String message;

        public TestExceptionThrowingInterceptor(int order, String message) {
            this.order = order;
            this.message = message;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
        ) {

            // Simulate different types of exceptions that can occur during request processing
            if (message.contains("Security")) {
                throw new SecurityException(message);
            } else if (message.contains("Rate limit")) {
                throw new RuntimeException(message);
            } else {
                throw new IllegalStateException(message);
            }
        }

        public int getOrder() {
            return order;
        }
    }

    // ===========================================
    // Test cases for GrpcInterceptorChain
    // ===========================================

    public void testGrpcInterceptorChainWithSuccessfulInterceptors() {
        List<OrderedGrpcInterceptor> interceptors = List.of(createTestInterceptor(10, false), createTestInterceptor(20, false));

        testGrpcInterceptorChain(interceptors, true, null);
    }

    public void testGrpcInterceptorChainWithFailingInterceptor() {
        List<OrderedGrpcInterceptor> interceptors = List.of(
            createTestInterceptor(10, false), // Success
            createTestInterceptor(20, true),  // Failure
            createTestInterceptor(30, false)  // Should not execute
        );

        testGrpcInterceptorChain(interceptors, false, "Test failure");
    }

    public void testGrpcInterceptorChainEmptyList() {
        List<OrderedGrpcInterceptor> interceptors = List.of();
        testGrpcInterceptorChain(interceptors, true, null);
    }

    public void testGrpcInterceptorChainOrdering() {
        List<String> executionOrder = new ArrayList<>();

        List<OrderedGrpcInterceptor> interceptors = List.of(
            createTestInterceptorWithCallback(30, executionOrder, "Third"),
            createTestInterceptorWithCallback(10, executionOrder, "First"),
            createTestInterceptorWithCallback(20, executionOrder, "Second")
        );

        // Sort them as GrpcPlugin would
        interceptors = new ArrayList<>(interceptors);
        interceptors.sort(Comparator.comparingInt(OrderedGrpcInterceptor::order));

        testGrpcInterceptorChain(interceptors, true, null);

        // Verify execution order
        assertEquals(3, executionOrder.size());
        assertEquals("First", executionOrder.get(0));
        assertEquals("Second", executionOrder.get(1));
        assertEquals("Third", executionOrder.get(2));
    }

    public void testGrpcInterceptorChainIntegrationWithPlugin() {
        // Test that GrpcPlugin correctly creates and uses GrpcInterceptorChain
        GrpcInterceptorProvider mockProvider = Mockito.mock(GrpcInterceptorProvider.class);
        List<OrderedGrpcInterceptor> interceptors = List.of(
            createTestInterceptor(10, false),
            createTestInterceptor(20, false),
            createTestInterceptor(30, false)
        );
        when(mockProvider.getOrderedGrpcInterceptors(Mockito.any())).thenReturn(interceptors);

        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(null);
        when(mockLoader.loadExtensions(GrpcInterceptorProvider.class)).thenReturn(List.of(mockProvider));

        GrpcPlugin plugin = new GrpcPlugin();

        // Should not throw exception and should load providers
        assertDoesNotThrow(() -> plugin.loadExtensions(mockLoader));

        // Need to call createComponents to actually initialize the chain
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        assertDoesNotThrow(() -> plugin.createComponents(client, null, mockThreadPool, null, null, null, null, null, null, null, null));
    }

    public void testGrpcInterceptorChainWithDuplicateOrders() {
        // Test that plugin validation catches duplicate orders
        GrpcInterceptorProvider mockProvider = Mockito.mock(GrpcInterceptorProvider.class);
        List<OrderedGrpcInterceptor> interceptors = List.of(
            createTestInterceptor(10, false),
            createTestInterceptor(10, false) // Duplicate order
        );
        when(mockProvider.getOrderedGrpcInterceptors(Mockito.any())).thenReturn(interceptors);

        ExtensiblePlugin.ExtensionLoader mockLoader = Mockito.mock(ExtensiblePlugin.ExtensionLoader.class);
        when(mockLoader.loadExtensions(QueryBuilderProtoConverter.class)).thenReturn(null);
        when(mockLoader.loadExtensions(GrpcInterceptorProvider.class)).thenReturn(List.of(mockProvider));

        GrpcPlugin plugin = new GrpcPlugin();

        // Load extensions first
        plugin.loadExtensions(mockLoader);

        // Mock ThreadPool for createComponents
        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY));

        // Should throw exception due to duplicate orders during createComponents
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> plugin.createComponents(client, null, mockThreadPool, null, null, null, null, null, null, null, null)
        );

        // Verify error message includes order value and interceptor class names
        String errorMessage = exception.getMessage();
        assertTrue(errorMessage.contains("Multiple gRPC interceptors have the same order value [10]"));
        assertTrue(errorMessage.contains("GrpcPluginTests"));
        assertTrue(errorMessage.contains("Each interceptor must have a unique order value"));
    }

    /**
     * Helper method to test GrpcInterceptorChain behavior
     */
    private void testGrpcInterceptorChain(List<OrderedGrpcInterceptor> interceptors, boolean shouldSucceed, String expectedErrorMessage) {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);

        @SuppressWarnings("unchecked")
        ServerCall<String, String> mockCall = Mockito.mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<String, String> mockHandler = Mockito.mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<String> mockListener = Mockito.mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();

        when(mockHandler.startCall(Mockito.any(), Mockito.any())).thenReturn(mockListener);

        // Track if call.close() was called
        AtomicBoolean callClosed = new AtomicBoolean(false);
        AtomicReference<Status> closedStatus = new AtomicReference<>();
        doAnswer(invocation -> {
            callClosed.set(true);
            closedStatus.set(invocation.getArgument(0));
            return null;
        }).when(mockCall).close(Mockito.any(Status.class), Mockito.any(Metadata.class));

        // Execute the chain
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        if (shouldSucceed) {
            assertNotNull("Should return a listener for successful chain", result);
            assertFalse("Call should not be closed for successful chain", callClosed.get());
        } else {
            // For failure cases, the call should be closed
            assertTrue("Call should be closed for failed chain", callClosed.get());
            if (expectedErrorMessage != null) {
                assertNotNull("Should have closed status", closedStatus.get());
                assertTrue(
                    "Error message should contain expected text",
                    closedStatus.get().getDescription().contains(expectedErrorMessage)
                );
            }
        }
    }

    /**
     * Creates a test interceptor that can succeed or fail
     */
    private OrderedGrpcInterceptor createTestInterceptor(int order, boolean shouldFail) {
        return new OrderedGrpcInterceptor() {
            @Override
            public int order() {
                return order;
            }

            @Override
            public ServerInterceptor getInterceptor() {
                return new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call,
                        Metadata headers,
                        ServerCallHandler<ReqT, RespT> next
                    ) {
                        if (shouldFail) {
                            throw new RuntimeException("Test failure");
                        }
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }

    /**
     * Creates a test interceptor that tracks execution order
     */
    private OrderedGrpcInterceptor createTestInterceptorWithCallback(int order, List<String> executionOrder, String name) {
        return new OrderedGrpcInterceptor() {
            @Override
            public int order() {
                return order;
            }

            @Override
            public ServerInterceptor getInterceptor() {
                return new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call,
                        Metadata headers,
                        ServerCallHandler<ReqT, RespT> next
                    ) {
                        executionOrder.add(name);
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }

}
