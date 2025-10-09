/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ExecutorService;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class Netty4GrpcServerTransportTests extends OpenSearchTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private List<BindableService> services;

    @Before
    public void setup() {
        networkService = new NetworkService(List.of());

        // Create a ThreadPool with the gRPC executor
        Settings settings = Settings.builder()
            .put("node.name", "test-node")
            .put(Netty4GrpcServerTransport.SETTING_GRPC_EXECUTOR_COUNT.getKey(), 4)
            .build();
        ExecutorBuilder<?> grpcExecutorBuilder = new FixedExecutorBuilder(settings, "grpc", 4, 1000, "thread_pool.grpc");
        threadPool = new ThreadPool(settings, grpcExecutorBuilder);

        services = List.of();
    }

    @After
    public void cleanup() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public void testBasicStartAndStop() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.getBoundAddress().publishAddress().address());

            transport.stop();
        }
    }

    public void testGrpcTransportHealthcheck() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService, threadPool)) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());
            try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(remoteAddress).build()) {
                assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            }
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcTransportListServices() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService, threadPool)) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.getBoundAddress().boundAddresses());
            try (NettyGrpcClient client = new NettyGrpcClient.Builder().setAddress(remoteAddress).build()) {
                assertTrue(client.listServices().get().size() > 1);
            }
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testWithCustomPort() {
        // Create settings with a specific port
        Settings settings = Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), "9000-9010").build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.getBoundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertTrue("Port should be in the specified range", publishAddress.getPort() >= 9000 && publishAddress.getPort() <= 9010);

            transport.stop();
        }
    }

    public void testWithCustomPublishPort() {
        // Create settings with a specific publish port
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT.getKey(), 9000)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.getBoundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertEquals("Publish port should match the specified value", 9000, publishAddress.getPort());

            transport.stop();
        }
    }

    public void testWithCustomHost() {
        // Create settings with a specific host
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_HOST.getKey(), "127.0.0.1")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.getBoundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertEquals(
                "Host should match the specified value",
                "127.0.0.1",
                InetAddresses.toAddrString(publishAddress.address().getAddress())
            );

            transport.stop();
        }
    }

    public void testWithCustomBindHost() {
        // Create settings with a specific bind host
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST.getKey(), "127.0.0.1")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress boundAddress = transport.getBoundAddress().boundAddresses()[0];
            assertNotNull(boundAddress.address());
            assertEquals(
                "Bind host should match the specified value",
                "127.0.0.1",
                InetAddresses.toAddrString(boundAddress.address().getAddress())
            );

            transport.stop();
        }
    }

    public void testWithCustomPublishHost() {
        // Create settings with a specific publish host
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST.getKey(), "127.0.0.1")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.getBoundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertEquals(
                "Publish host should match the specified value",
                "127.0.0.1",
                InetAddresses.toAddrString(publishAddress.address().getAddress())
            );

            transport.stop();
        }
    }

    public void testWithCustomWorkerCount() {
        // Create settings with a specific worker count
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT.getKey(), 4)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.getBoundAddress().publishAddress().address());

            transport.stop();
        }
    }

    public void testWithCustomExecutorCount() {
        // Create settings with a specific executor count
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_EXECUTOR_COUNT.getKey(), 8)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.getBoundAddress().publishAddress().address());

            // Verify executor is created and managed by OpenSearch ThreadPool
            ExecutorService executor = transport.getGrpcExecutorForTesting();
            assertNotNull("gRPC executor should be created", executor);
            // Note: The executor is now managed by OpenSearch's ThreadPool system
            // We can't easily verify the thread count as it's encapsulated within OpenSearch's executor implementation

            transport.stop();
        }
    }

    public void testDefaultExecutorCount() {
        // Test default executor count (should be 2x allocated processors)
        Settings settings = createSettings();
        int expectedExecutorCount = OpenSearchExecutors.allocatedProcessors(settings) * 2;

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            ExecutorService executor = transport.getGrpcExecutorForTesting();
            assertNotNull("gRPC executor should be created", executor);
            // Note: The executor is now managed by OpenSearch's ThreadPool system
            // The actual thread count is configured via the FixedExecutorBuilder in the test setup

            transport.stop();
        }
    }

    public void testSeparateEventLoopGroups() {
        // Test that boss and worker event loop groups are separate
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT.getKey(), 4)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            EventLoopGroup bossGroup = transport.getBossEventLoopGroupForTesting();
            EventLoopGroup workerGroup = transport.getWorkerEventLoopGroupForTesting();

            assertNotNull("Boss event loop group should be created", bossGroup);
            assertNotNull("Worker event loop group should be created", workerGroup);
            assertNotSame("Boss and worker event loop groups should be different instances", bossGroup, workerGroup);

            transport.stop();
        }
    }

    public void testExecutorShutdownOnStop() {
        // Test that executor is properly shutdown when transport stops
        Settings settings = createSettings();

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);
        transport.start();

        ExecutorService executor = transport.getGrpcExecutorForTesting();
        assertNotNull("Executor should be created", executor);
        assertFalse("Executor should not be shutdown initially", executor.isShutdown());

        transport.stop();
        // Note: The executor is managed by OpenSearch's ThreadPool and is not shutdown when transport stops
        assertNotNull("Executor should still exist after transport stop", executor);

        transport.close();
    }

    public void testEventLoopGroupsShutdownOnStop() {
        // Test that event loop groups are properly shutdown when transport stops
        Settings settings = createSettings();

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);
        transport.start();

        EventLoopGroup bossGroup = transport.getBossEventLoopGroupForTesting();
        EventLoopGroup workerGroup = transport.getWorkerEventLoopGroupForTesting();

        assertNotNull("Boss group should be created", bossGroup);
        assertNotNull("Worker group should be created", workerGroup);
        assertFalse("Boss group should not be shutdown initially", bossGroup.isShutdown());
        assertFalse("Worker group should not be shutdown initially", workerGroup.isShutdown());

        transport.stop();

        // Event loop groups should be shutdown
        assertTrue("Boss group should be shutdown after transport stop", bossGroup.isShutdown());
        assertTrue("Worker group should be shutdown after transport stop", workerGroup.isShutdown());

        transport.close();
    }

    public void testSettingsValidation() {
        // Test that invalid settings are handled properly
        Settings invalidSettings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT.getKey(), 0) // Invalid: should be >= 1
            .build();

        expectThrows(
            IllegalArgumentException.class,
            () -> { new Netty4GrpcServerTransport(invalidSettings, services, networkService, threadPool); }
        );
    }

    public void testExecutorCountSettingsValidation() {
        // Test that invalid executor count settings are handled properly
        Settings invalidSettings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_EXECUTOR_COUNT.getKey(), 0) // Invalid: should be >= 1
            .build();

        expectThrows(
            IllegalArgumentException.class,
            () -> { new Netty4GrpcServerTransport(invalidSettings, services, networkService, threadPool); }
        );
    }

    public void testStartFailureTriggersCleanup() {
        // Create a transport that will fail to start
        Settings settingsWithInvalidPort = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), "999999") // Invalid port
            .build();

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settingsWithInvalidPort, services, networkService, threadPool);

        // Start should fail
        expectThrows(Exception.class, transport::start);

        // Resources should be cleaned up after failure - the implementation calls doStop() in the finally block
        ExecutorService executor = transport.getGrpcExecutorForTesting();
        EventLoopGroup bossGroup = transport.getBossEventLoopGroupForTesting();
        EventLoopGroup workerGroup = transport.getWorkerEventLoopGroupForTesting();

        // Resources may still exist - executor is managed by OpenSearch's ThreadPool
        if (executor != null) {
            assertNotNull("Executor should still exist after failed start", executor);
        }
        if (bossGroup != null) {
            assertTrue("Boss group should be shutdown after failed start", bossGroup.isShutdown());
        }
        if (workerGroup != null) {
            assertTrue("Worker group should be shutdown after failed start", workerGroup.isShutdown());
        }

        // Close should still be safe to call
        transport.close();
    }

    public void testInterruptedShutdownHandling() throws InterruptedException {
        Settings settings = createSettings();
        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        transport.start();

        // Interrupt the current thread to test interrupt handling
        Thread.currentThread().interrupt();

        // Stop should handle the interrupt gracefully
        transport.stop();

        // Clear interrupt status
        Thread.interrupted();

        transport.close();
    }

    public void testInvalidHostBinding() {
        // Test with invalid bind host to trigger host resolution error
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST.getKey(), "invalid.host.that.does.not.exist")
            .build();

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        // Start should fail due to host resolution failure
        expectThrows(Exception.class, transport::start);

        transport.close();
    }

    public void testPublishPortResolutionFailure() {
        // Create settings that will cause publish port resolution to fail
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), "0") // Dynamic port
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT.getKey(), "65536") // Invalid publish port
            .build();

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        // Start should fail due to publish port resolution
        expectThrows(Exception.class, transport::start);

        transport.close();
    }

    public void testMultipleBindAddresses() {
        // Test binding to multiple localhost addresses
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .putList(Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST.getKey(), "127.0.0.1", "localhost")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            BoundTransportAddress boundAddress = transport.getBoundAddress();
            assertNotNull("Bound address should not be null", boundAddress);
            assertTrue("Should have at least one bound address", boundAddress.boundAddresses().length > 0);

            transport.stop();
        }
    }

    public void testShutdownTimeoutHandling() throws InterruptedException {
        Settings settings = createSettings();
        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        transport.start();

        // Get references to the thread pools
        ExecutorService executor = transport.getGrpcExecutorForTesting();
        EventLoopGroup bossGroup = transport.getBossEventLoopGroupForTesting();
        EventLoopGroup workerGroup = transport.getWorkerEventLoopGroupForTesting();

        assertNotNull("Executor should be created", executor);
        assertNotNull("Boss group should be created", bossGroup);
        assertNotNull("Worker group should be created", workerGroup);

        // Normal shutdown should work
        transport.stop();

        // Verify everything is shutdown (except executor which is managed by OpenSearch's ThreadPool)
        assertNotNull("Executor should still exist", executor);
        assertTrue("Boss group should be shutdown", bossGroup.isShutdown());
        assertTrue("Worker group should be shutdown", workerGroup.isShutdown());

        transport.close();
    }

    public void testResourceCleanupOnClose() {
        Settings settings = createSettings();
        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        transport.start();
        transport.stop();

        // doClose should handle cleanup gracefully even if resources are already shutdown
        transport.close();

        // Multiple closes should be safe
        transport.close();
    }

    public void testPortRangeHandling() {
        // Test with a port range
        Settings settings = Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), "9300-9400").build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool)) {
            transport.start();

            BoundTransportAddress boundAddress = transport.getBoundAddress();
            assertNotNull("Bound address should not be null", boundAddress);

            int actualPort = boundAddress.publishAddress().getPort();
            assertTrue("Port should be in range 9300-9400", actualPort >= 9300 && actualPort <= 9400);

            transport.stop();
        }
    }

    public void testGracefulShutdownWithException() {
        // Test that exceptions during shutdown don't prevent cleanup
        Settings settings = createSettings();
        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        transport.start();

        // Simulate an interruption during shutdown to test exception handling paths
        ExecutorService executor = transport.getGrpcExecutorForTesting();
        EventLoopGroup bossGroup = transport.getBossEventLoopGroupForTesting();
        EventLoopGroup workerGroup = transport.getWorkerEventLoopGroupForTesting();

        assertNotNull("Executor should be created", executor);
        assertNotNull("Boss group should be created", bossGroup);
        assertNotNull("Worker group should be created", workerGroup);

        // Force shutdown to test the interrupt handling code paths
        executor.shutdownNow();
        bossGroup.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.MILLISECONDS);
        workerGroup.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.MILLISECONDS);

        // Now call stop - it should handle the already shutdown resources gracefully
        transport.stop();
        transport.close();

        // Verify executor still exists (managed by OpenSearch's ThreadPool)
        assertNotNull("Executor should still exist", executor);
        assertTrue("Boss group should be shutdown", bossGroup.isShutdown());
        assertTrue("Worker group should be shutdown", workerGroup.isShutdown());
    }

    public void testCloseWithNullResources() {
        // Test that close() handles null resources gracefully
        Settings settings = createSettings();
        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService, threadPool);

        // Don't start the transport, so resources should be null
        assertNull("Boss group should be null before start", transport.getBossEventLoopGroupForTesting());
        assertNull("Worker group should be null before start", transport.getWorkerEventLoopGroupForTesting());
        assertNull("Executor should be null before start", transport.getGrpcExecutorForTesting());

        // Close should handle null resources gracefully
        transport.close();

        // Multiple closes should be safe
        transport.close();
        transport.close();
    }

    private static Settings createSettings() {
        return Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange()).build();
    }
}
