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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class Netty4GrpcServerTransportTests extends OpenSearchTestCase {

    private NetworkService networkService;
    private List<BindableService> services;

    @Before
    public void setup() {
        networkService = new NetworkService(List.of());
        services = List.of();
    }

    public void testBasicStartAndStop() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.getBoundAddress().publishAddress().address());

            transport.stop();
        }
    }

    public void testGrpcTransportHealthcheck() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService)) {
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
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.getBoundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.getBoundAddress().publishAddress().address());

            // Verify executor is created and is a ForkJoinPool
            ExecutorService executor = transport.getGrpcExecutorForTesting();
            assertNotNull("gRPC executor should be created", executor);
            assertTrue("Executor should be a ForkJoinPool", executor instanceof ForkJoinPool);

            ForkJoinPool forkJoinPool = (ForkJoinPool) executor;
            assertEquals("Executor should have 8 threads", 8, forkJoinPool.getParallelism());

            transport.stop();
        }
    }

    public void testDefaultExecutorCount() {
        // Test default executor count (should be 2x allocated processors)
        Settings settings = createSettings();
        int expectedExecutorCount = OpenSearchExecutors.allocatedProcessors(settings) * 2;

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            ExecutorService executor = transport.getGrpcExecutorForTesting();
            assertNotNull("gRPC executor should be created", executor);
            assertTrue("Executor should be a ForkJoinPool", executor instanceof ForkJoinPool);

            ForkJoinPool forkJoinPool = (ForkJoinPool) executor;
            assertEquals("Default executor count should be 2x allocated processors", expectedExecutorCount, forkJoinPool.getParallelism());

            transport.stop();
        }
    }

    public void testSeparateEventLoopGroups() {
        // Test that boss and worker event loop groups are separate
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT.getKey(), 4)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
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

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService);
        transport.start();

        ExecutorService executor = transport.getGrpcExecutorForTesting();
        assertNotNull("Executor should be created", executor);
        assertFalse("Executor should not be shutdown initially", executor.isShutdown());

        transport.stop();
        assertTrue("Executor should be shutdown after transport stop", executor.isShutdown());

        transport.close();
    }

    public void testEventLoopGroupsShutdownOnStop() {
        // Test that event loop groups are properly shutdown when transport stops
        Settings settings = createSettings();

        Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService);
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

        expectThrows(IllegalArgumentException.class, () -> { new Netty4GrpcServerTransport(invalidSettings, services, networkService); });
    }

    public void testExecutorCountSettingsValidation() {
        // Test that invalid executor count settings are handled properly
        Settings invalidSettings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_EXECUTOR_COUNT.getKey(), 0) // Invalid: should be >= 1
            .build();

        expectThrows(IllegalArgumentException.class, () -> { new Netty4GrpcServerTransport(invalidSettings, services, networkService); });
    }

    private static Settings createSettings() {
        return Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange()).build();
    }
}
