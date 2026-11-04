/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.grpc.InternalChannelz;

import static org.mockito.Mockito.mock;

/**
 * Verifies that {@link FlightTransport#bindToPort} does not leak gRPC {@code ServerImpl} instances
 * in {@link InternalChannelz} when a port bind attempt fails and the transport falls through to the
 * next port in the range.
 * <p>
 * The gRPC {@code ServerImpl} constructor registers itself in the process-wide
 * {@code InternalChannelz} static registry. If a built-but-not-started (or failed-to-start) server
 * is never shut down, it stays in that registry forever, pinning the entire handler chain
 * (FlightService → ArrowFlightProducer → FlightTransport → the OpenSearch node) in memory.
 */
public class FlightTransportBindLeakTests extends OpenSearchTestCase {

    /** Collect the ids of all servers currently registered in the process-wide InternalChannelz. */
    private static Set<Long> channelzServerIds() {
        Set<Long> ids = new HashSet<>();
        long fromId = 0;
        while (true) {
            // Fetch in small batches to avoid an oversized ArrayList allocation
            // that can OOM when many servers are leaked in a test JVM.
            InternalChannelz.ServerList batch = InternalChannelz.instance().getServers(fromId, 100);
            for (int i = 0; i < batch.servers.size(); i++) {
                ids.add(InternalChannelz.id(batch.servers.get(i)));
            }
            if (batch.end || batch.servers.isEmpty()) {
                break;
            }
            // The next page starts after the last id in this batch.
            fromId = InternalChannelz.id(batch.servers.get(batch.servers.size() - 1)) + 1;
        }
        return ids;
    }

    /**
     * Find two consecutive free ports on loopback. Binds port N and N+1 simultaneously to confirm
     * both are available, then releases them.
     * The release-then-rebind window is an accepted race mitigated by the retry loop.
     */
    private static int findConsecutiveFreePorts() throws Exception {
        for (int attempt = 0; attempt < 20; attempt++) {
            try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                int port = ss.getLocalPort();
                // Try to also grab port+1
                try (ServerSocket ss2 = new ServerSocket(port + 1, 1, InetAddress.getLoopbackAddress())) {
                    return port;
                } catch (Exception e) {
                    // port+1 not available, retry
                }
            }
        }
        throw new AssertionError("Could not find two consecutive free ports after 20 attempts");
    }

    /**
     * Occupies the first port of a 2-port range so bindToPort must skip it, then asserts that the
     * skipped server is cleaned up from {@link InternalChannelz} after the transport stops.
     */
    public void testFailedBindDoesNotLeakServerInChannelz() throws Exception {
        // Find two consecutive free ports: port1 and port1+1.
        int port1 = findConsecutiveFreePorts();
        int port2 = port1 + 1;

        // Record the channelz server ids before we do anything.
        Set<Long> baselineIds = channelzServerIds();

        // Occupy port1 so the transport's first bind attempt fails.
        try (ServerSocket blocker = new ServerSocket(port1, 1, InetAddress.getLoopbackAddress())) {
            String portRange = port1 + "-" + port2;
            Settings settings = Settings.builder()
                .put("node.name", getTestName())
                .put("aux.transport.transport-flight.port", portRange)
                .build();
            ServerConfig.init(settings);

            ThreadPool threadPool = new ThreadPool(
                settings,
                ServerConfig.getClientExecutorBuilder(),
                ServerConfig.getGrpcExecutorBuilder(),
                ServerConfig.getServerExecutorBuilder()
            );
            ArrowNativeAllocator nativeAllocator = new ArrowNativeAllocator();
            nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_FLIGHT, 0L, Long.MAX_VALUE, null);

            FlightTransport transport = new FlightTransport(
                settings,
                Version.CURRENT,
                threadPool,
                new PageCacheRecycler(settings),
                new NoneCircuitBreakerService(),
                new NamedWriteableRegistry(Collections.emptyList()),
                new NetworkService(Collections.emptyList()),
                mock(Tracer.class),
                null,
                new FlightStatsCollector(),
                nativeAllocator
            );

            try {
                transport.start();

                // The transport should have bound to port2 (port1 was occupied).
                int boundPort = transport.boundAddress().publishAddress().address().getPort();
                assertEquals("Transport should bind to the second port in the range", port2, boundPort);
            } finally {
                transport.close();
                threadPool.shutdown();
                nativeAllocator.close();
            }
        }

        // After closing the transport, all servers (the successful one AND any failed-to-bind ones)
        // must be removed from channelz. Use assertBusy because gRPC server termination is async.
        assertBusy(() -> {
            Set<Long> currentIds = channelzServerIds();
            Set<Long> leaked = new HashSet<>(currentIds);
            leaked.removeAll(baselineIds);
            assertTrue("Leaked gRPC server(s) in InternalChannelz — ids not cleaned up: " + leaked, leaked.isEmpty());
        });
    }
}
