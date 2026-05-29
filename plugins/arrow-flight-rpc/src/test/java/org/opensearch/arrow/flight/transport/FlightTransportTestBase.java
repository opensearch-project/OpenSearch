/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.TaskManager;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class FlightTransportTestBase extends OpenSearchTestCase {

    protected DiscoveryNode remoteNode;
    protected Location serverLocation;
    protected HeaderContext headerContext;
    protected ThreadPool threadPool;
    protected NamedWriteableRegistry namedWriteableRegistry;
    protected FlightStatsCollector statsCollector;
    protected BoundTransportAddress boundAddress;
    protected FlightTransport flightTransport;
    protected StreamTransportService streamTransportService;
    protected ArrowNativeAllocator nativeAllocator;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Configure FlightTransport with a port range rather than a single hardcoded port. The
        // transport iterates the range via PortsRange.iterate() and binds the first available
        // port. Using a single port made tests fail with BindTransportException whenever the
        // chosen port was held in TIME_WAIT or contended by another process on the CI runner.
        // The actual bound port is read from FlightTransport.boundAddress() after start() below.
        int basePort = getBasePort(9500);
        String portRange = basePort + "-" + (basePort + 99);

        Settings settings = Settings.builder()
            .put("node.name", getTestName())
            .put("aux.transport.transport-flight.port", portRange)
            .build();
        ServerConfig.init(settings);
        threadPool = new ThreadPool(
            settings,
            ServerConfig.getClientExecutorBuilder(),
            ServerConfig.getGrpcExecutorBuilder(),
            ServerConfig.getServerExecutorBuilder()
        );
        namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        statsCollector = new FlightStatsCollector();
        headerContext = new HeaderContext();

        // FlightTransport sources its allocator from the framework's FLIGHT pool. Construct one
        // here so the test has a usable allocator; tearDown closes it.
        nativeAllocator = new ArrowNativeAllocator(Long.MAX_VALUE);
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_FLIGHT, 0L, Long.MAX_VALUE);

        flightTransport = new FlightTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new PageCacheRecycler(settings),
            new NoneCircuitBreakerService(),
            namedWriteableRegistry,
            new NetworkService(Collections.emptyList()),
            mock(Tracer.class),
            null,
            statsCollector,
            nativeAllocator
        );
        flightTransport.start();

        // Resolve the actual bound stream port (chosen by PortsRange.iterate()) and derive the
        // remote node's transport/stream addresses from it. The transport address is never
        // bound by this test, so any distinct port suffices.
        int streamPort = flightTransport.boundAddress().publishAddress().address().getPort();
        TransportAddress streamAddress = new TransportAddress(InetAddress.getLoopbackAddress(), streamPort);
        TransportAddress transportAddress = new TransportAddress(InetAddress.getLoopbackAddress(), streamPort + 1);
        remoteNode = new DiscoveryNode(new DiscoveryNode("test-node-id", transportAddress, Version.CURRENT), streamAddress);
        boundAddress = new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress);
        serverLocation = Location.forGrpcInsecure("localhost", streamPort);

        TransportService transportService = mock(TransportService.class);
        TaskManager taskManager = mock(TaskManager.class);
        when(taskManager.taskExecutionStarted(any())).thenReturn(mock(ThreadContext.StoredContext.class));
        when(transportService.getTaskManager()).thenReturn(taskManager);
        streamTransportService = spy(
            new StreamTransportService(
                settings,
                flightTransport,
                threadPool,
                StreamTransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> remoteNode,
                null,
                transportService.getTaskManager(),
                null,
                mock(Tracer.class)
            )
        );
        streamTransportService.connectToNode(remoteNode);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (streamTransportService != null) {
            streamTransportService.close();
        }
        if (flightTransport != null) {
            flightTransport.close();
        }
        if (threadPool != null) {
            threadPool.shutdown();
        }
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    protected FlightClientChannel createChannel(FlightClient flightClient) {
        return createChannel(flightClient, threadPool, flightTransport.getResponseHandlers());
    }

    protected FlightClientChannel createChannel(FlightClient flightClient, ThreadPool threadPool) {
        return createChannel(flightClient, threadPool, flightTransport.getResponseHandlers());
    }

    protected FlightClientChannel createChannel(
        FlightClient flightClient,
        ThreadPool customThreadPool,
        Transport.ResponseHandlers handlers
    ) {
        return new FlightClientChannel(
            boundAddress,
            flightClient,
            remoteNode,
            serverLocation,
            headerContext,
            "test-profile",
            handlers,
            customThreadPool,
            new TransportMessageListener() {
            },
            namedWriteableRegistry,
            statsCollector,
            new FlightTransportConfig()
        );
    }

    protected static class TestRequest extends TransportRequest {
        public TestRequest() {}

        public TestRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    protected static class TestResponse extends TransportResponse {
        private final String data;

        public TestResponse(String data) {
            this.data = data;
        }

        public TestResponse(StreamInput in) throws IOException {
            super(in);
            this.data = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(data);
        }

        public String getData() {
            return data;
        }
    }
}
