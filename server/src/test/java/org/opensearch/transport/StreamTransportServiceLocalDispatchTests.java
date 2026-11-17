/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.nio.MockStreamNioTransport;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Exercises the local (in-process) dispatch path of {@link StreamTransportService} end-to-end on a real
 * started service: {@code getConnection(localNode)} returning a {@code LocalStreamConnection}, the async
 * fork in {@code sendLocalStreamRequest}, and {@code deliverLocalStream}/{@code deliverLocalStreamException}
 * feeding the caller's {@link TransportResponseHandler#handleStreamResponse}. Complements
 * {@code LocalStreamChannelTests}, which unit-tests the channel's queue mechanics in isolation.
 */
public class StreamTransportServiceLocalDispatchTests extends OpenSearchSingleNodeTestCase {

    private static final AtomicInteger ACTION_COUNTER = new AtomicInteger(0);

    private String nextAction() {
        return "internal:test/local-stream-" + ACTION_COUNTER.incrementAndGet();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(MockStreamTransportPlugin.class);
    }

    /** Supplies a streaming-capable FLIGHT transport so the node builds a real StreamTransportService. */
    public static class MockStreamTransportPlugin extends Plugin implements NetworkPlugin {
        @Override
        public Map<String, Supplier<Transport>> getTransports(
            Settings settings,
            ThreadPool threadPool,
            PageCacheRecycler pageCacheRecycler,
            CircuitBreakerService circuitBreakerService,
            NamedWriteableRegistry namedWriteableRegistry,
            NetworkService networkService,
            Tracer tracer
        ) {
            return Collections.singletonMap(
                "FLIGHT",
                () -> new MockStreamingTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    networkService,
                    pageCacheRecycler,
                    namedWriteableRegistry,
                    circuitBreakerService,
                    tracer
                )
            );
        }
    }

    private static class MockStreamingTransport extends MockStreamNioTransport {
        MockStreamingTransport(
            Settings settings,
            Version version,
            ThreadPool threadPool,
            NetworkService networkService,
            PageCacheRecycler pageCacheRecycler,
            NamedWriteableRegistry namedWriteableRegistry,
            CircuitBreakerService circuitBreakerService,
            Tracer tracer
        ) {
            super(settings, version, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, tracer);
        }

        @Override
        protected MockSocketChannel initiateChannel(DiscoveryNode node) throws IOException {
            InetSocketAddress address = node.getStreamAddress().address();
            return nioGroup.openChannel(address, clientChannelFactory);
        }
    }

    /** Minimal streaming request. */
    private static class IntRequest extends TransportRequest {
        final int count;

        IntRequest(int count) {
            this.count = count;
        }

        IntRequest(StreamInput in) throws IOException {
            super(in);
            this.count = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(count);
        }
    }

    /** Minimal streaming response carrying an int so batch order is checkable. */
    private static class IntResponse extends TransportResponse {
        final int value;

        IntResponse(int value) {
            this.value = value;
        }

        IntResponse(StreamInput in) throws IOException {
            this.value = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(value);
        }
    }

    private StreamTransportService streamService() {
        return node().injector().getInstance(StreamTransportService.class);
    }

    /**
     * Registers a handler that streams {@code request.count} batches then completes, and drives a
     * local child request against the local node. Verifies the batches arrive in order through the
     * in-process path (no serialization) and the stream terminates cleanly.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLocalDispatchStreamsBatchesInOrder() throws Exception {
        String action = nextAction();
        StreamTransportService service = streamService();
        service.registerRequestHandler(action, ThreadPool.Names.SAME, IntRequest::new, (request, channel, task) -> {
            for (int i = 0; i < request.count; i++) {
                channel.sendResponseBatch(new IntResponse(i));
            }
            channel.completeStream();
        });

        DiscoveryNode localNode = service.getLocalNode();
        List<Integer> received = new ArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        Transport.Connection connection = service.getConnection(localNode);
        assertEquals(localNode.getId(), connection.getNode().getId());

        service.sendChildRequest(
            connection,
            action,
            new IntRequest(5),
            new Task(1, "test", action, "", null, Collections.emptyMap()),
            streamHandler(received, failure, done)
        );

        assertTrue("stream should complete", done.await(30, TimeUnit.SECONDS));
        assertNull("no failure expected: " + failure.get(), failure.get());
        assertEquals(List.of(0, 1, 2, 3, 4), received);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLocalDispatchOnGenericExecutor() throws Exception {
        String action = nextAction();
        StreamTransportService service = streamService();
        service.registerRequestHandler(action, ThreadPool.Names.GENERIC, IntRequest::new, (request, channel, task) -> {
            for (int i = 0; i < request.count; i++) {
                channel.sendResponseBatch(new IntResponse(i));
            }
            channel.completeStream();
        });

        List<Integer> received = new ArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        DiscoveryNode localNode = service.getLocalNode();
        service.sendChildRequest(
            service.getConnection(localNode),
            action,
            new IntRequest(3),
            new Task(1, "test", action, "", null, Collections.emptyMap()),
            streamHandler(received, failure, done)
        );

        assertTrue("stream should complete", done.await(30, TimeUnit.SECONDS));
        assertNull("no failure expected: " + failure.get(), failure.get());
        assertEquals(List.of(0, 1, 2), received);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLocalDispatchProducerFailurePropagates() throws Exception {
        String action = nextAction();
        StreamTransportService service = streamService();
        service.registerRequestHandler(action, ThreadPool.Names.SAME, IntRequest::new, (request, channel, task) -> {
            channel.sendResponseBatch(new IntResponse(0));
            channel.sendResponse(new IllegalStateException("boom"));
        });

        List<Integer> received = new ArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        DiscoveryNode localNode = service.getLocalNode();
        service.sendChildRequest(
            service.getConnection(localNode),
            action,
            new IntRequest(1),
            new Task(1, "test", action, "", null, Collections.emptyMap()),
            streamHandler(received, failure, done)
        );

        assertTrue("stream should terminate", done.await(30, TimeUnit.SECONDS));
        assertNotNull("producer failure must surface to the consumer", failure.get());
    }

    /** Dispatching an unregistered action locally must fail the handler, not hang. */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLocalDispatchUnknownActionFailsHandler() throws Exception {
        StreamTransportService service = streamService();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        DiscoveryNode localNode = service.getLocalNode();
        service.sendChildRequest(
            service.getConnection(localNode),
            "internal:test/does-not-exist",
            new IntRequest(1),
            new Task(1, "test", "internal:test/does-not-exist", "", null, Collections.emptyMap()),
            streamHandler(new ArrayList<>(), failure, done)
        );

        assertTrue("handler should be notified", done.await(30, TimeUnit.SECONDS));
        assertNotNull("unknown action must produce a failure", failure.get());
    }

    /**
     * A streaming response handler that drains all batches into {@code received}, records any failure,
     * and counts down {@code done} on either terminal.
     */
    private TransportResponseHandler<IntResponse> streamHandler(
        List<Integer> received,
        AtomicReference<Exception> failure,
        CountDownLatch done
    ) {
        return new TransportResponseHandler<>() {
            @Override
            public IntResponse read(StreamInput in) throws IOException {
                return new IntResponse(in);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleStreamResponse(StreamTransportResponse<IntResponse> stream) {
                try {
                    IntResponse batch;
                    while ((batch = stream.nextResponse()) != null) {
                        received.add(batch.value);
                    }
                    stream.close();
                } catch (Exception e) {
                    failure.set(e);
                    stream.cancel("consumer failed", e);
                } finally {
                    done.countDown();
                }
            }

            @Override
            public void handleResponse(IntResponse response) {
                done.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                failure.set(exp);
                done.countDown();
            }
        };
    }
}
