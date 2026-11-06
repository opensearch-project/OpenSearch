/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

/**
 * With {@code transport.stream.connections_per_node} above 1, the Flight transport opens one independent
 * {@link FlightClientChannel} (its own gRPC channel and TCP connection) per connection, and stream requests
 * are spread across them.
 */
public class FlightTransportConnectionsPerNodeTests extends FlightTransportTestBase {

    private static final int CONNECTIONS = 3;

    @Override
    protected Settings additionalSettings() {
        return Settings.builder().put(StreamTransportService.STREAM_TRANSPORT_CONNECTIONS_PER_NODE_SETTING.getKey(), CONNECTIONS).build();
    }

    public void testOpensOneFlightChannelPerConnection() {
        Transport.Connection connection = streamTransportService.getConnection(remoteNode);
        assertThat(connection, instanceOf(TcpTransport.NodeChannels.class));
        List<TcpChannel> channels = ((TcpTransport.NodeChannels) connection).getChannels();
        assertEquals(CONNECTIONS, channels.size());

        Set<TcpChannel> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        for (TcpChannel channel : channels) {
            assertThat(channel, instanceOf(FlightClientChannel.class));
            assertTrue(channel.isOpen());
            distinct.add(channel);
        }
        assertEquals(CONNECTIONS, distinct.size());

        // Consecutive STREAM requests are spread round-robin over all connections.
        Set<TcpChannel> picked = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < CONNECTIONS; i++) {
            picked.add(((TcpTransport.NodeChannels) connection).channel(TransportRequestOptions.Type.STREAM));
        }
        assertEquals(CONNECTIONS, picked.size());
    }

    public void testStreamsSucceedOnEveryConnection() throws Exception {
        String action = "internal:test/connections-per-node";
        streamTransportService.registerRequestHandler(action, ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            channel.sendResponseBatch(new TestResponse("a"));
            channel.sendResponseBatch(new TestResponse("b"));
            channel.completeStream();
        });

        // Two full rounds, so every connection carries at least two streams.
        int requests = CONNECTIONS * 2;
        CountDownLatch done = new CountDownLatch(requests);
        AtomicInteger batches = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (int i = 0; i < requests; i++) {
            streamTransportService.sendRequest(
                remoteNode,
                action,
                new TestRequest(),
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new StreamTransportResponseHandler<TestResponse>() {
                    @Override
                    public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                        try {
                            while (streamResponse.nextResponse() != null) {
                                batches.incrementAndGet();
                            }
                        } catch (Exception e) {
                            failure.set(e);
                        } finally {
                            try {
                                streamResponse.close();
                            } catch (Exception ignored) {}
                            done.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        failure.set(exp);
                        done.countDown();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public TestResponse read(StreamInput in) throws IOException {
                        return new TestResponse(in);
                    }
                }
            );
        }

        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(requests * 2, batches.get());
    }

    public void testConnectionCloseClosesEveryChannel() throws Exception {
        TcpTransport.NodeChannels connection = (TcpTransport.NodeChannels) streamTransportService.getConnection(remoteNode);
        List<TcpChannel> channels = connection.getChannels();
        connection.close();
        for (TcpChannel channel : channels) {
            assertBusy(() -> assertFalse(channel.isOpen()));
        }
    }
}
