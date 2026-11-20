/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

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

    public void testEachNodeUsesAllOfItsConnections() {
        // CONNECTIONS peers, visited in a fixed order. With a round-robin position shared by all nodes, every node
        // would get the same channel each time (counter values i, i + CONNECTIONS, ...), so this catches it.
        List<TcpTransport.NodeChannels> connections = new ArrayList<>();
        connections.add((TcpTransport.NodeChannels) streamTransportService.getConnection(remoteNode));
        for (int n = 1; n < CONNECTIONS; n++) {
            TransportAddress transportAddress = new TransportAddress(
                remoteNode.getAddress().address().getAddress(),
                remoteNode.getAddress().getPort() + n
            );
            DiscoveryNode peer = new DiscoveryNode(
                new DiscoveryNode("test-node-id-" + n, transportAddress, Version.CURRENT),
                remoteNode.getStreamAddress()
            );
            streamTransportService.connectToNode(peer);
            connections.add((TcpTransport.NodeChannels) streamTransportService.getConnection(peer));
        }

        List<Set<TcpChannel>> picked = new ArrayList<>();
        for (int n = 0; n < CONNECTIONS; n++) {
            picked.add(Collections.newSetFromMap(new IdentityHashMap<>()));
        }
        for (int round = 0; round < CONNECTIONS; round++) {
            for (int n = 0; n < CONNECTIONS; n++) {
                picked.get(n).add(connections.get(n).channel(TransportRequestOptions.Type.STREAM));
            }
        }
        for (Set<TcpChannel> channelsOfNode : picked) {
            assertEquals(CONNECTIONS, channelsOfNode.size());
        }
    }

    public void testClosingOneChannelClosesTheConnection() throws Exception {
        TcpTransport.NodeChannels connection = (TcpTransport.NodeChannels) streamTransportService.getConnection(remoteNode);
        List<TcpChannel> channels = connection.getChannels();
        channels.get(1).close();
        assertBusy(() -> assertTrue(connection.isClosed()));
        for (TcpChannel channel : channels) {
            assertFalse(channel.isOpen());
        }
    }

    public void testFailedOpenClosesTheChannelsAlreadyOpened() throws Exception {
        FlightTransport transport = spy(flightTransport);
        AtomicReference<TcpChannel> opened = new AtomicReference<>();
        doAnswer(invocation -> {
            TcpChannel channel = (TcpChannel) invocation.callRealMethod();
            opened.set(channel);
            return channel;
        }).doThrow(new IOException("injected")).when(transport).initiateChannel(any());

        PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
        transport.openConnection(remoteNode, streamProfile(CONNECTIONS), future);

        expectThrows(ConnectTransportException.class, future::actionGet);
        assertNotNull(opened.get());
        assertFalse(opened.get().isOpen());
    }

    public void testRejectsProfilesWithNonStreamConnections() {
        PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
        flightTransport.openConnection(remoteNode, ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY), future);
        expectThrows(IllegalArgumentException.class, future::actionGet);
    }

    public void testSettingBounds() {
        Setting<Integer> setting = StreamTransportService.STREAM_TRANSPORT_CONNECTIONS_PER_NODE_SETTING;
        assertEquals(1, (int) setting.get(Settings.EMPTY));
        expectThrows(IllegalArgumentException.class, () -> setting.get(Settings.builder().put(setting.getKey(), 0).build()));
        expectThrows(IllegalArgumentException.class, () -> setting.get(Settings.builder().put(setting.getKey(), 17).build()));
    }

    private static ConnectionProfile streamProfile(int numConnections) {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(numConnections, TransportRequestOptions.Type.STREAM);
        EnumSet<TransportRequestOptions.Type> others = EnumSet.complementOf(EnumSet.of(TransportRequestOptions.Type.STREAM));
        builder.addConnections(0, others.toArray(new TransportRequestOptions.Type[0]));
        return builder.build();
    }
}
