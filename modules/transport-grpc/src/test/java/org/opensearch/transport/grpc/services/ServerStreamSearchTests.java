/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.search.SearchHits;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;
import org.opensearch.transport.grpc.proto.request.search.aggregation.AggregationBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.StreamPayload;
import org.opensearch.transport.grpc.spi.StreamSearchFormatHandler;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerMethodDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises {@code ServerStreamSearch} through the production gRPC server and a real Netty client.
 */
public class ServerStreamSearchTests extends OpenSearchTestCase {

    private static final String FORMAT = "test-format";

    private static final MethodDescriptor<org.opensearch.protobufs.SearchRequest, byte[]> RAW_STREAM_METHOD = SearchServiceGrpc
        .getServerStreamSearchMethod()
        .toBuilder(SearchServiceGrpc.getServerStreamSearchMethod().getRequestMarshaller(), new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] parse(InputStream stream) {
                try {
                    return stream.readAllBytes();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        })
        .build();

    private ThreadPool threadPool;
    private CircuitBreaker breaker;
    private Netty4GrpcServerTransport transport;
    private NettyGrpcClient client;
    private volatile BiConsumer<org.opensearch.action.search.SearchRequest, StreamSearchResponseChannel> behaviour;

    @Before
    public void startServer() throws Exception {
        Settings settings = Settings.builder().put("node.name", "test-node").build();
        ExecutorBuilder<?> grpcExecutor = new FixedExecutorBuilder(settings, "grpc", 4, 1000, "thread_pool.grpc");
        threadPool = new ThreadPool(settings, grpcExecutor);
        breaker = mock(CircuitBreaker.class);
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS)).thenReturn(breaker);

        StreamSearchFormatHandler handler = new StreamSearchFormatHandler() {
            @Override
            public String format() {
                return FORMAT;
            }

            @Override
            public void execute(org.opensearch.action.search.SearchRequest request, StreamSearchResponseChannel channel) {
                behaviour.accept(request, channel);
            }
        };
        SearchServiceImpl service = new SearchServiceImpl(
            mock(NodeClient.class),
            QueryBuilderProtoTestUtils.createQueryUtils(),
            new AggregationBuilderProtoConverterRegistryImpl(),
            new AggregateProtoConverterRegistryImpl(),
            breakerService,
            Map.of(FORMAT, handler)
        );
        transport = new Netty4GrpcServerTransport(
            Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), getPortRange()).build(),
            List.of(service.withServerStreamSearch()),
            new NetworkService(List.of()),
            threadPool
        );
        transport.start();
        TransportAddress address = randomFrom(transport.getBoundAddress().boundAddresses());
        client = new NettyGrpcClient.Builder().setAddress(address).build();
    }

    @After
    public void stopServer() throws Exception {
        client.close();
        transport.stop();
        transport.close();
        terminate(threadPool);
    }

    private static org.opensearch.protobufs.SearchRequest request() {
        return org.opensearch.protobufs.SearchRequest.newBuilder()
            .addIndex("logs")
            .setSearchRequestBody(SearchRequestBody.newBuilder().setSize(5).build())
            .build();
    }

    private Iterator<byte[]> call(String format) {
        Metadata headers = new Metadata();
        if (format != null) {
            headers.put(Metadata.Key.of(StreamSearchFormatHandler.FORMAT_HEADER, Metadata.ASCII_STRING_MARSHALLER), format);
        }
        return ClientCalls.blockingServerStreamingCall(
            ClientInterceptors.intercept(client.getChannel(), MetadataUtils.newAttachHeadersInterceptor(headers)),
            RAW_STREAM_METHOD,
            CallOptions.DEFAULT,
            request()
        );
    }

    private static List<byte[]> drain(Iterator<byte[]> messages) {
        List<byte[]> all = new ArrayList<>();
        messages.forEachRemaining(all::add);
        return all;
    }

    private static byte[] payloadOf(byte[] message) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(message);
        int tag = in.readTag();
        assertEquals(StreamSearchResponseChannel.PAYLOAD_FIELD_NUMBER, WireFormat.getTagFieldNumber(tag));
        byte[] payload = in.readByteArray();
        assertTrue(in.isAtEnd());
        return payload;
    }

    private static SearchResponse summary(long took) {
        return new SearchResponse(
            new SearchResponseSections(SearchHits.empty(), null, null, false, false, null, 1),
            null,
            2,
            2,
            0,
            took,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public void testStreamsPayloadsWithoutCopyingThenSummary() throws Exception {
        int payloadCount = randomIntBetween(1, 5);
        List<byte[]> expected = new ArrayList<>();
        List<String> releaseThreads = new CopyOnWriteArrayList<>();
        CountDownLatch released = new CountDownLatch(payloadCount);
        AtomicReference<String> handlerThread = new AtomicReference<>();
        AtomicReference<String[]> indices = new AtomicReference<>();
        for (int i = 0; i < payloadCount; i++) {
            expected.add(randomByteArrayOfLength(randomIntBetween(1, 64 * 1024)));
        }
        behaviour = (request, channel) -> {
            handlerThread.set(Thread.currentThread().getName());
            indices.set(request.indices());
            for (byte[] bytes : expected) {
                int split = bytes.length / 2;
                ByteBuffer head = ByteBuffer.allocateDirect(split).put(bytes, 0, split).flip();
                ByteBuffer tail = ByteBuffer.allocateDirect(bytes.length - split).put(bytes, split, bytes.length - split).flip();
                channel.sendPayload(new StreamPayload(List.of(head, tail), () -> {
                    releaseThreads.add(Thread.currentThread().getName());
                    released.countDown();
                }));
            }
            channel.complete(summary(7));
        };

        List<byte[]> messages = drain(call(FORMAT));

        assertArrayEquals(new String[] { "logs" }, indices.get());
        assertEquals(payloadCount + 1, messages.size());
        for (int i = 0; i < payloadCount; i++) {
            assertArrayEquals(expected.get(i), payloadOf(messages.get(i)));
        }
        org.opensearch.protobufs.SearchResponse last = org.opensearch.protobufs.SearchResponse.parseFrom(messages.get(payloadCount));
        assertEquals(7, last.getTook());
        assertEquals(2, last.getXShards().getSuccessful());

        assertTrue(released.await(10, TimeUnit.SECONDS));
        for (String thread : releaseThreads) {
            assertNotEquals("a payload released on the sending thread was copied", handlerThread.get(), thread);
        }
        verify(breaker, timeout(10_000)).addWithoutBreaking(-request().getSerializedSize());
    }

    public void testMissingFormatHeaderIsUnimplemented() {
        behaviour = (request, channel) -> fail("handler must not run");
        StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> drain(call(null)));
        assertEquals(Status.Code.UNIMPLEMENTED, e.getStatus().getCode());
        assertTrue(e.getStatus().getDescription().contains(StreamSearchFormatHandler.FORMAT_HEADER));
    }

    public void testUnknownFormatIsInvalidArgument() {
        behaviour = (request, channel) -> fail("handler must not run");
        StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> drain(call("csv")));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertTrue(e.getStatus().getDescription().contains(FORMAT));
    }

    public void testHandlerFailureClosesCallAndReleasesBreaker() {
        behaviour = (request, channel) -> { throw new IllegalArgumentException("unsupported search option"); };
        StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> drain(call(FORMAT)));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        verify(breaker, timeout(10_000)).addWithoutBreaking(-request().getSerializedSize());
    }

    public void testCircuitBreakerRejectsBeforeHandler() {
        doThrow(new CircuitBreakingException("too many bytes", CircuitBreaker.Durability.TRANSIENT)).when(breaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());
        behaviour = (request, channel) -> fail("handler must not run");
        StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> drain(call(FORMAT)));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, e.getStatus().getCode());
        verify(breaker, never()).addWithoutBreaking(anyLong());
    }

    public void testClientCancellationReachesHandler() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicReference<StreamSearchResponseChannel> channelRef = new AtomicReference<>();
        behaviour = (request, channel) -> {
            channelRef.set(channel);
            channel.setOnCancelHandler(cancelled::countDown);
            started.countDown();
        };
        ClientCall<org.opensearch.protobufs.SearchRequest, byte[]> clientCall = ClientInterceptors.intercept(
            client.getChannel(),
            MetadataUtils.newAttachHeadersInterceptor(formatHeaders())
        ).newCall(RAW_STREAM_METHOD, CallOptions.DEFAULT);
        clientCall.start(new ClientCall.Listener<>() {
        }, new Metadata());
        clientCall.sendMessage(request());
        clientCall.halfClose();
        clientCall.request(1);
        assertTrue(started.await(10, TimeUnit.SECONDS));

        clientCall.cancel("client gave up", null);

        assertTrue(cancelled.await(10, TimeUnit.SECONDS));
        assertTrue(channelRef.get().isCancelled());
        assertFalse(channelRef.get().isReady());
        verify(breaker, timeout(10_000)).addWithoutBreaking(-request().getSerializedSize());
    }

    public void testResetReleasesWhatAClientThatStoppedReadingHolds() throws Exception {
        CountDownLatch ended = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicReference<StreamSearchResponseChannel> channelRef = new AtomicReference<>();
        // Far more than the client's flow-control window, so most of it waits in the server for the client to read it.
        ByteBuffer large = ByteBuffer.allocateDirect(1 << 20);
        behaviour = (request, channel) -> {
            channelRef.set(channel);
            channel.sendPayload(new StreamPayload(List.of(large), released::countDown));
            channel.fail(new IllegalStateException("the client stopped reading"));
            ended.countDown();
        };
        TransportAddress address = randomFrom(transport.getBoundAddress().boundAddresses());
        ManagedChannel stuck = NettyChannelBuilder.forAddress(address.getAddress(), address.getPort())
            .usePlaintext()
            .flowControlWindow(16 * 1024)
            .build();
        try {
            ClientCall<org.opensearch.protobufs.SearchRequest, byte[]> clientCall = ClientInterceptors.intercept(
                stuck,
                MetadataUtils.newAttachHeadersInterceptor(formatHeaders())
            ).newCall(RAW_STREAM_METHOD, CallOptions.DEFAULT);
            // The client requests no messages, so it never reads the payload, but it keeps its connection open.
            clientCall.start(new ClientCall.Listener<>() {
            }, new Metadata());
            clientCall.sendMessage(request());
            clientCall.halfClose();
            assertTrue(ended.await(10, TimeUnit.SECONDS));
            assertEquals("a failed call keeps what its client did not read", 1, released.getCount());

            channelRef.get().reset();

            assertTrue("the reset released what the client did not read", released.await(10, TimeUnit.SECONDS));
        } finally {
            stuck.shutdownNow();
            stuck.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testReportsOnceThatItCannotResetCallsAnInterceptorWrapped() throws Exception {
        SearchServiceImpl service = new SearchServiceImpl(
            mock(NodeClient.class),
            QueryBuilderProtoTestUtils.createQueryUtils(),
            new AggregationBuilderProtoConverterRegistryImpl(),
            new AggregateProtoConverterRegistryImpl(),
            mock(CircuitBreakerService.class),
            Map.of(FORMAT, mock(StreamSearchFormatHandler.class))
        );
        ServerMethodDefinition<?, ?> method = service.withServerStreamSearch()
            .bindService()
            .getMethod(SearchServiceGrpc.getServerStreamSearchMethod().getFullMethodName());
        String loggerName = SearchServiceImpl.class.getCanonicalName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SearchServiceImpl.class))) {
            appender.addExpectation(new MockLogAppender.SeenEventExpectation("wrapped", loggerName, Level.WARN, "cannot reset*"));
            startWrappedCall(method);
            appender.assertAllExpectationsMatched();
        }
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SearchServiceImpl.class))) {
            appender.addExpectation(new MockLogAppender.UnseenEventExpectation("wrapped again", loggerName, Level.WARN, "*"));
            startWrappedCall(method);
            appender.assertAllExpectationsMatched();
        }
    }

    /** Starts a call the way an interceptor that wraps gRPC's own call passes it on. */
    private static <ReqT, RespT> void startWrappedCall(ServerMethodDefinition<ReqT, RespT> method) {
        @SuppressWarnings("unchecked")
        ServerCall<ReqT, RespT> grpcCall = mock(ServerCall.class);
        when(grpcCall.getMethodDescriptor()).thenReturn(method.getMethodDescriptor());
        method.getServerCallHandler().startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(grpcCall) {
        }, new Metadata());
    }

    private static Metadata formatHeaders() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(StreamSearchFormatHandler.FORMAT_HEADER, Metadata.ASCII_STRING_MARSHALLER), FORMAT);
        return headers;
    }
}
