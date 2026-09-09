/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Header;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;
import org.junit.After;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightClientChannelTests extends FlightTransportTestBase {
    private final int TIMEOUT_SEC = 10;
    private FlightClient mockFlightClient;
    private FlightClientChannel channel;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockFlightClient = mock(FlightClient.class);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.close();
        }
        super.tearDown();
    }

    public void testChannelLifecycle() throws InterruptedException {
        channel = createChannel(mockFlightClient);

        assertFalse(channel.isServerChannel());
        assertEquals("test-profile", channel.getProfile());
        assertTrue(channel.isOpen());
        assertNotNull(channel.getChannelStats());

        CountDownLatch connectLatch = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        channel.addConnectListener(ActionListener.wrap(response -> {
            connected.set(true);
            connectLatch.countDown();
        }, exception -> connectLatch.countDown()));
        assertTrue(connectLatch.await(1, TimeUnit.SECONDS));
        assertTrue(connected.get());

        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicBoolean closed = new AtomicBoolean(false);
        channel.addCloseListener(ActionListener.wrap(response -> {
            closed.set(true);
            closeLatch.countDown();
        }, exception -> closeLatch.countDown()));

        channel.close();
        assertTrue(closeLatch.await(1, TimeUnit.SECONDS));
        assertFalse(channel.isOpen());
        assertTrue(closed.get());

        channel.close();
    }

    public void testSendMessageWhenClosed() throws InterruptedException {
        channel = createChannel(mockFlightClient);
        channel.close();

        BytesReference message = new BytesArray("test message");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        channel.sendMessage(-1, message, ActionListener.wrap(response -> latch.countDown(), ex -> {
            exception.set(ex);
            latch.countDown();
        }));

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertNotNull(exception.get());
        assertTrue(exception.get() instanceof TransportException);
        assertEquals("FlightClientChannel is closed", exception.get().getMessage());
    }

    public void testSendMessageNotifiesHandlerWhenCloseRacesRegistration() throws Exception {
        CountDownLatch handlerRemoved = new CountDownLatch(1);
        CountDownLatch continueSend = new CountDownLatch(1);
        AtomicInteger handlerNotifications = new AtomicInteger();
        AtomicReference<TransportException> handlerFailure = new AtomicReference<>();
        AtomicReference<Exception> sendListenerFailure = new AtomicReference<>();
        AtomicReference<Throwable> sendThreadFailure = new AtomicReference<>();

        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<>() {
            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }

            @Override
            public void handleResponse(TestResponse response) {
                fail("stream response should not be dispatched after channel close");
            }

            @Override
            public void handleException(TransportException exp) {
                handlerFailure.set(exp);
                handlerNotifications.incrementAndGet();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        };

        TransportMessageListener blockingListener = new TransportMessageListener() {
            @Override
            @SuppressWarnings("rawtypes")
            public void onResponseReceived(long requestId, Transport.ResponseContext context) {
                handlerRemoved.countDown();
                try {
                    if (continueSend.await(5, TimeUnit.SECONDS) == false) {
                        throw new IllegalStateException("timed out waiting to continue send");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting to continue send", e);
                }
            }
        };

        channel = new FlightClientChannel(
            boundAddress,
            mockFlightClient,
            remoteNode,
            serverLocation,
            headerContext,
            "test-profile",
            flightTransport.getResponseHandlers(),
            threadPool,
            blockingListener,
            namedWriteableRegistry,
            statsCollector,
            new FlightTransportConfig()
        );

        Transport.Connection connection = new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return remoteNode;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
                channel.sendMessage(
                    requestId,
                    new BytesArray("test message"),
                    ActionListener.wrap(response -> {}, sendListenerFailure::set)
                );
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {}

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {}
        };

        Thread sendThread = new Thread(() -> {
            try {
                streamTransportService.sendRequest(
                    connection,
                    "internal:test/channel-close-race",
                    new TestRequest(),
                    TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                    responseHandler
                );
            } catch (Throwable t) {
                sendThreadFailure.set(t);
            }
        }, "flight-send-close-race-test");
        sendThread.start();
        try {
            assertTrue("send must remove its response handler before channel close", handlerRemoved.await(5, TimeUnit.SECONDS));
            channel.close();
        } finally {
            continueSend.countDown();
            sendThread.join(TimeUnit.SECONDS.toMillis(5));
        }

        assertFalse("send thread did not finish", sendThread.isAlive());
        assertNull("send thread failed", sendThreadFailure.get());
        assertEquals("response handler must be notified exactly once", 1, handlerNotifications.get());
        assertNotNull(handlerFailure.get());
        assertTrue(handlerFailure.get() instanceof StreamException);
        assertEquals(StreamErrorCode.UNAVAILABLE, ((StreamException) handlerFailure.get()).getErrorCode());
        assertNotNull(sendListenerFailure.get());
        assertTrue(sendListenerFailure.get() instanceof StreamException);
        assertEquals(StreamErrorCode.UNAVAILABLE, ((StreamException) sendListenerFailure.get()).getErrorCode());
    }

    // ── channel close vs. active streams ───────────────────────────────────────

    /**
     * Close must cancel an active stream and then wait for its consumer — running on another
     * thread — to release it, so the FlightClient's allocator is not closed while buffers are out.
     */
    public void testCloseWaitsForStreamHeldByAnotherThread() throws Exception {
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true);
        when(mockFlightClient.getStream(any(Ticket.class), any())).thenReturn(stream);

        CountDownLatch consumerHoldingStream = new CountDownLatch(1);
        CountDownLatch releaseConsumer = new CountDownLatch(1);
        CountDownLatch consumerDone = new CountDownLatch(1);

        // A long timeout: if the wait ever expires, that is a failure rather than a slow pass.
        FlightTransportConfig config = new FlightTransportConfig();
        config.setStreamCloseTimeout(TimeValue.timeValueSeconds(30));
        channel = createChannel(mockFlightClient, stubHeaderContext(), config);

        sendStreamRequest(streamingHandler(ThreadPool.Names.GENERIC, streamResponse -> {
            consumerHoldingStream.countDown();
            assertTrue("test must release the consumer", releaseConsumer.await(TIMEOUT_SEC, TimeUnit.SECONDS));
            streamResponse.close();
            consumerDone.countDown();
        }));

        assertTrue("consumer must take the stream", consumerHoldingStream.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        Thread closer = new Thread(channel::close, "channel-closer");
        closer.start();
        // Cancellation is issued up front, before the wait, to unblock a parked consumer.
        verify(stream, timeout(TimeUnit.SECONDS.toMillis(TIMEOUT_SEC))).cancel(anyString(), isNull());

        closer.join(500);
        assertTrue("close must wait for the consumer to release the stream", closer.isAlive());

        releaseConsumer.countDown();
        closer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SEC));
        assertFalse("close must return once the stream is released", closer.isAlive());
        assertTrue("consumer must finish", consumerDone.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        verify(stream, times(1)).close();
    }

    /**
     * A close reaching the channel from inside a consumer callback must not wait for that
     * consumer's own stream: it can only be released after the callback returns, which cannot
     * happen until the close returns. Waiting would burn the whole timeout to no effect.
     */
    public void testCloseFromConsumerCallbackDoesNotWaitForItsOwnStream() throws Exception {
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true);
        when(mockFlightClient.getStream(any(Ticket.class), any())).thenReturn(stream);

        final long timeoutMillis = TimeUnit.SECONDS.toMillis(10);
        FlightTransportConfig config = new FlightTransportConfig();
        config.setStreamCloseTimeout(TimeValue.timeValueMillis(timeoutMillis));
        channel = createChannel(mockFlightClient, stubHeaderContext(), config);

        CountDownLatch consumerDone = new CountDownLatch(1);
        AtomicLong closeElapsedMillis = new AtomicLong(-1);

        sendStreamRequest(streamingHandler(ThreadPool.Names.SAME, streamResponse -> {
            long start = System.nanoTime();
            channel.close();
            closeElapsedMillis.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            streamResponse.close();
            consumerDone.countDown();
        }));

        // Generous: a regression here shows up as the elapsed-time assertion below, not as a timeout.
        assertTrue("consumer must finish", consumerDone.await(3 * TIMEOUT_SEC, TimeUnit.SECONDS));
        assertThat(closeElapsedMillis.get(), greaterThanOrEqualTo(0L));
        assertThat(
            "close must skip the stream owned by the calling thread instead of waiting for it",
            closeElapsedMillis.get(),
            lessThan(timeoutMillis / 2)
        );
        verify(stream, times(1)).close();
    }

    /**
     * A stream that never gets released bounds the close at the configured timeout rather than
     * blocking shutdown indefinitely. Here the stream is registered but stuck in getStream(), so
     * there is nothing to cancel and nothing that can report itself released.
     */
    public void testCloseGivesUpAfterStreamCloseTimeout() throws Exception {
        CountDownLatch insideGetStream = new CountDownLatch(1);
        CountDownLatch releaseGetStream = new CountDownLatch(1);
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true);
        when(mockFlightClient.getStream(any(Ticket.class), any())).thenAnswer(inv -> {
            insideGetStream.countDown();
            assertTrue("test must release getStream", releaseGetStream.await(TIMEOUT_SEC, TimeUnit.SECONDS));
            return stream;
        });

        final long timeoutMillis = 300;
        FlightTransportConfig config = new FlightTransportConfig();
        config.setStreamCloseTimeout(TimeValue.timeValueMillis(timeoutMillis));
        channel = createChannel(mockFlightClient, stubHeaderContext(), config);

        try {
            AtomicReference<TransportException> handlerException = new AtomicReference<>();
            // The channel closes while the stream is still opening, so the handler is failed; that
            // is expected here and must not fail the test.
            sendStreamRequest(streamingHandler(ThreadPool.Names.GENERIC, streamResponse -> streamResponse.close(), handlerException));
            assertTrue("stream must be registered and opening", insideGetStream.await(TIMEOUT_SEC, TimeUnit.SECONDS));

            long start = System.nanoTime();
            channel.close();
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertThat("close must wait for the stream", elapsedMillis, greaterThanOrEqualTo(timeoutMillis));
            assertThat("close must give up at the timeout", elapsedMillis, lessThan(TimeUnit.SECONDS.toMillis(TIMEOUT_SEC)));
        } finally {
            releaseGetStream.countDown();
        }
    }

    // ── stream failure reporting ───────────────────────────────────────────────

    /**
     * A null header must fail the stream and stop, not fall through. {@code handleStreamException} does not
     * throw, so continuing would dereference the null header and replace the real failure with an NPE
     * raised inside the dispatch task — where nothing reports it to the handler.
     *
     * <p>A null header is reachable in production: {@code HeaderContext.getHeader} is a plain map removal,
     * so it returns null whenever the middleware never stored one (for example a call that closed before
     * its headers arrived).
     */
    public void testNullHeaderFailsStreamWithoutDereferencingIt() throws Exception {
        FlightStream stream = mock(FlightStream.class);
        when(stream.next()).thenReturn(true);
        when(mockFlightClient.getStream(any(Ticket.class), any())).thenReturn(stream);

        // The production path for a header that was never stored.
        HeaderContext noHeader = new HeaderContext() {
            @Override
            Header getHeader(long correlationId) {
                return null;
            }
        };
        channel = createChannel(mockFlightClient, noHeader, new FlightTransportConfig());

        AtomicReference<TransportException> handlerException = new AtomicReference<>();
        // A real executor, not SAME: that is where the bug is observable. The dispatch task runs on a
        // pool thread, so without the return the NPE escapes as an uncaught exception and fails the
        // test. Under SAME the task runs inside the CompletableFuture callback, which swallows it.
        sendStreamRequest(streamingHandler(ThreadPool.Names.GENERIC, streamResponse -> {
            throw new AssertionError("consumer must not be invoked without a header");
        }, handlerException));

        assertBusy(() -> assertNotNull("handler must be notified of the failure", handlerException.get()));
        TransportException exception = handlerException.get();
        assertTrue("expected a StreamException but got " + exception, exception instanceof StreamException);
        assertEquals(StreamErrorCode.INTERNAL, ((StreamException) exception).getErrorCode());
        assertEquals("Header is null", exception.getMessage());
    }

    /**
     * A stream failure must be reported without ever handing the throwable to log4j, at any level.
     *
     * <p>These paths run on the per-stream prefetch virtual thread. OpenSearch's JSON layout always applies
     * log4j's extended stack-trace renderer, which resolves every frame's class via {@code Class.forName};
     * the resulting native frame cannot be unmounted, so a contended classloader lock pins the carrier
     * instead of yielding it. With one such failure per stream, a mass reconnect can pin every carrier while
     * the lock holder sits unmounted and unschedulable, and the node stops making progress while still
     * reporting healthy. The cause must therefore reach the log as text.
     *
     * <p>TRACE is enabled here on purpose. The verbose branch is the one an operator reaches for while
     * debugging this very failure, so it is the branch that most needs to be safe: turning it on must not be
     * what arms the deadlock.
     */
    @TestLogging(value = "org.opensearch.arrow.flight.transport.FlightClientChannel:TRACE", reason = "verbose branch must be safe")
    public void testStreamFailureIsLoggedWithoutAThrowable() throws Exception {
        when(mockFlightClient.getStream(any(Ticket.class), any())).thenThrow(
            CallStatus.UNAVAILABLE.withDescription("connection reset").toRuntimeException()
        );

        channel = createChannel(mockFlightClient, stubHeaderContext(), new FlightTransportConfig());

        AtomicReference<TransportException> handlerException = new AtomicReference<>();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(FlightClientChannel.class))) {
            // The cause must still be identifiable from the message alone, since the throwable is gone.
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "stream failure reported with its cause",
                    FlightClientChannel.class.getCanonicalName(),
                    Level.ERROR,
                    "*Exception while handling stream response*connection reset*"
                )
            );
            // The stack trace is still available, just pre-rendered as text instead of as a throwable.
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "stack trace rendered as text at TRACE",
                    FlightClientChannel.class.getCanonicalName(),
                    Level.TRACE,
                    "*Exception while handling stream response*\n\tat *"
                )
            );
            appender.addExpectation(new NoThrowableAttachedExpectation(FlightClientChannel.class.getCanonicalName()));

            sendStreamRequest(streamingHandler(ThreadPool.Names.SAME, streamResponse -> {
                throw new AssertionError("consumer must not be invoked after an open failure");
            }, handlerException));

            assertBusy(() -> assertNotNull("handler must be notified of the failure", handlerException.get()));
            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * Fails if any event from {@code logger} carries a throwable, whatever its level. The framework's
     * expectations all assert that something was seen; this asserts a property of everything that was.
     */
    private static final class NoThrowableAttachedExpectation implements MockLogAppender.LoggingExpectation {
        private final String logger;
        private final AtomicReference<LogEvent> offender = new AtomicReference<>();

        NoThrowableAttachedExpectation(String logger) {
            this.logger = logger;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLoggerName().equals(logger) && event.getThrown() != null) {
                offender.compareAndSet(null, event.toImmutable());
            }
        }

        @Override
        public void assertMatched() {
            LogEvent event = offender.get();
            if (event != null) {
                throw new AssertionError(
                    "no throwable may be attached at any level, but ["
                        + event.getMessage().getFormattedMessage()
                        + "] logged at "
                        + event.getLevel()
                        + " carried "
                        + event.getThrown(),
                    event.getThrown()
                );
            }
        }
    }

    /** A header context that answers any correlation id, so a mocked client can drive dispatch. */
    private HeaderContext stubHeaderContext() {
        Header header = mock(Header.class);
        when(header.getHeaders()).thenReturn(new Tuple<>(Map.of(), Map.of()));
        when(header.getVersion()).thenReturn(Version.CURRENT);
        return new HeaderContext() {
            @Override
            Header getHeader(long correlationId) {
                return header;
            }
        };
    }

    private FlightClientChannel createChannel(FlightClient flightClient, HeaderContext context, FlightTransportConfig config) {
        return new FlightClientChannel(
            boundAddress,
            flightClient,
            remoteNode,
            serverLocation,
            context,
            "test-profile",
            flightTransport.getResponseHandlers(),
            threadPool,
            new TransportMessageListener() {
            },
            namedWriteableRegistry,
            statsCollector,
            config
        );
    }

    /** Consumer body for {@link #streamingHandler}, allowed to block and to throw. */
    private interface StreamConsumer {
        void accept(StreamTransportResponse<TestResponse> streamResponse) throws Exception;
    }

    private StreamTransportResponseHandler<TestResponse> streamingHandler(String executor, StreamConsumer consumer) {
        return streamingHandler(executor, consumer, null);
    }

    /**
     * @param exceptionSink where an expected handler exception is recorded; when {@code null}, any
     *                      handler exception fails the test.
     */
    private StreamTransportResponseHandler<TestResponse> streamingHandler(
        String executor,
        StreamConsumer consumer,
        AtomicReference<TransportException> exceptionSink
    ) {
        return new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    consumer.accept(streamResponse);
                } catch (Exception e) {
                    throw new AssertionError("stream consumer failed", e);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                if (exceptionSink == null) {
                    throw new AssertionError("unexpected handler exception", exp);
                }
                exceptionSink.set(exp);
            }

            @Override
            public String executor() {
                return executor;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };
    }

    /**
     * Sends a stream request over {@link #channel}, registering the handler through the transport
     * service so the channel resolves it from its response handlers as it would in production.
     */
    private void sendStreamRequest(TransportResponseHandler<TestResponse> handler) {
        Transport.Connection connection = new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return remoteNode;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
                channel.sendMessage(requestId, new BytesArray("test message"), ActionListener.wrap(r -> {}, e -> {
                    throw new AssertionError("send failed", e);
                }));
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {}

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {}
        };

        streamTransportService.sendRequest(
            connection,
            "internal:test/channel-close",
            new TestRequest(),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            handler
        );
    }

    public void testStreamResponseProcessingWithValidHandler() throws InterruptedException, IOException {

        channel = createChannel(mockFlightClient);

        String action = "internal:test/stream";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicReference<Exception> handlerException = new AtomicReference<>();
        AtomicInteger messageSentCount = new AtomicInteger(0);

        TransportMessageListener testListener = new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, TransportResponse response) {
                messageSentCount.incrementAndGet();
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                // messageSentCount.incrementAndGet();
            }
        };

        flightTransport.setMessageListener(testListener);

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    TestResponse response1 = new TestResponse("Response 1");
                    TestResponse response2 = new TestResponse("Response 2");
                    TestResponse response3 = new TestResponse("Response 3");
                    channel.sendResponseBatch(response1);
                    channel.sendResponseBatch(response2);
                    channel.sendResponseBatch(response3);
                    channel.completeStream();
                } catch (Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ioException) {
                        // Handle IO exception
                    }
                }
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    TestResponse response;
                    while ((response = streamResponse.nextResponse()) != null) {
                        assertEquals("Response " + (Integer.valueOf(responseCount.get()) + 1), response.getData());
                        responseCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    handlerException.set(e);
                } finally {
                    try {
                        streamResponse.close();
                    } catch (Exception e) {}
                    handlerLatch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                handlerException.set(exp);
                handlerLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertEquals(3, responseCount.get());
        assertNull(handlerException.get());
        assertEquals(4, messageSentCount.get()); // completeStream is counted too
    }

    public void testStreamResponseProcessingWithHandlerException() throws InterruptedException {
        String action = "internal:test/stream/exception";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    channel.sendResponse(new RuntimeException("Simulated handler exception"));
                } catch (IOException e) {}
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    while (streamResponse.nextResponse() != null) {
                    }
                } catch (RuntimeException e) {
                    handlerException.set(e);
                    handlerLatch.countDown();
                    try {
                        streamResponse.close();
                    } catch (IOException ignored) {}
                    throw e;
                }
            }

            @Override
            public void handleResponse(TestResponse response) {
                handlerLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                handlerException.set(exp);
                handlerLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNotNull(handlerException.get());
        assertEquals("Simulated handler exception", handlerException.get().getMessage());
    }

    /**
     * Wire round-trip: a {@link StreamException} with a specific {@link StreamErrorCode} sent from the
     * server handler must arrive at the client as a StreamException carrying the SAME error code and
     * message. This is the leg analytics relies on — {@code AnalyticsTransportErrors.toWireError} tags a
     * resource-exhaustion failure RESOURCE_EXHAUSTED on the data node, and the coordinator's
     * {@code fromWireError} rebuilds a 429 from the code that survives here. Flight does not serialize the
     * exception type, so the code is the only signal that crosses.
     */
    public void testStreamErrorCodeSurvivesWire() throws InterruptedException {
        assertErrorCodeRoundTrips(StreamErrorCode.RESOURCE_EXHAUSTED, "memory budget exhausted on shard");
        assertErrorCodeRoundTrips(StreamErrorCode.UNAVAILABLE, "Network closed for unknown reason");
    }

    private void assertErrorCodeRoundTrips(StreamErrorCode code, String message) throws InterruptedException {
        String action = "internal:test/stream/errorcode/" + code.name();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> received = new AtomicReference<>();

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    channel.sendResponse(new StreamException(code, message));
                } catch (IOException ignored) {}
            }
        );

        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();
        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    while (streamResponse.nextResponse() != null) {
                    }
                } catch (Exception e) {
                    received.set(e);
                    try {
                        streamResponse.close();
                    } catch (IOException ignored) {}
                    latch.countDown();
                }
            }

            @Override
            public void handleResponse(TestResponse response) {
                latch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                received.set(exp);
                latch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, new TestRequest(), options, responseHandler);

        assertTrue("no error surfaced for " + code, latch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        Exception e = received.get();
        assertNotNull("expected an error for " + code, e);
        StreamException se = (StreamException) ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof StreamException)
            .orElse(null);
        assertNotNull("error must surface as a StreamException, got: " + e, se);
        assertEquals("error code must survive the wire", code, se.getErrorCode());
        assertTrue("message must survive the wire, got: " + se.getMessage(), se.getMessage() != null && se.getMessage().contains(message));
    }

    public void testThreadPoolExhaustion() throws InterruptedException {
        ThreadPool exhaustedThreadPool = mock(ThreadPool.class);
        when(exhaustedThreadPool.executor(any())).thenThrow(new RejectedExecutionException("Thread pool exhausted"));
        FlightClientChannel testChannel = createChannel(mockFlightClient, exhaustedThreadPool);

        BytesReference message = new BytesArray("test message");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        testChannel.sendMessage(-1, message, ActionListener.wrap(response -> latch.countDown(), ex -> {
            exception.set(ex);
            latch.countDown();
        }));

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertNotNull(exception.get());

        testChannel.close();
    }

    public void testListenerManagement() throws InterruptedException {
        channel = createChannel(mockFlightClient);

        CountDownLatch connectLatch = new CountDownLatch(2);
        channel.addConnectListener(ActionListener.wrap(r -> connectLatch.countDown(), e -> connectLatch.countDown()));
        channel.addConnectListener(ActionListener.wrap(r -> connectLatch.countDown(), e -> connectLatch.countDown()));
        assertTrue(connectLatch.await(1, TimeUnit.SECONDS));

        Thread.sleep(100);
        CountDownLatch lateLatch = new CountDownLatch(1);
        channel.addConnectListener(ActionListener.wrap(r -> lateLatch.countDown(), e -> lateLatch.countDown()));
        assertTrue(lateLatch.await(1, TimeUnit.SECONDS));

        CountDownLatch closeLatch = new CountDownLatch(2);
        channel.addCloseListener(ActionListener.wrap(r -> closeLatch.countDown(), e -> closeLatch.countDown()));
        channel.addCloseListener(ActionListener.wrap(r -> closeLatch.countDown(), e -> closeLatch.countDown()));

        channel.close();
        assertTrue(closeLatch.await(1, TimeUnit.SECONDS));
    }

    public void testErrorInInterimBatchFromServer() throws InterruptedException, IOException {
        String action = "internal:test/interim-batch-error";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();
        AtomicInteger responseCount = new AtomicInteger(0);

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    TestResponse response1 = new TestResponse("Response 1");
                    channel.sendResponseBatch(response1);
                    // Add small delay to ensure batch is processed before error
                    Thread.sleep(1000);
                    throw new RuntimeException("Interim batch error");
                } catch (Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ioException) {}
                }
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    while ((streamResponse.nextResponse()) != null) {
                        responseCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    handlerException.set(e);
                } finally {
                    try {
                        streamResponse.close();
                    } catch (Exception e) {}
                    handlerLatch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                handlerException.set(exp);
                handlerLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        // Allow for race condition - response count could be 0 or 1 depending on timing
        assertTrue("Response count should be 1, but was: " + responseCount.get(), responseCount.get() == 1);
        assertNotNull(handlerException.get());
    }

    public void testStreamResponseWithCustomExecutor() throws InterruptedException, IOException {
        channel = createChannel(mockFlightClient);

        String action = "internal:test/custom-executor";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicReference<Exception> handlerException = new AtomicReference<>();

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    TestResponse response1 = new TestResponse("Response 1");
                    channel.sendResponseBatch(response1);
                    channel.completeStream();
                } catch (Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ioException) {
                        // Handle IO exception
                    }
                }
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    while ((streamResponse.nextResponse()) != null) {
                        responseCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    handlerException.set(e);
                } finally {
                    try {
                        streamResponse.close();
                    } catch (Exception e) {}
                    handlerLatch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                handlerException.set(exp);
                handlerLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);
        assertTrue(handlerLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertEquals(1, responseCount.get());
        assertNull(handlerException.get());
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/18938")
    public void testStreamResponseWithEarlyCancellation() throws InterruptedException {
        String action = "internal:test/early-cancel";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        CountDownLatch serverLatch = new CountDownLatch(1);
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicReference<Exception> handlerException = new AtomicReference<>();
        AtomicReference<Exception> serverException = new AtomicReference<>();
        AtomicBoolean secondBatchCalled = new AtomicBoolean(false);

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    TestResponse response1 = new TestResponse("Response 1");
                    channel.sendResponseBatch(response1);
                    Thread.sleep(4000); // Allow client to process and cancel
                    TestResponse response2 = new TestResponse("Response 2");
                    secondBatchCalled.set(true);
                    channel.sendResponseBatch(response2); // This should throw StreamException with CANCELLED code
                } catch (StreamException e) {
                    if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                        serverException.set(e);
                    }
                } finally {
                    serverLatch.countDown();
                }
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    TestResponse response = streamResponse.nextResponse();
                    if (response != null) {
                        responseCount.incrementAndGet();
                        // Cancel after first response
                        streamResponse.cancel("Client early cancellation", null);
                    }
                } catch (Exception e) {
                    handlerException.set(e);
                } finally {
                    handlerLatch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                handlerException.set(exp);
                handlerLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertTrue(serverLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        assertEquals(1, responseCount.get());
        assertNull(handlerException.get());

        assertTrue(secondBatchCalled.get());
        assertNotNull(
            "Server should receive StreamException with CANCELLED code when calling sendResponseBatch after cancellation",
            serverException.get()
        );
        assertEquals(StreamErrorCode.CANCELLED, ((StreamException) serverException.get()).getErrorCode());
    }

    public void testFrameworkLevelStreamCreationError() throws InterruptedException {
        String action = "internal:test/unregistered-action";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();

        // Don't register any handler for this action - this will cause framework-level error

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    while (streamResponse.nextResponse() != null) {
                    }
                } catch (Exception e) {
                    handlerException.set(e);
                    handlerLatch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                handlerException.set(exp);
                handlerLatch.countDown();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNotNull(handlerException.get());
        assertTrue(
            "Expected TransportException but got: " + handlerException.get().getClass(),
            handlerException.get() instanceof TransportException
        );
    }

    public void testSetMessageListenerTwice() {
        TransportMessageListener listener1 = new TransportMessageListener() {
        };
        TransportMessageListener listener2 = new TransportMessageListener() {
        };

        flightTransport.setMessageListener(listener1);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> flightTransport.setMessageListener(listener2));
        assertEquals("Cannot set message listener twice", exception.getMessage());
    }

    static class LargeTestRequest extends TestRequest {
        private final String largeData;

        LargeTestRequest(String data) {
            this.largeData = data;
        }

        LargeTestRequest(StreamInput in) throws IOException {
            super(in);
            this.largeData = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(largeData);
        }
    }

    public void testLargeRequest() throws Exception {
        String action = "internal:test/large";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        streamTransportService.registerRequestHandler(action, ThreadPool.Names.SAME, LargeTestRequest::new, (request, channel, task) -> {
            try {
                channel.sendResponseBatch(new TestResponse("OK"));
                channel.completeStream();
            } catch (Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (IOException ex) {}
            }
        });

        LargeTestRequest testRequest = new LargeTestRequest("X".repeat(20 * 1024));

        streamTransportService.sendRequest(
            remoteNode,
            action,
            testRequest,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new StreamTransportResponseHandler<TestResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                    try {
                        while (streamResponse.nextResponse() != null) {
                        }
                        streamResponse.close();
                    } catch (Exception e) {
                        error.set(e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    error.set(exp);
                    latch.countDown();
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

        assertTrue(latch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNull(error.get());
    }
}
