/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;
import org.junit.After;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
