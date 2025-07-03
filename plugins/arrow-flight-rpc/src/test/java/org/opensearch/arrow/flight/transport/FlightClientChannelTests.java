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
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightClientChannelTests extends FlightTransportTestBase {

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
        verify(mockFlightClient).close();

        channel.close();
        verify(mockFlightClient, times(1)).close();
    }

    public void testChannelCloseWithException() throws Exception {
        channel = createChannel(mockFlightClient);
        doThrow(new RuntimeException("Close failed")).when(mockFlightClient).close();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        channel.addCloseListener(ActionListener.wrap(response -> latch.countDown(), ex -> {
            exception.set(ex);
            latch.countDown();
        }));

        channel.close();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertFalse(channel.isOpen());
        assertNotNull(exception.get());
        assertEquals("Close failed", exception.get().getMessage());
    }

    public void testSendMessageWhenClosed() throws InterruptedException {
        channel = createChannel(mockFlightClient);
        channel.close();

        BytesReference message = new BytesArray("test message");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        channel.sendMessage(message, ActionListener.wrap(response -> latch.countDown(), ex -> {
            exception.set(ex);
            latch.countDown();
        }));

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertNotNull(exception.get());
        assertTrue(exception.get() instanceof TransportException);
        assertEquals("FlightClientChannel is closed", exception.get().getMessage());
    }

    public void testSendMessageFailure() throws InterruptedException {
        String action = "internal:test/failure";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                throw new RuntimeException("Simulated transport failure");
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().build();

        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<TestResponse>() {
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

        assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        assertNotNull(handlerException.get());
        assertTrue(handlerException.get() instanceof TransportException);
    }

    public void testStreamResponseProcessingWithValidHandler() throws InterruptedException {
        channel = createChannel(mockFlightClient);

        String action = "internal:test/stream";
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
                    handlerLatch.countDown();
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

        assertTrue(handlerLatch.await(5, TimeUnit.SECONDS));
        assertEquals(3, responseCount.get());
        assertNull(handlerException.get());
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
                } catch (IOException e) {
                    // Handle IO exception
                }
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    TestResponse response;
                    while ((response = streamResponse.nextResponse()) != null) {
                        // Process response
                    }
                    RuntimeException ex = new RuntimeException("Handler processing failed");
                    handlerException.set(ex);
                    handlerLatch.countDown();
                    throw ex;
                } catch (RuntimeException e) {
                    handlerException.set(e);
                    handlerLatch.countDown();
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

        assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        assertNotNull(handlerException.get());
        assertTrue(handlerException.get().getMessage().contains("Failed to fetch batch"));
    }

    public void testThreadPoolExhaustion() throws InterruptedException {
        ThreadPool exhaustedThreadPool = mock(ThreadPool.class);
        when(exhaustedThreadPool.executor(any())).thenThrow(new RejectedExecutionException("Thread pool exhausted"));

        FlightClientChannel testChannel = createChannel(mockFlightClient, exhaustedThreadPool);

        BytesReference message = new BytesArray("test message");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        testChannel.sendMessage(message, ActionListener.wrap(response -> latch.countDown(), ex -> {
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

    public void testErrorInDeserializingResponse() throws InterruptedException {
        String action = "internal:test/deserialize-error";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    channel.sendResponse(new TestResponse("valid-response"));
                } catch (IOException e) {
                    // Handle IO exception
                }
            }
        );

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().build();

        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<TestResponse>() {
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
                throw new IOException("Simulated deserialization error");
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        assertNotNull(handlerException.get());
    }

    public void testErrorInInterimBatchFromServer() throws InterruptedException {
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

                    throw new RuntimeException("Interim batch error");
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
                        responseCount.incrementAndGet();
                    }
                    handlerLatch.countDown();
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

        assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        assertEquals(1, responseCount.get());
    }

    public void testStreamResponseWithCustomExecutor() throws InterruptedException {
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
                    TestResponse response;
                    while ((response = streamResponse.nextResponse()) != null) {
                        responseCount.incrementAndGet();
                    }
                    handlerLatch.countDown();
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
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        };

        streamTransportService.sendRequest(remoteNode, action, testRequest, options, responseHandler);

        assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        assertEquals(1, responseCount.get());
        assertNull(handlerException.get());
    }

    public void testRequestWithTimeout() throws InterruptedException {
        String action = "internal:test/timeout";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();

        streamTransportService.registerRequestHandler(
            action,
            ThreadPool.Names.SAME,
            in -> new TestRequest(in),
            (request, channel, task) -> {
                try {
                    Thread.sleep(2000);
                    channel.sendResponse(new TestResponse("delayed response"));
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
        TransportRequestOptions options = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.STREAM)
            .withTimeout(1)
            .build();

        TransportResponseHandler<TestResponse> responseHandler = new TransportResponseHandler<TestResponse>() {
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

        assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        assertTrue(handlerException.get() instanceof ReceiveTimeoutTransportException);
    }
}
