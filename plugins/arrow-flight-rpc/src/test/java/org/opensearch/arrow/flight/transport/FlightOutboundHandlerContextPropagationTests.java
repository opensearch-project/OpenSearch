/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests that thread context headers are properly propagated through the Flight transport layer
 * when sending response batches, completing streams, and sending errors.
 */
public class FlightOutboundHandlerContextPropagationTests extends FlightTransportTestBase {

    private static final int TIMEOUT_SEC = 10;
    private static final String CONTEXT_HEADER = "test-context-header";
    private static final String CONTEXT_VALUE = "propagated-value";

    public void testThreadContextPropagatedThroughStreamResponseBatch() throws InterruptedException {
        String action = "internal:test/context-propagation";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicReference<Exception> handlerException = new AtomicReference<>();
        AtomicReference<String> capturedHeaderOnServer = new AtomicReference<>();

        streamTransportService.registerRequestHandler(action, ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            try {
                // Set a header in the request handler's thread context
                threadPool.getThreadContext().putHeader(CONTEXT_HEADER, CONTEXT_VALUE);

                // Verify context is set before sending batch
                assertEquals(CONTEXT_VALUE, threadPool.getThreadContext().getHeader(CONTEXT_HEADER));

                channel.sendResponseBatch(new TestResponse("Response 1"));

                // Verify the caller's context is preserved after sendResponseBatch
                capturedHeaderOnServer.set(threadPool.getThreadContext().getHeader(CONTEXT_HEADER));

                channel.sendResponseBatch(new TestResponse("Response 2"));

                // Verify context is still preserved after second batch
                assertEquals(CONTEXT_VALUE, threadPool.getThreadContext().getHeader(CONTEXT_HEADER));

                channel.completeStream();

                // Verify context is still preserved after completeStream
                assertEquals(CONTEXT_VALUE, threadPool.getThreadContext().getHeader(CONTEXT_HEADER));
            } catch (Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (IOException ignored) {}
            }
        });

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try (streamResponse) {
                    try {
                        while (streamResponse.nextResponse() != null) {
                            responseCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        handlerException.set(e);
                    }
                } catch (Exception ignored) {} finally {
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
        assertEquals(2, responseCount.get());
        assertNull("No exception expected but got: " + handlerException.get(), handlerException.get());
        assertEquals(
            "Thread context header should be preserved on the server handler thread after sendResponseBatch",
            CONTEXT_VALUE,
            capturedHeaderOnServer.get()
        );
    }

    public void testThreadContextPropagatedThroughErrorResponse() throws InterruptedException {
        String action = "internal:test/context-error-propagation";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicReference<Exception> handlerException = new AtomicReference<>();
        AtomicReference<String> capturedHeaderOnServer = new AtomicReference<>();

        streamTransportService.registerRequestHandler(action, ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            try {
                // Set a header in the request handler's thread context
                threadPool.getThreadContext().putHeader(CONTEXT_HEADER, CONTEXT_VALUE);

                // Send an error
                channel.sendResponse(new RuntimeException("Intentional test error"));

                // Verify the caller's context is preserved after sendErrorResponse
                capturedHeaderOnServer.set(threadPool.getThreadContext().getHeader(CONTEXT_HEADER));
            } catch (IOException ignored) {}
        });

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try (streamResponse) {
                    try {
                        while (streamResponse.nextResponse() != null) {
                        }
                    } catch (Exception e) {
                        handlerException.set(e);
                    }
                } catch (Exception ignored) {} finally {
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
        assertEquals(
            "Thread context header should be preserved on the server handler thread after sendErrorResponse",
            CONTEXT_VALUE,
            capturedHeaderOnServer.get()
        );
    }

    public void testContextHeaderPropagatedToResponseHeaders() throws InterruptedException {
        String action = "internal:test/context-header-in-response";
        CountDownLatch handlerLatch = new CountDownLatch(1);
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicReference<Exception> handlerException = new AtomicReference<>();
        AtomicInteger messageSentCount = new AtomicInteger(0);

        TransportMessageListener testListener = new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, TransportResponse response) {
                messageSentCount.incrementAndGet();
            }

        };

        flightTransport.setMessageListener(testListener);

        streamTransportService.registerRequestHandler(action, ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            try {
                channel.sendResponseBatch(new TestResponse("batch-1"));
                channel.sendResponseBatch(new TestResponse("batch-2"));
                channel.completeStream();
            } catch (Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (IOException ioException) {}
            }
        });

        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<TestResponse>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try (streamResponse) {
                    try {
                        while (streamResponse.nextResponse() != null) {
                            responseCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        handlerException.set(e);
                    }
                } catch (Exception ignored) {} finally {
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
        assertEquals(2, responseCount.get());
        assertNull(handlerException.get());
        // 2 batches + 1 completeStream = 3 message sent events
        assertEquals(3, messageSentCount.get());
    }
}
