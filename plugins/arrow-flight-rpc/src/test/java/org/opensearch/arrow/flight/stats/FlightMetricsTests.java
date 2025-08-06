/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.arrow.flight.transport.FlightTransportTestBase;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FlightMetricsTests extends FlightTransportTestBase {
    private final int TIMEOUT_SEC = 10;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testComprehensiveMetrics() throws Exception {
        registerHandlers();
        sendSimpleMessage();
        sendSuccessfulStreamingRequest();
        sendFailingStreamingRequest();
        sendCancelledStreamingRequest();
        Thread.sleep(2000);
        verifyMetrics();
    }

    private void registerHandlers() {
        streamTransportService.registerRequestHandler(
            "internal:test/metrics/success",
            ThreadPool.Names.SAME,
            TestRequest::new,
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
                    } catch (IOException ioException) {}
                }
            }
        );

        streamTransportService.registerRequestHandler(
            "internal:test/metrics/failure",
            ThreadPool.Names.SAME,
            TestRequest::new,
            (request, channel, task) -> {
                try {
                    channel.sendResponse(new RuntimeException("Simulated failure"));
                } catch (IOException ignored) {}
            }
        );

        streamTransportService.registerRequestHandler(
            "internal:test/metrics/cancel",
            ThreadPool.Names.SAME,
            TestRequest::new,
            (request, channel, task) -> {
                try {
                    TestResponse response1 = new TestResponse("Response 1");
                    channel.sendResponseBatch(response1);

                    Thread.sleep(1000);

                    try {
                        TestResponse response2 = new TestResponse("Response 2");
                        channel.sendResponseBatch(response2);
                    } catch (Exception e) {}
                } catch (Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ioException) {}
                }
            }
        );

        streamTransportService.registerRequestHandler(
            "internal:test/simple",
            ThreadPool.Names.SAME,
            TestRequest::new,
            (request, channel, task) -> {
                try {
                    TestResponse response = new TestResponse("Simple Response");
                    channel.sendResponseBatch(response);
                    channel.completeStream();
                } catch (Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException ioException) {}
                }
            }
        );
    }

    private void sendSimpleMessage() throws Exception {
        TestRequest testRequest = new TestRequest();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try (streamResponse) {
                    try {
                        TestResponse response = streamResponse.nextResponse();
                        if (response != null) {
                            latch.countDown();
                        }
                    } catch (Exception e) {
                        exception.set(e);
                        latch.countDown();
                    }
                } catch (Exception ignored) {}
            }

            @Override
            public void handleException(TransportException exp) {
                exception.set(exp);
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

        streamTransportService.sendRequest(remoteNode, "internal:test/simple", testRequest, options, responseHandler);

        assertTrue("Simple message should complete", latch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNull("Simple message should not fail", exception.get());
    }

    private void sendSuccessfulStreamingRequest() throws Exception {
        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger responseCount = new AtomicInteger(0);
        AtomicReference<Exception> exception = new AtomicReference<>();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try (streamResponse) {
                    try {
                        while (streamResponse.nextResponse() != null) {
                            responseCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        exception.set(e);
                    }
                } catch (Exception ignored) {} finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                exception.set(exp);
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

        streamTransportService.sendRequest(remoteNode, "internal:test/metrics/success", testRequest, options, responseHandler);

        assertTrue("Successful streaming should complete", latch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNull("Successful streaming should not fail", exception.get());
        assertEquals("Should receive 3 responses", 3, responseCount.get());
    }

    private void sendFailingStreamingRequest() throws Exception {
        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try {
                    while (streamResponse.nextResponse() != null) {
                        // Process responses
                    }
                } catch (Exception e) {
                    exception.set(e);
                    throw e;
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                exception.set(exp);
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

        streamTransportService.sendRequest(remoteNode, "internal:test/metrics/failure", testRequest, options, responseHandler);

        assertTrue("Failing streaming should complete", latch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNotNull("Failing streaming should fail", exception.get());
    }

    private void sendCancelledStreamingRequest() throws Exception {
        TestRequest testRequest = new TestRequest();
        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        StreamTransportResponseHandler<TestResponse> responseHandler = new StreamTransportResponseHandler<>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                try (streamResponse) {
                    try {
                        // Get first response then cancel
                        TestResponse response = streamResponse.nextResponse();
                        if (response != null) {
                            streamResponse.cancel("Client cancellation", null);
                        }
                    } catch (Exception e) {
                        exception.set(e);
                    }
                } catch (Exception ignored) {} finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                exception.set(exp);
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

        streamTransportService.sendRequest(remoteNode, "internal:test/metrics/cancel", testRequest, options, responseHandler);

        assertTrue("Cancelled streaming should complete", latch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
        assertNull("Cancelled streaming should not fail in client", exception.get());
    }

    private void verifyMetrics() {
        FlightMetrics metrics = statsCollector.collectStats();

        // Client call metrics
        FlightMetrics.ClientCallMetrics clientCallMetrics = metrics.getClientCallMetrics();
        assertEquals("Should have 4 client calls started", 4, clientCallMetrics.getStarted());
        assertEquals("Should have 4 client calls completed", 4, clientCallMetrics.getCompleted());

        // Check status counts from the status map
        long okStatusCount = metrics.getStatusCount(true, StreamErrorCode.OK.name());
        long cancelledStatusCount = metrics.getStatusCount(true, StreamErrorCode.CANCELLED.name());

        // Check for error statuses
        long errorStatusCount = 0;
        for (StreamErrorCode errorCode : new StreamErrorCode[] {
            StreamErrorCode.INTERNAL,
            StreamErrorCode.UNKNOWN,
            StreamErrorCode.UNAVAILABLE }) {
            errorStatusCount += metrics.getStatusCount(true, errorCode.name());
        }

        assertEquals("Should have 2 OK status", 2, okStatusCount);
        assertEquals("Should have 1 CANCELLED status", 1, cancelledStatusCount);
        assertTrue("Should have at least one error status", errorStatusCount > 0);

        assertTrue("Client request bytes should be recorded", clientCallMetrics.getRequestBytes().getSum() > 0);

        // Client batch metrics
        FlightMetrics.ClientBatchMetrics clientBatchMetrics = metrics.getClientBatchMetrics();
        assertTrue("Should have batches requested", clientBatchMetrics.getBatchesRequested() >= 3);
        assertTrue("Should have batches received", clientBatchMetrics.getBatchesReceived() >= 5);
        assertTrue("Client batch received bytes should be recorded", clientBatchMetrics.getReceivedBytes().getSum() > 0);

        // Server call metrics
        FlightMetrics.ServerCallMetrics serverCallMetrics = metrics.getServerCallMetrics();
        assertEquals("Should have 4 server calls started", 4, serverCallMetrics.getStarted());
        assertEquals("Should have 4 server calls completed", 4, serverCallMetrics.getCompleted());

        // Check server status counts
        okStatusCount = metrics.getStatusCount(false, StreamErrorCode.OK.name());
        cancelledStatusCount = metrics.getStatusCount(false, StreamErrorCode.CANCELLED.name());

        // Check for error statuses
        errorStatusCount = 0;
        for (StreamErrorCode errorCode : new StreamErrorCode[] {
            StreamErrorCode.INTERNAL,
            StreamErrorCode.UNKNOWN,
            StreamErrorCode.UNAVAILABLE }) {
            errorStatusCount += metrics.getStatusCount(false, errorCode.name());
        }

        assertEquals("Should have 1 OK status", 2, okStatusCount);
        assertEquals("Should have 1 CANCELLED status", 1, cancelledStatusCount);
        assertEquals("Should have one error status", 1, errorStatusCount);

        assertTrue("Server request bytes should be recorded", serverCallMetrics.getRequestBytes().getSum() > 0);

        // Server batch metrics
        FlightMetrics.ServerBatchMetrics serverBatchMetrics = metrics.getServerBatchMetrics();
        assertTrue("Should have batches sent", serverBatchMetrics.getBatchesSent() >= 5);
        assertTrue("Server batch sent bytes should be recorded", serverBatchMetrics.getSentBytes().getSum() > 0);
    }
}
