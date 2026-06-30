/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightOutboundHandlerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private FlightOutboundHandler handler;
    private ExecutorService executor;
    private FlightServerChannel mockFlightChannel;
    private TransportMessageListener mockListener;

    private static final String HEADER_KEY = "test-header";
    private static final String HEADER_VALUE = "test-value";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        executor = Executors.newSingleThreadExecutor();
        handler = new FlightOutboundHandler("test-node", Version.CURRENT, new String[0], new StatsTracker(), threadPool);

        mockFlightChannel = mock(FlightServerChannel.class);
        when(mockFlightChannel.getExecutor()).thenReturn(executor);
        when(mockFlightChannel.getAllocator()).thenReturn(mock(BufferAllocator.class));

        mockListener = mock(TransportMessageListener.class);
        handler.setMessageListener(mockListener);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        threadPool.shutdown();
        super.tearDown();
    }

    public void testSendResponseBatchPreservesCallerThreadContext() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader(HEADER_KEY, HEADER_VALUE);

        doAnswer(invocation -> null).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        // Verify the caller's thread context is NOT cleared
        assertEquals(
            "Caller's thread context should be preserved after sendResponseBatch",
            HEADER_VALUE,
            threadContext.getHeader(HEADER_KEY)
        );
    }

    public void testCompleteStreamPreservesCallerThreadContext() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader(HEADER_KEY, HEADER_VALUE);

        handler.completeStream(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action"
        );

        assertEquals("Caller's thread context should be preserved after completeStream", HEADER_VALUE, threadContext.getHeader(HEADER_KEY));
    }

    public void testSendErrorResponsePreservesCallerThreadContext() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader(HEADER_KEY, HEADER_VALUE);

        handler.sendErrorResponse(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            new RuntimeException("test error")
        );

        assertEquals(
            "Caller's thread context should be preserved after sendErrorResponse",
            HEADER_VALUE,
            threadContext.getHeader(HEADER_KEY)
        );
    }

    public void testSendResponseBatchPropagatesContextToExecutorThread() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader(HEADER_KEY, HEADER_VALUE);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> capturedHeader = new AtomicReference<>();

        // Capture the thread context header inside onResponseSent, which runs
        // within the preserveContext wrapper on the executor thread
        doAnswer(invocation -> {
            capturedHeader.set(threadPool.getThreadContext().getHeader(HEADER_KEY));
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("Executor task should complete", latch.await(5, TimeUnit.SECONDS));
        assertEquals("Context should be propagated to executor thread", HEADER_VALUE, capturedHeader.get());
    }

    public void testMultipleBatchesMaintainCallerContext() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader(HEADER_KEY, HEADER_VALUE);

        Set<String> features = Collections.emptySet();
        FlightTransportChannel mockTransportChannel = mock(FlightTransportChannel.class);

        // Send multiple batches
        for (int i = 0; i < 3; i++) {
            handler.sendResponseBatch(
                Version.CURRENT,
                features,
                mockFlightChannel,
                mockTransportChannel,
                1L,
                "test-action",
                mock(TransportResponse.class),
                false,
                false
            );

            assertEquals(
                "Caller's thread context should be preserved after batch " + (i + 1),
                HEADER_VALUE,
                threadContext.getHeader(HEADER_KEY)
            );
        }

        // Complete the stream
        handler.completeStream(Version.CURRENT, features, mockFlightChannel, mockTransportChannel, 1L, "test-action");

        assertEquals("Caller's thread context should be preserved after completeStream", HEADER_VALUE, threadContext.getHeader(HEADER_KEY));
    }

    // --- processBatchTask delegates to the channel ---
    // The channel owns the stream root (create/transfer/adopt/free) and builds the header internally;
    // these tests verify delegation + listener notification. Root ownership is covered in
    // FlightServerChannelTests.

    public void testProcessBatchTaskDelegatesToChannelSendBatch() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot producerRoot = newSingleIntRoot(allocator, 42);

            CountDownLatch latch = new CountDownLatch(1);
            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

            // The channel takes the response; the handler must not touch its root.
            AtomicReference<TransportResponse> sent = new AtomicReference<>();
            doAnswer(invocation -> {
                sent.set(invocation.getArgument(0));
                return null;
            }).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

            TestArrowResponse response = new TestArrowResponse(producerRoot);
            handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                mock(FlightTransportChannel.class),
                1L,
                "test-action",
                response,
                false,
                false
            );

            assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendBatch(any(TransportResponse.class), any());
            assertSame("handler should pass the response straight to the channel", response, sent.get());
            // Channel mock did not consume buffers; free the producer root so teardown is clean.
            producerRoot.close();
        }
    }

    public void testProcessBatchTaskNonArrowResponseDelegatesToSendBatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        doAnswer(invocation -> null).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
        verify(mockFlightChannel).sendBatch(any(TransportResponse.class), any());
    }

    // --- Hand-off discipline: a batch that never reaches the channel must be released (S7) ---

    /**
     * When the back-pressure gate throws (cancel/timeout), the batch is never submitted, so the
     * handler must free the source root via releaseUnsent and must NOT submit the task.
     */
    public void testSendResponseBatchReleasesSourceWhenGateThrows() {
        ExecutorService submitTrap = mock(ExecutorService.class);
        when(mockFlightChannel.getExecutor()).thenReturn(submitTrap);

        StreamException cancelled = StreamException.cancelled("client gone");
        doThrow(cancelled).when(mockFlightChannel).awaitReadyOrThrow();

        TransportResponse response = mock(TransportResponse.class);
        StreamException thrown = expectThrows(
            StreamException.class,
            () -> handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                mock(FlightTransportChannel.class),
                1L,
                "test-action",
                response,
                false,
                false
            )
        );

        assertSame(cancelled, thrown);
        verify(mockFlightChannel).releaseUnsent(response); // freed on the failed-handoff path
        verify(submitTrap, never()).execute(any());        // not submitted after the gate failed
    }

    /**
     * When the executor rejects the task (e.g. shutdown), the batch never runs, so the handler must
     * free the source root via releaseUnsent.
     */
    public void testSendResponseBatchReleasesSourceWhenExecutorRejects() {
        ExecutorService rejectingExecutor = mock(ExecutorService.class);
        when(mockFlightChannel.getExecutor()).thenReturn(rejectingExecutor);
        doThrow(new RejectedExecutionException("shutting down")).when(rejectingExecutor).execute(any());

        TransportResponse response = mock(TransportResponse.class);
        expectThrows(
            RejectedExecutionException.class,
            () -> handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                mock(FlightTransportChannel.class),
                1L,
                "test-action",
                response,
                false,
                false
            )
        );

        verify(mockFlightChannel).releaseUnsent(response);
    }

    /** A successful hand-off must NOT also release the source (it is owned by sendBatch's finally). */
    public void testSendResponseBatchDoesNotReleaseOnSuccessfulHandoff() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));
        doAnswer(invocation -> null).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        TransportResponse response = mock(TransportResponse.class);
        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            response,
            false,
            false
        );

        assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
        verify(mockFlightChannel, never()).releaseUnsent(any());
    }

    // --- failStream: a batch send failure fails the stream instead of hanging the consumer ---

    public void testProcessBatchTaskFailureSendsErrorAndReleasesChannel() throws Exception {
        doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);
        CountDownLatch notified = new CountDownLatch(1);
        doAnswer(invocation -> {
            notified.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            transportChannel,
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("failed batch send must terminate the stream", notified.await(5, TimeUnit.SECONDS));
        // Stream is failed BEFORE the listener is notified (consumer sees an error, not a hang).
        org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(mockFlightChannel, transportChannel, mockListener);
        inOrder.verify(mockFlightChannel).sendError(any(), any(Exception.class));
        inOrder.verify(transportChannel).releaseChannel(true);
        inOrder.verify(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));
    }

    public void testProcessBatchTaskFlightRuntimeExceptionFailsStreamWithMappedError() throws Exception {
        FlightRuntimeException flightError = CallStatus.INTERNAL.withDescription("flight send failed").toRuntimeException();
        doThrow(flightError).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);
        CountDownLatch notified = new CountDownLatch(1);
        AtomicReference<Object> notifiedArg = new AtomicReference<>();
        doAnswer(invocation -> {
            notifiedArg.set(invocation.getArgument(2));
            notified.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            transportChannel,
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("FlightRuntimeException must terminate the stream", notified.await(5, TimeUnit.SECONDS));
        verify(mockFlightChannel).sendError(any(), any(Exception.class));
        verify(transportChannel).releaseChannel(true);
        assertTrue("listener should receive the mapped StreamException", notifiedArg.get() instanceof StreamException);
    }

    public void testFailStreamSwallowsSendErrorFailureAndStillReleases() throws Exception {
        doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());
        // sendError itself fails (consumer already gone) — must be swallowed.
        doThrow(new RuntimeException("sendError failed too")).when(mockFlightChannel).sendError(any(), any(Exception.class));

        FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);
        CountDownLatch released = new CountDownLatch(1);
        doAnswer(invocation -> {
            released.countDown();
            return null;
        }).when(transportChannel).releaseChannel(true);

        CountDownLatch notified = new CountDownLatch(1);
        AtomicReference<Object> notifiedArg = new AtomicReference<>();
        doAnswer(invocation -> {
            notifiedArg.set(invocation.getArgument(2));
            notified.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            transportChannel,
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("channel must still be released after sendError fails", released.await(5, TimeUnit.SECONDS));
        assertTrue("listener must still be notified after sendError fails", notified.await(5, TimeUnit.SECONDS));
        verify(transportChannel).releaseChannel(true);
        // The sendError failure must not be lost: it is attached as a suppressed exception on the
        // original cause that the listener receives.
        assertTrue("listener arg must be the original cause", notifiedArg.get() instanceof Exception);
        Throwable[] suppressed = ((Throwable) notifiedArg.get()).getSuppressed();
        assertEquals("the sendError failure must be attached as suppressed", 1, suppressed.length);
        assertEquals("sendError failed too", suppressed[0].getMessage());
    }

    public void testFailStreamWithNullTransportChannelDoesNotThrow() throws Exception {
        doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        CountDownLatch notified = new CountDownLatch(1);
        doAnswer(invocation -> {
            notified.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            null,
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("stream must still be failed and the listener notified", notified.await(5, TimeUnit.SECONDS));
        verify(mockFlightChannel).sendError(any(), any(Exception.class));
    }

    /**
     * A non-Exception {@code Throwable} from the send (e.g. OOM / AssertionError after the channel
     * adopted the stream root) must still release the channel so its close()-posted stream-root free
     * runs — the batch task is non-terminal, so {@code BatchTask.close()} would not release it. The
     * {@code Throwable} must not be swallowed.
     */
    public void testProcessBatchTaskThrowableReleasesChannel() throws Exception {
        doThrow(new AssertionError("putNext boom")).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);
        CountDownLatch released = new CountDownLatch(1);
        doAnswer(invocation -> {
            released.countDown();
            return null;
        }).when(transportChannel).releaseChannel(true);

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            transportChannel,
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        // The Throwable path must release the channel (-> close() -> stream-root free) even though
        // failStream is not invoked for a non-Exception Throwable.
        assertTrue("channel must be released on a non-Exception Throwable", released.await(5, TimeUnit.SECONDS));
        verify(transportChannel).releaseChannel(true);
    }

    /** With no transport channel, the Throwable path must close the channel directly (still frees the root). */
    public void testProcessBatchTaskThrowableClosesChannelWhenNoTransportChannel() throws Exception {
        doThrow(new AssertionError("putNext boom")).when(mockFlightChannel).sendBatch(any(TransportResponse.class), any());

        CountDownLatch closed = new CountDownLatch(1);
        doAnswer(invocation -> {
            closed.countDown();
            return null;
        }).when(mockFlightChannel).close();

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            null,
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        assertTrue("channel must be closed directly when there is no transport channel", closed.await(5, TimeUnit.SECONDS));
        verify(mockFlightChannel).close();
    }

    // --- processCompleteTask error path ---

    public void testProcessCompleteTaskErrorPath() throws Exception {
        RuntimeException completeError = new RuntimeException("complete failed");
        doThrow(completeError).when(mockFlightChannel).completeStream(any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> capturedError = new AtomicReference<>();

        doAnswer(invocation -> {
            capturedError.set(invocation.getArgument(2));
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));

        handler.completeStream(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action"
        );

        assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
        assertSame("Error should be passed to listener", completeError, capturedError.get());
    }

    // --- Back-pressure path: gating on the producer thread before executor submit ---

    /**
     * sendResponseBatch must call {@code awaitReadyOrThrow} on the producer thread
     * BEFORE submitting the BatchTask to the executor — that is what throttles
     * allocation under a slow consumer.
     */
    public void testSendResponseBatchGatesBeforeExecutor() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean awaitCalled = new java.util.concurrent.atomic.AtomicBoolean(false);
        doAnswer(inv -> {
            awaitCalled.set(true);
            return null;
        }).when(mockFlightChannel).awaitReadyOrThrow();

        CountDownLatch executorRan = new CountDownLatch(1);
        doAnswer(invocation -> {
            executorRan.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        handler.sendResponseBatch(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mock(FlightTransportChannel.class),
            1L,
            "test-action",
            mock(TransportResponse.class),
            false,
            false
        );

        // sendResponseBatch returns only after the gate has run.
        assertTrue("awaitReadyOrThrow must run before sendResponseBatch returns", awaitCalled.get());
        assertTrue("Executor task should complete", executorRan.await(5, TimeUnit.SECONDS));
        verify(mockFlightChannel).awaitReadyOrThrow();
    }

    /**
     * If awaitReadyOrThrow throws (timeout / cancellation), the StreamException must
     * propagate to the caller — sendResponseBatch must NOT submit the BatchTask, and
     * NOT silently swallow the failure.
     */
    public void testSendResponseBatchPropagatesAwaitReadyException() {
        ExecutorService submitTrap = mock(ExecutorService.class);
        when(mockFlightChannel.getExecutor()).thenReturn(submitTrap);

        StreamException timeoutEx = new StreamException(StreamErrorCode.TIMED_OUT, "consumer not ready");
        doThrow(timeoutEx).when(mockFlightChannel).awaitReadyOrThrow();

        StreamException thrown = expectThrows(
            StreamException.class,
            () -> handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                mock(FlightTransportChannel.class),
                1L,
                "test-action",
                mock(TransportResponse.class),
                false,
                false
            )
        );
        assertSame(timeoutEx, thrown);
        // Crucially: we must not have submitted the BatchTask after the gate failed.
        verify(submitTrap, never()).execute(any());
    }

    public void testBatchTaskCloseWithIsErrorCallsReleaseChannelWithTrue() {
        FlightTransportChannel mockTransportChannel = mock(FlightTransportChannel.class);

        FlightOutboundHandler.BatchTask task = new FlightOutboundHandler.BatchTask(
            Version.CURRENT,
            Collections.emptySet(),
            mockFlightChannel,
            mockTransportChannel,
            1L,
            "test-action",
            null,
            false,
            false,
            false, // isComplete
            true,  // isError
            new RuntimeException("error")
        );

        task.close();

        verify(mockTransportChannel).releaseChannel(true);
    }

    // --- Test helpers ---

    private static VectorSchemaRoot newSingleIntRoot(BufferAllocator allocator, int value) {
        Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector vec = (IntVector) root.getVector("val");
        vec.allocateNew();
        vec.setSafe(0, value);
        vec.setValueCount(1);
        root.setRowCount(1);
        return root;
    }

    static class TestArrowResponse extends ArrowBatchResponse {
        TestArrowResponse(VectorSchemaRoot root) {
            super(root);
        }

        TestArrowResponse(StreamInput in) throws IOException {
            super(in);
        }
    }
}
