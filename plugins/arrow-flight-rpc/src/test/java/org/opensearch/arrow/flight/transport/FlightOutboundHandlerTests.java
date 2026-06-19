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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
        when(mockFlightChannel.getRoot()).thenReturn(mock(VectorSchemaRoot.class));

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

    // --- Native Arrow branch in processBatchTask ---
    // The handler delegates the native path to FlightServerChannel.sendArrowBatch, which owns the
    // stream root (create/transfer/adopt/free). These tests verify the delegation + listener
    // notification; root ownership is covered in FlightServerChannelTests.

    public void testProcessBatchTaskNativeArrowDelegatesToSendArrowBatch() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 42);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            CountDownLatch latch = new CountDownLatch(1);
            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

            // The channel owns the root; the mock receives the producer root and the metadata.
            AtomicReference<VectorSchemaRoot> sent = new AtomicReference<>();
            doAnswer(invocation -> {
                sent.set(invocation.getArgument(1));
                return null;
            }).when(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());

            handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                mock(FlightTransportChannel.class),
                1L,
                "test-action",
                new TestArrowResponse(producerRoot),
                false,
                false
            );

            assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());
            verify(mockFlightChannel, org.mockito.Mockito.never()).sendBatch(any(), any(VectorStreamOutput.class), any());
            assertSame("handler should pass the producer root to the channel", producerRoot, sent.get());
            producerRoot.close();
        }
    }

    public void testProcessBatchTaskNonArrowResponseUsesSendBatch() throws Exception {
        // setUp's mockFlightChannel.getRoot() returns a mock root, so the byte-serialization path
        // reuses it instead of allocating from the mock allocator.
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

        doAnswer(invocation -> null).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

        // A plain (non-Arrow) TransportResponse goes through the byte-serialization sendBatch path.
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
        verify(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());
        verify(mockFlightChannel, org.mockito.Mockito.never()).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());
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

        org.opensearch.transport.stream.StreamException timeoutEx = new org.opensearch.transport.stream.StreamException(
            org.opensearch.transport.stream.StreamErrorCode.TIMED_OUT,
            "consumer not ready"
        );
        doThrow(timeoutEx).when(mockFlightChannel).awaitReadyOrThrow();

        org.opensearch.transport.stream.StreamException thrown = expectThrows(
            org.opensearch.transport.stream.StreamException.class,
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
        verify(submitTrap, org.mockito.Mockito.never()).execute(any());
    }

    /**
     * When a batch send fails, the handler must fail the stream via {@code sendError} instead of
     * silently dropping the batch. Otherwise the stream is later closed with no schema frame ever
     * reaching the client, whose {@code FlightStream.getRoot()} then blocks forever. Verifies the
     * batch failure (a) fails the stream via {@code sendError}, (b) releases the channel (so a
     * later {@code completeStream} cannot double-terminate the gRPC listener), and (c) fails the
     * stream BEFORE notifying the listener. The channel owns the stream root (sendArrowBatch frees
     * it on failure), so the handler has nothing to clean up.
     */
    public void testProcessBatchTaskFailureSendsErrorOnStream() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot producerRoot = newSingleIntRoot(allocator, 7);

            // The native batch send fails (e.g. malformed batch / transport error).
            doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());

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
                new TestArrowResponse(producerRoot),
                false,
                false
            );

            assertTrue("failed batch send must terminate the stream", released.await(5, TimeUnit.SECONDS));
            // sendError fails the gRPC call; releaseChannel(true) makes the channel terminal so a
            // later completeStream no-ops instead of calling completed() after error().
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            verify(transportChannel).releaseChannel(true);
            // Stream is failed BEFORE the listener is notified (mirrors processErrorTask ordering).
            org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(mockFlightChannel, mockListener);
            inOrder.verify(mockFlightChannel).sendError(any(), any(Exception.class));
            inOrder.verify(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));
            producerRoot.close();
        }
    }

    /**
     * {@code FlightRuntimeException} variant of the failure path: when the send throws a Flight
     * transport error it is caught by the dedicated {@code catch (FlightRuntimeException)} branch,
     * which still fails the stream and then notifies the listener with the mapped
     * {@code StreamException} (via {@code FlightErrorMapper.fromFlightException}) rather than the raw
     * Flight exception.
     */
    public void testProcessBatchTaskFlightRuntimeExceptionFailsStream() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot producerRoot = newSingleIntRoot(allocator, 11);

            FlightRuntimeException flightError = CallStatus.INTERNAL.withDescription("flight send failed").toRuntimeException();
            doThrow(flightError).when(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());

            FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);

            // Await the listener notification (the last step of the catch block) so the assertions
            // below race neither sendError nor releaseChannel, both of which run earlier in failStream.
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
                new TestArrowResponse(producerRoot),
                false,
                false
            );

            assertTrue("FlightRuntimeException must terminate the stream", notified.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            verify(transportChannel).releaseChannel(true);
            // The listener is notified with the mapped StreamException, not the raw Flight exception.
            assertTrue("listener should receive the mapped StreamException", notifiedArg.get() instanceof StreamException);
            producerRoot.close();
        }
    }

    /**
     * When the send failure carries a {@code StreamException}, {@code failStream} maps it back to a
     * {@code FlightRuntimeException} (via {@code FlightErrorMapper.toFlightException}) before calling
     * {@code sendError}, so the consumer sees the original stream error code. Exercises the
     * {@code cause instanceof StreamException} branch in {@code failStream}.
     */
    public void testProcessBatchTaskStreamExceptionMappedToFlightError() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot producerRoot = newSingleIntRoot(allocator, 13);

            StreamException streamError = new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, "breaker tripped");
            doThrow(streamError).when(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());

            FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);
            CountDownLatch released = new CountDownLatch(1);
            doAnswer(invocation -> {
                released.countDown();
                return null;
            }).when(transportChannel).releaseChannel(true);

            AtomicReference<Object> sentError = new AtomicReference<>();
            doAnswer(invocation -> {
                sentError.set(invocation.getArgument(1));
                return null;
            }).when(mockFlightChannel).sendError(any(), any(Exception.class));

            handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                transportChannel,
                1L,
                "test-action",
                new TestArrowResponse(producerRoot),
                false,
                false
            );

            assertTrue("StreamException must terminate the stream", released.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            verify(transportChannel).releaseChannel(true);
            // The StreamException is mapped to a FlightRuntimeException before being sent on the wire.
            assertTrue("StreamException should be mapped to a FlightRuntimeException", sentError.get() instanceof FlightRuntimeException);
            producerRoot.close();
        }
    }

    /**
     * {@code sendError} itself can fail (consumer already gone, header serialization error). The
     * failure must be swallowed (logged at debug) and must NOT prevent the channel from being
     * released or the listener from being notified — otherwise a failed {@code sendError} would
     * leave the stream un-terminated. Exercises the {@code catch (Exception suppressed)} branch.
     */
    public void testProcessBatchTaskSendErrorFailureStillReleasesChannel() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot producerRoot = newSingleIntRoot(allocator, 17);

            doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());
            // sendError also fails (consumer already gone) — must be swallowed.
            doThrow(new RuntimeException("sendError failed too")).when(mockFlightChannel).sendError(any(), any(Exception.class));

            FlightTransportChannel transportChannel = mock(FlightTransportChannel.class);
            CountDownLatch released = new CountDownLatch(1);
            doAnswer(invocation -> {
                released.countDown();
                return null;
            }).when(transportChannel).releaseChannel(true);

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
                new TestArrowResponse(producerRoot),
                false,
                false
            );

            // A failed sendError must not prevent release or listener notification.
            assertTrue("channel must still be released after sendError fails", released.await(5, TimeUnit.SECONDS));
            assertTrue("listener must still be notified after sendError fails", notified.await(5, TimeUnit.SECONDS));
            verify(transportChannel).releaseChannel(true);
            producerRoot.close();
        }
    }

    /**
     * Null-{@code transportChannel} guard: {@code failStream} must skip {@code releaseChannel} when
     * there is no transport channel (mirrors {@code BatchTask#close}'s own null guard) and still
     * fail the stream + notify the listener without NPEing. Exercises the {@code false} branch of
     * the {@code task.transportChannel() != null} guard in {@code failStream}.
     */
    public void testProcessBatchTaskFailureWithNullTransportChannelDoesNotThrow() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot producerRoot = newSingleIntRoot(allocator, 23);

            doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendArrowBatch(any(), any(VectorSchemaRoot.class), any());

            CountDownLatch notified = new CountDownLatch(1);
            doAnswer(invocation -> {
                notified.countDown();
                return null;
            }).when(mockListener).onResponseSent(anyLong(), anyString(), any(Exception.class));

            // No transport channel: failStream must not attempt releaseChannel and must not NPE.
            handler.sendResponseBatch(
                Version.CURRENT,
                Collections.emptySet(),
                mockFlightChannel,
                null,
                1L,
                "test-action",
                new TestArrowResponse(producerRoot),
                false,
                false
            );

            assertTrue("stream must still be failed and the listener notified", notified.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            producerRoot.close();
        }
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
