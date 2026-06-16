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

    public void testProcessBatchTaskNativeArrowFirstBatch() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 42);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            // First batch: streamRoot is null, so it should be created
            when(mockFlightChannel.getRoot()).thenReturn(null);

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();

            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

            doAnswer(invocation -> {
                // Verify the output has a root with transferred data
                VectorStreamOutput out = invocation.getArgument(1);
                VectorSchemaRoot sentRoot = out.getRoot();
                assertNotNull(sentRoot);
                assertEquals(1, sentRoot.getRowCount());
                assertEquals(42, ((IntVector) sentRoot.getVector("val")).get(0));
                // Clean up the stream root created by the handler
                sentRoot.close();
                return null;
            }).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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
            assertNull("No error expected", error.get());
        }
    }

    public void testProcessBatchTaskNativeArrowWithExistingStreamRoot() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));

            // Simulate existing stream root (second batch scenario)
            VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(streamRoot);

            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 99);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            CountDownLatch latch = new CountDownLatch(1);

            doAnswer(invocation -> {
                VectorStreamOutput out = invocation.getArgument(1);
                VectorSchemaRoot sentRoot = out.getRoot();
                // Should reuse the existing stream root
                assertSame(streamRoot, sentRoot);
                assertEquals(1, sentRoot.getRowCount());
                assertEquals(99, ((IntVector) sentRoot.getVector("val")).get(0));
                return null;
            }).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(mockListener).onResponseSent(anyLong(), anyString(), any(TransportResponse.class));

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
            streamRoot.close();
        }
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
     * stream BEFORE notifying the listener. Uses an existing stream root so there is no
     * handler-created root to leak; the test owns and closes it.
     */
    public void testProcessBatchTaskFailureSendsErrorOnStream() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(streamRoot);

            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 7);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            // The batch send fails (e.g. malformed batch / transport error).
            doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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
            streamRoot.close();
        }
    }

    /**
     * First-frame failure variant: when {@code getRoot()} is null (no schema frame sent yet) and
     * the first batch send fails, the handler must still fail the stream. This is the exact shape
     * that hung the consumer — the gRPC client never receives a schema, so {@code getRoot()} blocks
     * forever unless the stream is explicitly failed.
     */
    public void testProcessBatchTaskFirstFrameFailureSendsError() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 1);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            // No schema frame sent yet (first batch). The handler creates a stream root from the
            // producer's allocator, then sendBatch fails before ownership transfers.
            when(mockFlightChannel.getRoot()).thenReturn(null);
            doThrow(new RuntimeException("first-frame send failed")).when(mockFlightChannel)
                .sendBatch(any(), any(VectorStreamOutput.class), any());

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

            assertTrue("first-frame failure must terminate the stream", released.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            verify(transportChannel).releaseChannel(true);
        }
    }

    /**
     * Adopt-then-throw: {@code FlightServerChannel.sendBatch} assigns {@code root = output.getRoot()}
     * BEFORE {@code start()}/{@code putNext()}, so a throw after that assignment leaves the channel
     * owning the handler-created root. {@code failStream} must NOT close it then (the channel owns it
     * and closes it on its own close) — closing it here would free buffers out from under the
     * channel. Simulates adoption by having {@code getRoot()} return the handler-created root once
     * {@code sendBatch} is invoked, then throwing. Asserts via allocator accounting that the root's
     * buffers are STILL allocated after the handler returns (i.e. {@code failStream} left them
     * alone). Without the adoption guard, {@code failStream} would free them and this drops to 0.
     */
    public void testProcessBatchTaskAdoptThenThrowDoesNotCloseAdoptedRoot() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 3);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            // First frame: handler creates a stream root and transfers the producer buffers into it.
            // Simulate the channel adopting that root (as the real sendBatch does at
            // `root = output.getRoot()`) and THEN failing in start()/putNext().
            AtomicReference<VectorSchemaRoot> adopted = new AtomicReference<>();
            when(mockFlightChannel.getRoot()).thenAnswer(invocation -> adopted.get());
            doAnswer(invocation -> {
                VectorStreamOutput out = invocation.getArgument(1);
                adopted.set(out.getRoot()); // channel now owns this root
                throw new RuntimeException("putNext failed after adoption");
            }).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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

            assertTrue("adopt-then-throw must terminate the stream", released.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            // The channel adopted the root, so failStream must have LEFT IT ALLOCATED (the channel
            // owns the close). Without the adoption guard this would have been freed to 0.
            VectorSchemaRoot channelRoot = adopted.get();
            assertNotNull("channel should have adopted the handler-created root", channelRoot);
            assertTrue("adopted root must stay allocated (channel owns it)", allocator.getAllocatedMemory() > 0);
            // Now release it the way the channel would, and confirm everything is accounted for.
            channelRoot.close();
            assertEquals("no buffers should leak after the channel closes its root", 0, allocator.getAllocatedMemory());
        }
    }

    /**
     * {@code FlightRuntimeException} variant of the failure path: when {@code sendBatch} throws a
     * Flight transport error it is caught by the dedicated {@code catch (FlightRuntimeException)}
     * branch, which still fails the stream and then notifies the listener with the mapped
     * {@code StreamException} (via {@code FlightErrorMapper.fromFlightException}) rather than the raw
     * Flight exception.
     */
    public void testProcessBatchTaskFlightRuntimeExceptionFailsStream() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(streamRoot);

            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 11);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            FlightRuntimeException flightError = CallStatus.INTERNAL.withDescription("flight send failed").toRuntimeException();
            doThrow(flightError).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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
            streamRoot.close();
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
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(streamRoot);

            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 13);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            StreamException streamError = new StreamException(StreamErrorCode.RESOURCE_EXHAUSTED, "breaker tripped");
            doThrow(streamError).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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
            streamRoot.close();
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
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(streamRoot);

            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 17);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());
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
            streamRoot.close();
        }
    }

    /**
     * Defensive probe path: if {@code getRoot()} throws while {@code failStream} is resolving whether
     * the channel adopted the handler-created root, adoption is treated as unknown==adopted, so the
     * root is left alone (a rare leak is preferred over freeing a root the channel may still own) and
     * the channel is still released. Exercises the {@code catch (Exception probeFailed)} branch.
     * {@code getRoot()} returns null on the first call (so the handler creates the first-frame root),
     * then throws on the probe call inside {@code failStream}.
     */
    public void testProcessBatchTaskRootProbeFailureTreatsAsAdopted() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 19);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            // First call (in processBatchTask) returns null so the handler creates the first-frame
            // root; the probe call inside failStream then throws.
            when(mockFlightChannel.getRoot()).thenReturn(null).thenThrow(new RuntimeException("getRoot probe failed"));

            // Capture the handler-created root so the test can close it: because the probe throws,
            // failStream treats the root as adopted and deliberately does NOT close it.
            AtomicReference<VectorSchemaRoot> handlerRoot = new AtomicReference<>();
            doAnswer(invocation -> {
                VectorStreamOutput out = invocation.getArgument(1);
                handlerRoot.set(out.getRoot());
                throw new RuntimeException("send failed");
            }).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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

            assertTrue("probe failure must still terminate the stream", released.await(5, TimeUnit.SECONDS));
            verify(mockFlightChannel).sendError(any(), any(Exception.class));
            verify(transportChannel).releaseChannel(true);
            // Probe threw -> treated as adopted -> root left allocated (not closed by failStream).
            VectorSchemaRoot leftAlone = handlerRoot.get();
            assertNotNull("handler should have created a first-frame root", leftAlone);
            assertTrue("root must be left allocated when adoption is unknown", allocator.getAllocatedMemory() > 0);
            // The test owns the close in this case to balance the allocator.
            leftAlone.close();
            assertEquals("no buffers should leak after the test closes the root", 0, allocator.getAllocatedMemory());
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
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
            VectorSchemaRoot streamRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(streamRoot);

            VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
            IntVector vec = (IntVector) producerRoot.getVector("val");
            vec.allocateNew();
            vec.setSafe(0, 23);
            vec.setValueCount(1);
            producerRoot.setRowCount(1);

            doThrow(new RuntimeException("send failed")).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class), any());

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
            streamRoot.close();
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

    // --- Test helper ---

    static class TestArrowResponse extends ArrowBatchResponse {
        TestArrowResponse(VectorSchemaRoot root) {
            super(root);
        }

        TestArrowResponse(StreamInput in) throws IOException {
            super(in);
        }
    }
}
