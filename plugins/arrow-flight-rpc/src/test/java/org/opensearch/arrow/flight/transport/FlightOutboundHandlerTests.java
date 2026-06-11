/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

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
