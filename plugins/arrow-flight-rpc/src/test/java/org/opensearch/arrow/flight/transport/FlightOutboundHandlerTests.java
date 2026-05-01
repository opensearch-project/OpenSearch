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

            // First batch: sharedRoot is null, so it should be created
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
                // Clean up the shared root created by the handler
                sentRoot.close();
                return null;
            }).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class));

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

    public void testProcessBatchTaskNativeArrowWithExistingSharedRoot() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));

            // Simulate existing shared root (second batch scenario)
            VectorSchemaRoot sharedRoot = VectorSchemaRoot.create(schema, allocator);
            when(mockFlightChannel.getRoot()).thenReturn(sharedRoot);

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
                // Should reuse the existing shared root
                assertSame(sharedRoot, sentRoot);
                assertEquals(1, sentRoot.getRowCount());
                assertEquals(99, ((IntVector) sentRoot.getVector("val")).get(0));
                return null;
            }).when(mockFlightChannel).sendBatch(any(), any(VectorStreamOutput.class));

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
            sharedRoot.close();
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
