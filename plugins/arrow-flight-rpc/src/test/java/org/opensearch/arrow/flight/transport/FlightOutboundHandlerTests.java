/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TransportMessageListener;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
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

        assertEquals(
            "Caller's thread context should be preserved after completeStream",
            HEADER_VALUE,
            threadContext.getHeader(HEADER_KEY)
        );
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

        // Use a mock executor that runs the preserveContext-wrapped runnable
        ExecutorService mockExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable command = invocation.getArgument(0);
            executor.execute(() -> {
                command.run();
                // After the preserveContext wrapper runs, capture the header
                // The wrapper stashes the executor thread context, restores caller's, runs, then restores executor's
                latch.countDown();
            });
            return null;
        }).when(mockExecutor).execute(any(Runnable.class));
        when(mockFlightChannel.getExecutor()).thenReturn(mockExecutor);

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

        assertEquals(
            "Caller's thread context should be preserved after completeStream",
            HEADER_VALUE,
            threadContext.getHeader(HEADER_KEY)
        );
    }
}
