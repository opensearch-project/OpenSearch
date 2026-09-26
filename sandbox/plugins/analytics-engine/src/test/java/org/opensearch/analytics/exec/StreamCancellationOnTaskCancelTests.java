/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reproduces the wedged-query defect: a drain blocked in {@code stream.nextResponse()} must be released
 * when the query task is cancelled.
 *
 * <p>The read has no deadline, and the drain loop's own {@code stream.cancel(...)} sits in a
 * {@code finally} that the blocked thread never reaches, while stage cancellation only flips state
 * flags. So unless dispatch registers the stream against the task, cancelling the query leaves the
 * thread parked and the task live until the node is restarted.
 *
 * <p>Without the fix this test times out on the join below rather than failing fast — which is exactly
 * the production symptom.
 */
public class StreamCancellationOnTaskCancelTests extends OpenSearchTestCase {

    public void testTaskCancellationReleasesBlockedStreamRead() throws Exception {
        AnalyticsQueryTask task = newTask();

        // A producer that never sends. The read parks; only cancel() releases it, and then it throws,
        // just as a cancelled Flight stream makes a parked next() throw.
        CountDownLatch readParked = new CountDownLatch(1);
        CountDownLatch readReleased = new CountDownLatch(1);

        @SuppressWarnings("unchecked")
        StreamTransportResponse<FragmentExecutionArrowResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenAnswer(inv -> {
            readParked.countDown();
            if (readReleased.await(30, TimeUnit.SECONDS) == false) {
                throw new AssertionError("read was never released — task cancellation did not reach the stream");
            }
            throw new RuntimeException("stream cancelled");
        });
        doAnswer(inv -> {
            readReleased.countDown();
            return null;
        }).when(stream).cancelStreamOnly(anyString());

        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch drainFinished = new CountDownLatch(1);
        TransportResponseHandler<FragmentExecutionArrowResponse> handler = captureHandler(task, new StreamingResponseListener<>() {
            @Override
            public boolean onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });
        // The transport hands the stream over before opening it; that is where cancellation binds.
        handler.onStreamCreated(stream);

        Thread drain = new Thread(() -> {
            try {
                handler.handleStreamResponse(stream);
            } finally {
                drainFinished.countDown();
            }
        }, "drain");
        drain.setDaemon(true);
        drain.start();

        assertTrue("drain should have reached the blocking read", readParked.await(10, TimeUnit.SECONDS));
        assertEquals("drain must still be blocked before cancellation", 1, drainFinished.getCount());

        task.cancel("test cancel");

        assertTrue(
            "cancelling the query task must cancel the in-flight stream so the blocked read returns; "
                + "otherwise the query leaks a parked thread and a live task until the node restarts",
            drainFinished.await(10, TimeUnit.SECONDS)
        );
        // The hook released the read with cancelStreamOnly; the drain's own abnormal-exit finally then
        // cancel()s — on the drain's thread, where closing the stream is safe.
        verify(stream, atLeastOnce()).cancelStreamOnly(anyString());
        verify(stream, atLeastOnce()).cancel(anyString(), any());
        assertNotNull("the released read must surface as a query failure", failure.get());
        drain.join(TimeUnit.SECONDS.toMillis(5));
    }

    /**
     * The failure the wedged-query reports actually show: the producer stalls before its first batch,
     * so the transport's prefetch parks and {@code handleStreamResponse} is never invoked. Cancellation
     * must still reach that stream — a hook bound from the consumer callback would not exist yet.
     */
    public void testTaskCancellationCancelsStreamThatNeverReachedTheConsumer() {
        AnalyticsQueryTask task = newTask();

        @SuppressWarnings("unchecked")
        StreamTransportResponse<FragmentExecutionArrowResponse> stream = mock(StreamTransportResponse.class);

        // Dispatch happened and the transport created the stream, but the first batch never arrived,
        // so the drain loop never started.
        captureHandler(task, noopListener()).onStreamCreated(stream);

        task.cancel("test cancel");

        verify(stream).cancelStreamOnly(anyString());
    }

    /**
     * The hook runs on whichever thread cancelled the task, while the drain may be mid-batch inside
     * {@code nextResponse()}. It must therefore only signal cancellation: {@code cancel} closes the
     * stream, freeing the Arrow root that reader is copying out of, and the consumer owns
     * {@code close()}.
     */
    public void testTaskCancellationDoesNotCloseTheStreamUnderTheReader() throws Exception {
        AnalyticsQueryTask task = newTask();

        @SuppressWarnings("unchecked")
        StreamTransportResponse<FragmentExecutionArrowResponse> stream = mock(StreamTransportResponse.class);
        captureHandler(task, noopListener()).onStreamCreated(stream);

        task.cancel("test cancel");

        // Asserted first so the two never-verifications below cannot pass vacuously on a hook that
        // was never registered.
        verify(stream).cancelStreamOnly(anyString());
        verify(stream, never()).cancel(anyString(), any());
        verify(stream, never()).close();
    }

    /**
     * A stream that drained cleanly is already closed, so the hook must have been deregistered: a later
     * task cancellation must not touch it, and the registration must not accumulate for the lifetime of
     * a query that opens hundreds of streams.
     */
    public void testDrainedStreamIsNotCancelledByALaterTaskCancel() throws Exception {
        AnalyticsQueryTask task = newTask();

        FragmentExecutionArrowResponse batch = mock(FragmentExecutionArrowResponse.class);
        @SuppressWarnings("unchecked")
        StreamTransportResponse<FragmentExecutionArrowResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenReturn(batch, (FragmentExecutionArrowResponse) null);

        TransportResponseHandler<FragmentExecutionArrowResponse> handler = captureHandler(task, noopListener());
        handler.onStreamCreated(stream);
        handler.handleStreamResponse(stream);
        verify(stream).close();

        task.cancel("test cancel");

        verify(stream, never()).cancelStreamOnly(anyString());
    }

    private static AnalyticsQueryTask newTask() {
        return new AnalyticsQueryTask(
            1L,
            "transport",
            FragmentExecutionAction.NAME,
            "query-1",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            TimeValue.timeValueMinutes(1)
        );
    }

    private static StreamingResponseListener<FragmentExecutionArrowResponse> noopListener() {
        return new StreamingResponseListener<>() {
            @Override
            public boolean onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                return true;
            }

            @Override
            public void onFailure(Exception e) {}
        };
    }

    /** Builds the service and captures the streaming handler dispatch would have sent, for the given task. */
    private TransportResponseHandler<FragmentExecutionArrowResponse> captureHandler(
        Task parentTask,
        StreamingResponseListener<FragmentExecutionArrowResponse> listener
    ) {
        StreamTransportService transportService = mock(StreamTransportService.class);
        AnalyticsSearchTransportService service = new AnalyticsSearchTransportService(
            transportService,
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(AnalyticsSearchService.class),
            mock(IndicesService.class),
            mock(TaskResourceTrackingService.class)
        );

        @SuppressWarnings("unchecked")
        ArgumentCaptor<TransportResponseHandler<FragmentExecutionArrowResponse>> handlerCaptor = ArgumentCaptor.forClass(
            TransportResponseHandler.class
        );
        doAnswer(inv -> null).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                eq(FragmentExecutionAction.NAME),
                any(),
                any(),
                any(),
                handlerCaptor.capture()
            );

        DiscoveryNode target = mock(DiscoveryNode.class);
        when(target.getId()).thenReturn("node-1");
        when(transportService.getConnection(target)).thenReturn(mock(Transport.Connection.class));
        service.dispatchFragmentStreaming(
            mock(FragmentExecutionRequest.class),
            target,
            listener,
            parentTask,
            new PendingExecutions(1),
            null
        );

        List<TransportResponseHandler<FragmentExecutionArrowResponse>> handlers = handlerCaptor.getAllValues();
        assertFalse("handler must have been dispatched to sendChildRequest", handlers.isEmpty());
        return handlers.get(handlers.size() - 1);
    }
}
