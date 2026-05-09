/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ShardFragmentStageExecution}, focused on ensuring
 * Arrow resource cleanup on cancellation and terminal state transitions.
 */
public class ShardFragmentStageExecutionTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    /**
     * Verifies that Arrow batches arriving after the stage is cancelled
     * are properly closed (no buffer leak).
     */
    public void testArrowResponseClosedWhenStageAlreadyCancelled() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();

        ShardFragmentStageExecution exec = buildExecution(sink, true, capturedListener);
        exec.start();

        assertNotNull("listener should have been captured by dispatch", capturedListener.get());

        exec.cancel("test");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());

        VectorSchemaRoot root = createTestBatch(5);
        long allocatedBefore = allocator.getAllocatedMemory();
        assertTrue("batch should have allocated memory", allocatedBefore > 0);

        FragmentExecutionArrowResponse response = new FragmentExecutionArrowResponse(root);
        capturedListener.get().onStreamResponse(response, true);

        assertEquals("Arrow buffers must be released after cancellation", 0, allocator.getAllocatedMemory());
        assertTrue("sink should not have received any batch", sink.fed.isEmpty());
    }

    /**
     * Verifies that on the happy path, batches are fed into the sink normally.
     */
    public void testArrowResponseFedToSinkOnHappyPath() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();

        ShardFragmentStageExecution exec = buildExecution(sink, true, capturedListener);
        exec.start();

        VectorSchemaRoot root = createTestBatch(3);
        FragmentExecutionArrowResponse response = new FragmentExecutionArrowResponse(root);
        capturedListener.get().onStreamResponse(response, true);

        assertEquals("sink should have received the batch", 1, sink.fed.size());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    /**
     * Verifies that non-Arrow (row codec) responses don't crash
     * the release path on cancellation (they hold no Arrow resources).
     */
    public void testRowResponseSafeOnCancellation() {
        AtomicReference<StreamingResponseListener<FragmentExecutionResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();

        ShardFragmentStageExecution exec = buildExecution(sink, false, capturedListener);
        exec.start();

        exec.cancel("test");

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 42 });
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("col"), rows);
        capturedListener.get().onStreamResponse(response, true);

        assertTrue("sink should not have received anything post-cancel", sink.fed.isEmpty());
    }

    /**
     * Reproduces the lookahead-reordering bug: if the listener offloads
     * {@code onStreamResponse} bodies onto a multi-threaded executor, the
     * {@code isLast=true} task can complete before earlier batches' tasks
     * have started — flipping the stage to SUCCEEDED so the still-queued
     * earlier tasks short-circuit via {@code isDone()} and drop their data.
     *
     * <p>Forces the worst-case schedule deterministically with a manual
     * executor that runs tasks in reverse submission order.
     */
    public void testListenerPreservesBatchesAcrossReorderedExecution() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();
        ManualExecutor manualExecutor = new ManualExecutor();

        ShardFragmentStageExecution exec = buildExecution(sink, true, capturedListener, manualExecutor);
        exec.start();

        FragmentExecutionArrowResponse first = new FragmentExecutionArrowResponse(createTestBatch(2));
        FragmentExecutionArrowResponse last = new FragmentExecutionArrowResponse(createTestBatch(3));

        capturedListener.get().onStreamResponse(first, false);
        capturedListener.get().onStreamResponse(last, true);

        // Force the bug's worst case: isLast=true task runs first, transitions
        // to SUCCEEDED; the earlier task then sees isDone()=true if the
        // implementation is still offloading to the executor.
        manualExecutor.runInReverseOrder();

        assertEquals("both batches must be fed despite reverse-order execution", 2, sink.fed.size());
        sink.close();
    }

    /**
     * Verifies that RowResponseCodec rejects null allocator.
     */
    public void testRowResponseCodecRejectsNullAllocator() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 1 });
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("x"), rows);
        expectThrows(IllegalArgumentException.class, () -> RowResponseCodec.INSTANCE.decode(response, null));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private <T extends ActionResponse> ShardFragmentStageExecution buildExecution(
        CapturingSink sink,
        boolean streaming,
        AtomicReference<StreamingResponseListener<T>> listenerCapture
    ) {
        return buildExecution(sink, streaming, listenerCapture, Runnable::run);
    }

    @SuppressWarnings("unchecked")
    private <T extends ActionResponse> ShardFragmentStageExecution buildExecution(
        CapturingSink sink,
        boolean streaming,
        AtomicReference<StreamingResponseListener<T>> listenerCapture,
        Executor searchExecutor
    ) {
        Stage stage = mockStage();
        QueryContext config = mockQueryContext(searchExecutor);
        ClusterService clusterService = mockClusterService();
        AnalyticsSearchTransportService dispatcher = mock(AnalyticsSearchTransportService.class);
        when(dispatcher.isStreamingEnabled()).thenReturn(streaming);

        if (streaming) {
            doAnswer(invocation -> {
                StreamingResponseListener<T> listener = (StreamingResponseListener<T>) invocation.getArgument(2);
                listenerCapture.set(listener);
                return null;
            }).when(dispatcher).dispatchFragmentStreaming(any(), any(), any(), any(), any());
        } else {
            doAnswer(invocation -> {
                StreamingResponseListener<T> listener = (StreamingResponseListener<T>) invocation.getArgument(2);
                listenerCapture.set(listener);
                return null;
            }).when(dispatcher).dispatchFragment(any(), any(), any(), any(), any());
        }

        ResponseCodec<FragmentExecutionResponse> codec = (resp, alloc) -> {
            VectorSchemaRoot vsr = createTestBatch(resp.getRows().size());
            return vsr;
        };

        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            "test-query",
            0,
            target.shardId(),
            List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", new byte[0], List.of()))
        );

        return new ShardFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, dispatcher, codec);
    }

    private VectorSchemaRoot createTestBatch(int rows) {
        Schema schema = new Schema(List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        IntVector vec = (IntVector) root.getVector(0);
        for (int i = 0; i < rows; i++) {
            vec.setSafe(i, i);
        }
        vec.setValueCount(rows);
        root.setRowCount(rows);
        return root;
    }

    private Stage mockStage() {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(0);
        TargetResolver resolver = mock(TargetResolver.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("test-node-1");
        ShardExecutionTarget target = new ShardExecutionTarget(node, new ShardId("idx", "_na_", 0));
        when(resolver.resolve(any(ClusterState.class), any())).thenReturn(List.of(target));
        when(stage.getTargetResolver()).thenReturn(resolver);
        return stage;
    }

    private QueryContext mockQueryContext() {
        return mockQueryContext(Runnable::run);
    }

    private QueryContext mockQueryContext(Executor searchExecutor) {
        QueryContext config = mock(QueryContext.class);
        when(config.searchExecutor()).thenReturn(searchExecutor);
        when(config.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));
        when(config.maxConcurrentShardRequests()).thenReturn(5);
        when(config.bufferAllocator()).thenReturn(allocator);
        return config;
    }

    /**
     * Test executor that records submitted tasks instead of running them.
     * Run them explicitly in any order to deterministically simulate the
     * worst-case multi-thread completion order on a real pool.
     */
    private static final class ManualExecutor implements Executor {
        private final List<Runnable> queue = new ArrayList<>();

        @Override
        public synchronized void execute(Runnable command) {
            queue.add(command);
        }

        synchronized void runInReverseOrder() {
            for (int i = queue.size() - 1; i >= 0; i--) {
                queue.get(i).run();
            }
            queue.clear();
        }
    }

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));
        return clusterService;
    }

    private static final class CapturingSink implements ExchangeSink {
        final List<VectorSchemaRoot> fed = new ArrayList<>();
        boolean closed = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            fed.add(batch);
        }

        @Override
        public void close() {
            closed = true;
            for (VectorSchemaRoot batch : fed) {
                batch.close();
            }
        }
    }
}
