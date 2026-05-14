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
import org.opensearch.analytics.exec.stage.RowResponseCodec;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
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

        ShardFragmentStageExecution exec = buildExecution(sink, capturedListener);
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

        ShardFragmentStageExecution exec = buildExecution(sink, capturedListener);
        exec.start();

        VectorSchemaRoot root = createTestBatch(3);
        FragmentExecutionArrowResponse response = new FragmentExecutionArrowResponse(root);
        capturedListener.get().onStreamResponse(response, true);

        assertEquals("sink should have received the batch", 1, sink.fed.size());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private ShardFragmentStageExecution buildExecution(
        CapturingSink sink,
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> listenerCapture
    ) {
        Stage stage = mockStage();
        QueryContext config = mockQueryContext();
        ClusterService clusterService = mockClusterService();
        AnalyticsSearchTransportService dispatcher = mock(AnalyticsSearchTransportService.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            StreamingResponseListener<FragmentExecutionArrowResponse> listener = (StreamingResponseListener<
                FragmentExecutionArrowResponse>) invocation.getArgument(2);
            listenerCapture.set(listener);
            return null;
        }).when(dispatcher).dispatchFragmentStreaming(any(), any(), any(), any(), any());

        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            "test-query",
            0,
            target.shardId(),
            List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", new byte[0], List.of()))
        );

        return new ShardFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, dispatcher, RowResponseCodec.INSTANCE);
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
        QueryContext config = mock(QueryContext.class);
        when(config.searchExecutor()).thenReturn(Runnable::run);
        when(config.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));
        when(config.maxConcurrentShardRequests()).thenReturn(5);
        when(config.bufferAllocator()).thenReturn(allocator);
        return config;
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
