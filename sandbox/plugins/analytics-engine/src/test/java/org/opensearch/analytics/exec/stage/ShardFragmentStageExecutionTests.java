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
import org.opensearch.analytics.exec.stage.shard.ShardFragmentStageExecution;
import org.opensearch.analytics.exec.stage.shard.ShardStageTask;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.NodeDisconnectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
        scheduleAndDispatch(exec);

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
     * Fast-fail contract: the stage transitions to FAILED on the first failing task
     * without waiting for sibling tasks to terminate. Subsequent terminals on the
     * already-failed stage are safe no-ops; the originally captured failure is retained.
     */
    public void testFastFailsOnFirstTaskFailureWithoutWaitingForSiblings() {
        CapturingSink sink = new CapturingSink();
        ShardFragmentStageExecution exec = buildExecutionWithTargets(sink, 3);
        exec.start();

        assertEquals("setup: stage transitions to RUNNING", StageExecution.State.RUNNING, exec.getState());
        assertEquals("setup: one task per target", 3, exec.tasks().size());

        RuntimeException injected = new RuntimeException("first task failure");
        exec.onTaskTerminal(exec.tasks().get(0), injected);

        assertEquals("stage must fail-fast on first task failure", StageExecution.State.FAILED, exec.getState());
        assertSame("captured failure must be the original cause", injected, exec.getFailure());

        // Later terminals (success or failure) are safe no-ops; the stage stays FAILED with
        // its original cause. This guarantees an in-flight task's eventual callback can't
        // overwrite the captured failure or trigger a spurious transition.
        exec.onTaskTerminal(exec.tasks().get(1), null);
        exec.onTaskTerminal(exec.tasks().get(2), new RuntimeException("late second failure"));
        assertEquals("stage stays FAILED across late terminals", StageExecution.State.FAILED, exec.getState());
        assertSame("original failure cause is retained", injected, exec.getFailure());
    }

    /**
     * Verifies that on the happy path, batches are fed into the sink normally.
     */
    public void testArrowResponseFedToSinkOnHappyPath() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();

        ShardFragmentStageExecution exec = buildExecution(sink, capturedListener);
        scheduleAndDispatch(exec);

        VectorSchemaRoot root = createTestBatch(3);
        FragmentExecutionArrowResponse response = new FragmentExecutionArrowResponse(root);
        capturedListener.get().onStreamResponse(response, true);

        assertEquals("sink should have received the batch", 1, sink.fed.size());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    /**
     * When the downstream consumer is satisfied (e.g. a LimitExec above the reduce finished and
     * dropped this input's receiver), the shard listener must stop reading: it feeds the in-hand
     * batch, settles its task as success, and returns {@code false} so the transport drain loop
     * cancels the stream instead of scanning to exhaustion. The non-last flag proves we stop early.
     */
    public void testStreamStopsAndTaskSucceedsWhenConsumerDone() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();
        sink.consumerDone = true; // consumer already satisfied before this batch arrives

        ShardFragmentStageExecution exec = buildExecution(sink, capturedListener);
        scheduleAndDispatch(exec);
        assertNotNull("listener should have been captured by dispatch", capturedListener.get());

        VectorSchemaRoot root = createTestBatch(3);
        FragmentExecutionArrowResponse response = new FragmentExecutionArrowResponse(root);
        // isLast=false: there are more batches upstream, but the consumer is done — we stop anyway.
        boolean keepReading = capturedListener.get().onStreamResponse(response, false);

        assertFalse("listener must signal stop when the consumer is done", keepReading);
        assertEquals("the in-hand batch is still fed before stopping", 1, sink.fed.size());
        assertEquals("task completes as SUCCESS (not cancelled/failed)", StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    /**
     * Mirrors {@code QueryExecution.scheduleStage} for unit-test purposes — calls
     * start() to materialise + transition, then iterates the stage's tasks via its
     * dispatcher with a scheduler-side listener. The real QueryExecution does the
     * same work; replicating it here lets us exercise stage behavior without wiring
     * a full QueryExecution + ExecutionGraph in the test.
     */
    private static void scheduleAndDispatch(ShardFragmentStageExecution exec) {
        exec.start();
        @SuppressWarnings("unchecked")
        TaskRunner<StageTask> dispatcher = (TaskRunner<StageTask>) exec.taskRunner();
        if (dispatcher == null) return;
        for (StageTask task : exec.tasks()) {
            task.transitionTo(StageTaskState.RUNNING);
            dispatcher.run(task, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    task.transitionTo(StageTaskState.FINISHED);
                    exec.onTaskTerminal(task, null);
                }

                @Override
                public void onFailure(Exception cause) {
                    task.transitionTo(StageTaskState.FAILED);
                    exec.onTaskTerminal(task, cause);
                }
            });
        }
    }

    /**
     * Retry on shard failure must advance to the next replica copy in the shard iterator,
     * mirroring {@code AbstractSearchAsyncAction.onShardFailure → FailAwareWeightedRouting.findNext}.
     * Returned task preserves the original {@link StageTaskId} slot so the scheduler treats it
     * as a retry of the same partition.
     */
    public void testRetargetForRetryAdvancesToNextReplica() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        DiscoveryNode primaryNode = mock(DiscoveryNode.class);
        when(primaryNode.getId()).thenReturn("node-primary");
        DiscoveryNode replicaNode = mock(DiscoveryNode.class);
        when(replicaNode.getId()).thenReturn("node-replica");

        ShardRouting primaryRouting = mock(ShardRouting.class);
        when(primaryRouting.currentNodeId()).thenReturn("node-primary");
        when(primaryRouting.shardId()).thenReturn(shardId);
        ShardRouting replicaRouting = mock(ShardRouting.class);
        when(replicaRouting.currentNodeId()).thenReturn("node-replica");
        when(replicaRouting.shardId()).thenReturn(shardId);

        ShardIterator shardIt = mock(ShardIterator.class);
        // Iterator state: primary already consumed at resolve() time, replica is next.
        when(shardIt.nextOrNull()).thenReturn(replicaRouting).thenReturn(null);

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterState.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(nodes.get("node-primary")).thenReturn(primaryNode);
        when(nodes.get("node-replica")).thenReturn(replicaNode);

        ShardExecutionTarget primaryTarget = new ShardExecutionTarget(primaryNode, shardId, 0, shardIt, clusterState);
        ShardStageTask failed = new ShardStageTask(new StageTaskId(0, 0), primaryTarget);
        ShardFragmentStageExecution exec = buildExecutionWithTargets(new CapturingSink(), 1);

        Optional<StageTask> retry = exec.retargetForRetry(failed, new NodeDisconnectedException(primaryNode, "test"));

        assertTrue("retry must be present when replica copies remain", retry.isPresent());
        assertTrue("retry must be a ShardStageTask", retry.get() instanceof ShardStageTask);
        ShardStageTask retriedTask = (ShardStageTask) retry.get();
        assertEquals("retry preserves the StageTaskId slot", failed.id(), retriedTask.id());
        assertTrue("retry target must be a ShardExecutionTarget", retriedTask.target() instanceof ShardExecutionTarget);
        ShardExecutionTarget retriedTarget = (ShardExecutionTarget) retriedTask.target();
        assertEquals("retry must dispatch to the next copy (replica)", "node-replica", retriedTarget.node().getId());
        assertEquals("retry preserves the shardId", shardId, retriedTarget.shardId());
    }

    /**
     * When the shard iterator has no more copies, retargetForRetry returns empty and the
     * scheduler propagates the original cause via {@code onTaskTerminal}. Matches the
     * search API's {@code lastShard} branch in {@code AbstractSearchAsyncAction.onShardFailure}.
     */
    public void testRetargetForRetryReturnsEmptyWhenCopiesExhausted() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        DiscoveryNode primaryNode = mock(DiscoveryNode.class);
        when(primaryNode.getId()).thenReturn("node-primary");

        ShardIterator shardIt = mock(ShardIterator.class);
        when(shardIt.nextOrNull()).thenReturn(null);  // primary already consumed; no replicas
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(mock(DiscoveryNodes.class));
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterState.metadata()).thenReturn(Metadata.EMPTY_METADATA);

        ShardExecutionTarget singletonTarget = new ShardExecutionTarget(primaryNode, shardId, 0, shardIt, clusterState);
        ShardStageTask failed = new ShardStageTask(new StageTaskId(0, 0), singletonTarget);
        ShardFragmentStageExecution exec = buildExecutionWithTargets(new CapturingSink(), 1);

        Optional<StageTask> retry = exec.retargetForRetry(failed, new NodeDisconnectedException(primaryNode, "test"));

        assertFalse("retry must be empty when no further copies exist", retry.isPresent());
    }

    /**
     * Defensive: a target constructed without iterator state (e.g. by older callers / unit
     * tests) returns empty rather than NPE. Preserves the legacy {@code ShardExecutionTarget(node, shardId)}
     * constructor's "no replica failover" semantics.
     */
    public void testRetargetForRetryReturnsEmptyWhenTargetCarriesNoIterator() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-only");

        ShardExecutionTarget legacyTarget = new ShardExecutionTarget(node, shardId, 0);  // no iterator wired
        ShardStageTask failed = new ShardStageTask(new StageTaskId(0, 0), legacyTarget);
        ShardFragmentStageExecution exec = buildExecutionWithTargets(new CapturingSink(), 1);

        Optional<StageTask> retry = exec.retargetForRetry(failed, new NodeDisconnectedException(node, "test"));

        assertFalse("retry must be empty for legacy targets without iterator state", retry.isPresent());
    }

    /**
     * Defensive type check: retargetForRetry must not crash if the scheduler hands back a
     * task that isn't a {@code ShardStageTask}. Returns empty so the scheduler treats it
     * as a non-retryable terminal.
     */
    public void testRetargetForRetryReturnsEmptyForNonShardStageTask() {
        StageTask nonShardTask = new StageTask(new StageTaskId(0, 0)) {
        };
        ShardFragmentStageExecution exec = buildExecutionWithTargets(new CapturingSink(), 1);

        Optional<StageTask> retry = exec.retargetForRetry(nonShardTask, new NodeDisconnectedException(mock(DiscoveryNode.class), "test"));

        assertFalse("non-ShardStageTask must return empty", retry.isPresent());
    }

    /**
     * The iterator yields a routing whose node is no longer present in the cluster state
     * (e.g. the node left between scheduling and retry). {@code nextCopy()} returns null in
     * that case, mirroring {@code ShardTargetResolver.resolve}'s initial filter. The
     * scheduler treats the task as terminal.
     */
    public void testRetargetForRetryReturnsEmptyWhenNextCopyNodeMissingFromClusterState() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        DiscoveryNode primaryNode = mock(DiscoveryNode.class);
        when(primaryNode.getId()).thenReturn("node-primary");

        ShardRouting orphanReplicaRouting = mock(ShardRouting.class);
        when(orphanReplicaRouting.currentNodeId()).thenReturn("node-gone");
        when(orphanReplicaRouting.shardId()).thenReturn(shardId);
        ShardIterator shardIt = mock(ShardIterator.class);
        when(shardIt.nextOrNull()).thenReturn(orphanReplicaRouting).thenReturn(null);

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterState.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(nodes.get("node-gone")).thenReturn(null);  // node left the cluster

        ShardExecutionTarget target = new ShardExecutionTarget(primaryNode, shardId, 0, shardIt, clusterState);
        ShardStageTask failed = new ShardStageTask(new StageTaskId(0, 0), target);
        ShardFragmentStageExecution exec = buildExecutionWithTargets(new CapturingSink(), 1);

        Optional<StageTask> retry = exec.retargetForRetry(failed, new NodeDisconnectedException(primaryNode, "test"));

        assertFalse("retry must be empty when the iterator's next copy points at a vanished node", retry.isPresent());
    }

    /**
     * Walk the iterator across multiple consecutive failures. The {@link StageTaskId} slot
     * is preserved at each retry; each call advances exactly one copy; the chain terminates
     * cleanly with an empty optional once exhausted. Pins the contract that drives
     * {@code QueryScheduler.handleFor}'s reuse-this-listener retry loop.
     */
    public void testRetargetForRetryChainAdvancesEachAttemptUntilExhausted() {
        ShardId shardId = new ShardId(new Index("idx", "uuid"), 0);
        DiscoveryNode primaryNode = mock(DiscoveryNode.class);
        when(primaryNode.getId()).thenReturn("node-primary");
        DiscoveryNode replica1 = mock(DiscoveryNode.class);
        when(replica1.getId()).thenReturn("node-replica-1");
        DiscoveryNode replica2 = mock(DiscoveryNode.class);
        when(replica2.getId()).thenReturn("node-replica-2");

        ShardRouting r1 = mock(ShardRouting.class);
        when(r1.currentNodeId()).thenReturn("node-replica-1");
        when(r1.shardId()).thenReturn(shardId);
        ShardRouting r2 = mock(ShardRouting.class);
        when(r2.currentNodeId()).thenReturn("node-replica-2");
        when(r2.shardId()).thenReturn(shardId);
        ShardIterator shardIt = mock(ShardIterator.class);
        // Three copies total: primary already popped at resolve(), two replicas remain.
        when(shardIt.nextOrNull()).thenReturn(r1).thenReturn(r2).thenReturn(null);

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterState.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(nodes.get("node-primary")).thenReturn(primaryNode);
        when(nodes.get("node-replica-1")).thenReturn(replica1);
        when(nodes.get("node-replica-2")).thenReturn(replica2);

        ShardExecutionTarget primaryTarget = new ShardExecutionTarget(primaryNode, shardId, 0, shardIt, clusterState);
        ShardStageTask initial = new ShardStageTask(new StageTaskId(0, 0), primaryTarget);
        ShardFragmentStageExecution exec = buildExecutionWithTargets(new CapturingSink(), 1);

        // Attempt 1 → replica 1
        Optional<StageTask> attempt1 = exec.retargetForRetry(initial, new NodeDisconnectedException(primaryNode, "boom"));
        assertTrue("first retry must be present", attempt1.isPresent());
        ShardStageTask task1 = (ShardStageTask) attempt1.get();
        assertEquals("StageTaskId preserved on attempt 1", initial.id(), task1.id());
        assertEquals("attempt 1 routes to replica 1", "node-replica-1", ((ShardExecutionTarget) task1.target()).node().getId());

        // Attempt 2 → replica 2 (chained off the previous retry task — the iterator advances)
        Optional<StageTask> attempt2 = exec.retargetForRetry(task1, new NodeDisconnectedException(replica1, "boom"));
        assertTrue("second retry must be present", attempt2.isPresent());
        ShardStageTask task2 = (ShardStageTask) attempt2.get();
        assertEquals("StageTaskId preserved on attempt 2", initial.id(), task2.id());
        assertEquals("attempt 2 routes to replica 2", "node-replica-2", ((ShardExecutionTarget) task2.target()).node().getId());

        // Attempt 3 → exhausted
        Optional<StageTask> attempt3 = exec.retargetForRetry(task2, new NodeDisconnectedException(replica2, "boom"));
        assertFalse("third retry must be empty — all copies exhausted", attempt3.isPresent());
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

        return new ShardFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, dispatcher);
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
        return mockStageWithTargets(1);
    }

    /** Mock stage whose resolver returns {@code n} distinct shard targets (one per fake node). */
    private Stage mockStageWithTargets(int n) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(0);
        TargetResolver resolver = mock(TargetResolver.class);
        List<org.opensearch.analytics.planner.dag.ExecutionTarget> targets = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("test-node-" + i);
            targets.add(new ShardExecutionTarget(node, new ShardId("idx", "_na_", i), i));
        }
        when(resolver.resolve(any(ClusterState.class), any())).thenReturn(targets);
        when(stage.getTargetResolver()).thenReturn(resolver);
        return stage;
    }

    /** Builds a stage execution with N tasks; dispatcher is a no-op stub since the test invokes onTaskTerminal directly. */
    private ShardFragmentStageExecution buildExecutionWithTargets(CapturingSink sink, int n) {
        Stage stage = mockStageWithTargets(n);
        QueryContext config = mockQueryContext();
        ClusterService cs = mockClusterService();
        AnalyticsSearchTransportService dispatcher = mock(AnalyticsSearchTransportService.class);
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = t -> new FragmentExecutionRequest(
            "test-query",
            0,
            t.shardId(),
            List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", new byte[0], List.of()))
        );
        return new ShardFragmentStageExecution(stage, config, sink, cs, requestBuilder, dispatcher);
    }

    private QueryContext mockQueryContext() {
        QueryContext config = mock(QueryContext.class);
        when(config.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));
        when(config.maxConcurrentShardRequestsPerNode()).thenReturn(5);
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
        volatile boolean consumerDone = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            fed.add(batch);
        }

        @Override
        public boolean isConsumerDone() {
            return consumerDone;
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
