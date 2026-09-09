/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.exec.canmatch.SortSpec;
import org.opensearch.analytics.exec.canmatch.TopNGate;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Stage-level wiring for the top-N gate: which can-match results set up a gate, which refuse to, and
 * that observing sort keys on the response path leaves the query otherwise unchanged.
 *
 * <p>Lives in the {@code shard} package because the gate accessors are package-private.
 */
public class SortGateWiringTests extends OpenSearchTestCase {

    private static final String SORT_COLUMN = "@timestamp";

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

    // ── what sets up a gate, and what refuses to ──────────────────────────

    /**
     * Bounds in one agreed value domain set up a gate with the spec's limit, and the bounds are retained
     * identity-keyed by the check's own target instances for the dispatch-side lookup.
     */
    public void testShardCheckBoundsSetUpTheGate() {
        SortSpec sortSpec = new SortSpec(SORT_COLUMN, true, 3);
        ShardFragmentStageExecution exec = buildExecution(new CapturingSink(), sortSpec, new AtomicReference<>());

        ExecutionTarget target = target(0);
        ShardSortBounds bounds = millisBounds(10L, 20L);
        exec.setupSortGate(sortSpec, checkResult(target, bounds));

        assertNotNull("bounds in one agreed domain set up the gate", exec.topNGate());
        assertEquals("capacity is the spec's limit", 3, exec.topNGate().capacity());
        assertFalse("a fresh gate cannot eliminate until K keys land", exec.topNGate().isArmed());
        assertEquals("bounds are retained for the dispatch-side lookup", 1, exec.sortBounds().size());
        assertSame("bounds are reachable by the check's own target instance", bounds, exec.sortBounds().get(target));
        assertNull("and not by an equal-but-distinct target", exec.sortBounds().get(target(0)));
    }

    /**
     * Bounds at different scales (millis vs nanos) aren't comparable, so no gate is built and no bounds
     * are published. Getting this wrong costs a wrong result, not just a lost optimisation.
     */
    public void testMixedValueKindsRefuseTheGate() {
        SortSpec sortSpec = new SortSpec(SORT_COLUMN, true, 3);
        ShardFragmentStageExecution exec = buildExecution(new CapturingSink(), sortSpec, new AtomicReference<>());

        ExecutionTarget millisShard = target(0);
        ExecutionTarget nanosShard = target(1);
        Map<ExecutionTarget, ShardSortBounds> bounds = new IdentityHashMap<>();
        bounds.put(millisShard, millisBounds(10L, 20L));
        bounds.put(nanosShard, new ShardSortBounds(10L, 20L, false, ShardSortBounds.VALUE_KIND_INT64_NANOS));
        exec.setupSortGate(sortSpec, new CanMatchPreFilterPhase.ShardCheckResult(List.of(millisShard, nanosShard), bounds));

        assertNull("mixed value kinds must not set up a gate", exec.topNGate());
        assertTrue("and no bounds are published", exec.sortBounds().isEmpty());
    }

    /**
     * The other three un-gated shapes, one per remaining branch of {@code setupSortGate}: no sort spec,
     * no bounds, and a limit past {@code TopNGate.MAX_CAPACITY}.
     */
    public void testUngateableShapesLeaveTheQueryUngated() {
        SortSpec sortSpec = new SortSpec(SORT_COLUMN, true, 3);

        ShardFragmentStageExecution noSpec = buildExecution(new CapturingSink(), null, new AtomicReference<>());
        noSpec.setupSortGate(null, checkResult(target(0), millisBounds(10L, 20L)));
        assertNull("no sort spec, no gate", noSpec.topNGate());

        ShardFragmentStageExecution noBounds = buildExecution(new CapturingSink(), sortSpec, new AtomicReference<>());
        noBounds.setupSortGate(sortSpec, CanMatchPreFilterPhase.ShardCheckResult.keepAll(List.of(target(0))));
        assertNull("no bounds, no gate", noBounds.topNGate());
        assertTrue(noBounds.sortBounds().isEmpty());

        SortSpec oversized = new SortSpec(SORT_COLUMN, true, 1_000_000);
        ShardFragmentStageExecution tooBig = buildExecution(new CapturingSink(), oversized, new AtomicReference<>());
        tooBig.setupSortGate(oversized, checkResult(target(0), millisBounds(10L, 20L)));
        assertNull("a limit past MAX_CAPACITY refuses the gate", tooBig.topNGate());
    }

    // ── the response path with, and without, a gate ───────────────────────

    /** With no gate — most query shapes — the batch is fed unmodified and the stage succeeds. */
    public void testNoGateLeavesTheStreamPathUnchanged() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();
        ShardFragmentStageExecution exec = buildExecution(sink, null, capturedListener);
        scheduleAndDispatch(exec);

        assertNull("no sort spec on the stage means no gate", exec.topNGate());
        assertTrue("and no bounds to consult", exec.sortBounds().isEmpty());

        VectorSchemaRoot root = millisBatch(100L, 98L, 95L);
        capturedListener.get().onStreamResponse(new FragmentExecutionArrowResponse(root), true);

        assertEquals("batch still fed", 1, sink.fed.size());
        assertSame("and fed unmodified", root, sink.fed.get(0));
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    /**
     * With a gate set up, every row of every batch reaches it and the batch is still fed downstream
     * untouched. Observation must precede the feed, since the sink takes ownership of the batch.
     */
    public void testGatedStreamObservesEveryRowAndStillFeedsTheBatch() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();
        SortSpec sortSpec = new SortSpec(SORT_COLUMN, true, 3);
        ShardFragmentStageExecution exec = buildExecution(sink, sortSpec, capturedListener);

        exec.setupSortGate(sortSpec, checkResult(target(0), millisBounds(10L, 100L)));
        scheduleAndDispatch(exec);

        VectorSchemaRoot first = millisBatch(100L, 98L);
        capturedListener.get().onStreamResponse(new FragmentExecutionArrowResponse(first), false);
        assertFalse("two keys is not yet K=3", exec.topNGate().isArmed());

        VectorSchemaRoot second = millisBatch(95L, 12L);
        capturedListener.get().onStreamResponse(new FragmentExecutionArrowResponse(second), true);

        TopNGate gate = exec.topNGate();
        assertTrue("all four keys observed across both batches", gate.isArmed());
        assertEquals("bar is the 3rd-best of {100,98,95,12}", 95L, gate.bottom());
        assertEquals("both batches still fed", 2, sink.fed.size());
        assertSame("and fed unmodified", first, sink.fed.get(0));
        assertSame(second, sink.fed.get(1));
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    /**
     * The sort column is projected away, so there is nothing to observe: retire the gate and leave the
     * query untouched rather than letting a missing-column lookup fail it.
     */
    public void testAbsentSortColumnDisablesTheGateWithoutAffectingTheQuery() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();
        SortSpec sortSpec = new SortSpec(SORT_COLUMN, true, 1);
        ShardFragmentStageExecution exec = buildExecution(sink, sortSpec, capturedListener);

        exec.setupSortGate(sortSpec, checkResult(target(0), millisBounds(10L, 100L)));
        scheduleAndDispatch(exec);
        capturedListener.get().onStreamResponse(new FragmentExecutionArrowResponse(intBatch(1, 2, 3)), true);

        assertTrue("a missing sort column retires the gate", exec.topNGate().isDisabled());
        assertFalse("a retired gate eliminates nothing", exec.topNGate().canEliminate(millisBounds(0L, 1L)));
        assertEquals("the query is unaffected: batch fed", 1, sink.fed.size());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    /**
     * The sort column arrives as a type the feeder can't read: nothing is observed, so the gate stays
     * unarmed and eliminates nothing — fail-open without retiring.
     */
    public void testUnreadableVectorTypeLeavesTheGateUnarmedButTheQueryIntact() {
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> capturedListener = new AtomicReference<>();
        CapturingSink sink = new CapturingSink();
        SortSpec sortSpec = new SortSpec(SORT_COLUMN, true, 1);
        ShardFragmentStageExecution exec = buildExecution(sink, sortSpec, capturedListener);

        exec.setupSortGate(sortSpec, checkResult(target(0), millisBounds(10L, 100L)));
        scheduleAndDispatch(exec);
        capturedListener.get().onStreamResponse(new FragmentExecutionArrowResponse(varCharBatch("not a timestamp")), true);

        assertFalse("a type the feeder can't read is not a reason to retire", exec.topNGate().isDisabled());
        assertFalse("but nothing was observed, so nothing can be eliminated", exec.topNGate().isArmed());
        assertEquals("the query is unaffected", 1, sink.fed.size());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        sink.close();
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static ShardSortBounds millisBounds(long min, long max) {
        return new ShardSortBounds(min, max, false, ShardSortBounds.VALUE_KIND_INT64_MILLIS);
    }

    /**
     * A shard target on its own fake node. Never call inside a {@code when(...)} argument — it stubs a
     * mock, and Mockito reads a nested stubbing as an unfinished one.
     */
    private static ExecutionTarget target(int ordinal) {
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("test-node-" + ordinal);
        return new ShardExecutionTarget(node, new ShardId("idx", "_na_", ordinal), ordinal);
    }

    private static CanMatchPreFilterPhase.ShardCheckResult checkResult(ExecutionTarget target, ShardSortBounds bounds) {
        Map<ExecutionTarget, ShardSortBounds> byTarget = new IdentityHashMap<>();
        byTarget.put(target, bounds);
        return new CanMatchPreFilterPhase.ShardCheckResult(List.of(target), byTarget);
    }

    /** MILLISECOND-typed {@code @timestamp} column — what a gated query sorts on. */
    private VectorSchemaRoot millisBatch(long... values) {
        Field field = new Field(SORT_COLUMN, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator);
        TimeStampMilliVector vector = (TimeStampMilliVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /** Same column name, a type {@code TopNGate.readInto} has no arm for. */
    private VectorSchemaRoot varCharBatch(String... values) {
        Field field = new Field(SORT_COLUMN, FieldType.nullable(new ArrowType.Utf8()), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator);
        VarCharVector vector = (VarCharVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /** A batch with no {@code @timestamp} column at all — the projected-away shape. */
    private VectorSchemaRoot intBatch(int... values) {
        Field field = new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator);
        IntVector vector = (IntVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /**
     * Stage execution over one shard target, with {@code sortSpec} on the stage (null for the un-gated
     * shape) and the streaming listener captured for the test to drive.
     */
    private ShardFragmentStageExecution buildExecution(
        CapturingSink sink,
        SortSpec sortSpec,
        AtomicReference<StreamingResponseListener<FragmentExecutionArrowResponse>> listenerCapture
    ) {
        // Built before any when(...) — target() stubs a mock, which can't be nested in a stubbing.
        List<ExecutionTarget> resolved = List.of(target(0));
        AnalyticsQueryTask parentTask = mock(AnalyticsQueryTask.class);
        ClusterState clusterState = mock(ClusterState.class);

        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(0);
        when(stage.getSortSpec()).thenReturn(sortSpec);
        TargetResolver resolver = mock(TargetResolver.class);
        when(resolver.resolve(any(ClusterState.class), any())).thenReturn(resolved);
        when(stage.getTargetResolver()).thenReturn(resolver);

        QueryContext config = mock(QueryContext.class);
        when(config.parentTask()).thenReturn(parentTask);
        when(config.maxConcurrentShardRequestsPerNode()).thenReturn(5);
        when(config.bufferAllocator()).thenReturn(allocator);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        AnalyticsSearchTransportService dispatcher = mock(AnalyticsSearchTransportService.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            StreamingResponseListener<FragmentExecutionArrowResponse> listener = (StreamingResponseListener<
                FragmentExecutionArrowResponse>) invocation.getArgument(2);
            listenerCapture.set(listener);
            return null;
        }).when(dispatcher).dispatchFragmentStreaming(any(), any(), any(), any(), any(), any());

        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = t -> new FragmentExecutionRequest(
            "test-query",
            0,
            t.shardId(),
            List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", new byte[0], List.of()))
        );
        return new ShardFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, dispatcher);
    }

    /** Mirrors {@code QueryExecution.scheduleStage}: start, then run each task through the runner. */
    private static void scheduleAndDispatch(ShardFragmentStageExecution exec) {
        exec.start(ActionListener.wrap(v -> {}, e -> {}));
        @SuppressWarnings("unchecked")
        TaskRunner<StageTask> dispatcher = (TaskRunner<StageTask>) exec.taskRunner();
        for (StageTask task : exec.tasks()) {
            task.transitionTo(StageTaskState.RUNNING);
            dispatcher.run(task, ActionListener.wrap(v -> {
                task.transitionTo(StageTaskState.FINISHED);
                exec.onTaskTerminal(task, null);
            }, cause -> {
                task.transitionTo(StageTaskState.FAILED);
                exec.onTaskTerminal(task, cause);
            }));
        }
    }

    private static final class CapturingSink implements ExchangeSink {
        final List<VectorSchemaRoot> fed = new ArrayList<>();

        @Override
        public void feed(VectorSchemaRoot batch) {
            fed.add(batch);
        }

        @Override
        public void close() {
            for (VectorSchemaRoot batch : fed) {
                batch.close();
            }
        }
    }
}
