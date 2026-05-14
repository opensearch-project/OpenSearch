/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.analytics.exec.profile.QueryProfileBuilder;
import org.opensearch.analytics.exec.profile.StageProfile;
import org.opensearch.analytics.exec.profile.TaskProfile;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageStateListener;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryProfileBuilderTests extends OpenSearchTestCase {

    public void testSnapshotCapturesQueryIdAndStageIdsFromEmptyGraph() {
        Stage rootStage = stageWithId(0);
        QueryContext ctx = new QueryContext(new QueryDAG("q-empty", rootStage), Runnable::run, taskStub(), 1, Long.MAX_VALUE);
        StubExecution root = new StubExecution(0);
        ExecutionGraph graph = singleStageGraph("q-empty", root);

        QueryProfile profile = QueryProfileBuilder.snapshot(graph, ctx);

        assertEquals("q-empty", profile.queryId());
        assertEquals(1, profile.stages().size());
        assertEquals(0, profile.stages().get(0).stageId());
        assertEquals("CREATED", profile.stages().get(0).state());
        assertEquals(0L, profile.totalElapsedMs());
    }

    public void testSnapshotComputesElapsedFromMetricsStartEnd() {
        Stage rootStage = stageWithId(0);
        QueryContext ctx = new QueryContext(new QueryDAG("q-timed", rootStage), Runnable::run, taskStub(), 1, Long.MAX_VALUE);
        StubExecution root = new StubExecution(0);
        root.transitionInternal(StageExecution.State.RUNNING); // stamps start
        root.transitionInternal(StageExecution.State.SUCCEEDED); // stamps end
        ExecutionGraph graph = singleStageGraph("q-timed", root);

        QueryProfile profile = QueryProfileBuilder.snapshot(graph, ctx);

        StageProfile stage = profile.stages().get(0);
        assertTrue("start stamped", stage.startMs() > 0);
        assertTrue("end stamped", stage.endMs() > 0);
        assertTrue("elapsed non-negative", stage.elapsedMs() >= 0);
        // Query total spans earliest-to-latest across all stages; single stage == stage elapsed.
        assertEquals(stage.elapsedMs(), profile.totalElapsedMs());
    }

    public void testSnapshotSplitsFullPlanIntoLines() {
        Stage rootStage = stageWithId(0);
        QueryContext ctx = new QueryContext(new QueryDAG("q-plan", rootStage), Runnable::run, taskStub(), 1, Long.MAX_VALUE);
        ExecutionGraph graph = singleStageGraph("q-plan", new StubExecution(0));

        // Calcite's RelOptUtil.toString produces "Node\n child\n" — mimic that.
        QueryProfile profile = QueryProfileBuilder.snapshot(graph, ctx, "Aggregate\n  TableScan\n");

        assertEquals(java.util.List.of("Aggregate", "  TableScan"), profile.fullPlan());
    }

    public void testSnapshotEmptyFullPlanReturnsEmptyList() {
        Stage rootStage = stageWithId(0);
        QueryContext ctx = new QueryContext(new QueryDAG("q-plan", rootStage), Runnable::run, taskStub(), 1, Long.MAX_VALUE);
        ExecutionGraph graph = singleStageGraph("q-plan", new StubExecution(0));

        QueryProfile profile = QueryProfileBuilder.snapshot(graph, ctx, "");

        assertTrue(profile.fullPlan().isEmpty());
    }

    public void testSnapshotCollectsTaskProfilesFromTracker() {
        Stage rootStage = stageWithId(0);
        QueryContext ctx = new QueryContext(new QueryDAG("q-tasks", rootStage), Runnable::run, taskStub(), 1, Long.MAX_VALUE);
        StageTask t0 = new StageTask(new StageTaskId(0, 0), mockTargetWithNode("node_a"));
        StageTask t1 = new StageTask(new StageTaskId(0, 1), mockTargetWithNode("node_b"));
        ctx.taskTracker().register(t0);
        ctx.taskTracker().register(t1);
        t0.transitionTo(StageTaskState.RUNNING);
        t1.transitionTo(StageTaskState.RUNNING);
        t0.transitionTo(StageTaskState.FINISHED);
        t1.transitionTo(StageTaskState.FAILED);

        ExecutionGraph graph = singleStageGraph("q-tasks", new StubExecution(0));
        QueryProfile profile = QueryProfileBuilder.snapshot(graph, ctx);

        List<TaskProfile> tasks = profile.stages().get(0).tasks();
        assertEquals(2, tasks.size());
        // tasksForStage ordering isn't guaranteed — check set membership by partition id.
        TaskProfile p0 = tasks.stream().filter(t -> t.partitionId() == 0).findFirst().orElseThrow();
        TaskProfile p1 = tasks.stream().filter(t -> t.partitionId() == 1).findFirst().orElseThrow();
        assertEquals("FINISHED", p0.state());
        assertEquals("node_a", p0.node());
        assertEquals("FAILED", p1.state());
        assertEquals("node_b", p1.node());
        assertTrue("task start stamped", p0.startMs() > 0);
        assertTrue("task end stamped", p0.endMs() > 0);
    }

    // ─── helpers ────────────────────────────────────────────────────────

    private static Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        when(stage.getExecutionType()).thenReturn(org.opensearch.analytics.planner.dag.StageExecutionType.LOCAL_PASSTHROUGH);
        when(stage.getFragment()).thenReturn(null);
        when(stage.getExchangeInfo()).thenReturn(null);
        return stage;
    }

    private static AnalyticsQueryTask taskStub() {
        return new AnalyticsQueryTask(1L, "transport", "analytics_query", "q-test", TaskId.EMPTY_TASK_ID, Map.of(), null);
    }

    private static ExecutionGraph singleStageGraph(String queryId, StageExecution root) {
        return new ExecutionGraph(queryId, Map.of(root.getStageId(), root), root, List.of(root));
    }

    private static ExecutionTarget mockTargetWithNode(String nodeId) {
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn(nodeId);
        return new TestTarget(node);
    }

    private static final class TestTarget extends ExecutionTarget {
        TestTarget(DiscoveryNode node) {
            super(node);
        }
    }

    /**
     * Minimal {@link StageExecution} that exposes the protected {@code transitionTo} for tests.
     * Mirrors {@code AbstractStageExecution}'s metrics stamping so elapsed math is real.
     */
    private static final class StubExecution implements StageExecution {
        private final int stageId;
        private final StageMetrics metrics = new StageMetrics();
        private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
        private final java.util.List<StageStateListener> listeners = new java.util.ArrayList<>();

        StubExecution(int stageId) {
            this.stageId = stageId;
        }

        @Override
        public int getStageId() {
            return stageId;
        }

        @Override
        public State getState() {
            return state.get();
        }

        @Override
        public StageMetrics getMetrics() {
            return metrics;
        }

        @Override
        public void start() {}

        @Override
        public void addStateListener(StageStateListener listener) {
            listeners.add(listener);
        }

        @Override
        public Exception getFailure() {
            return null;
        }

        @Override
        public boolean failFromChild(Exception cause) {
            return false;
        }

        @Override
        public void cancel(String reason) {}

        void transitionInternal(State target) {
            State prev = state.getAndSet(target);
            if (prev == State.CREATED) metrics.recordStart();
            if (target == State.SUCCEEDED || target == State.FAILED || target == State.CANCELLED) metrics.recordEnd();
        }
    }
}
