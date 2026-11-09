/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats;

import org.opensearch.analytics.exec.ExecutionGraph;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AnalyticsStatsCollector}. Feeds synthetic
 * {@link ExecutionGraph}s into {@link AnalyticsStatsCollector#recordExecution} and
 * asserts the rollup at {@link AnalyticsStatsCollector#snapshot} reflects them.
 */
public class AnalyticsStatsCollectorTests extends OpenSearchTestCase {

    public void testRecordsQueryAndPlanningElapsed() {
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        QueryDAG dag = dag(stageOf(0, StageExecutionType.SHARD_FRAGMENT, List.of()));

        c.recordExecution(graphOf(execOf(0, StageExecution.State.SUCCEEDED, 100L, 130L, 0L, List.of())), dag, 15L);
        c.recordExecution(graphOf(execOf(0, StageExecution.State.SUCCEEDED, 200L, 260L, 0L, List.of())), dag, 45L);

        AnalyticsStats.LatencyStats elapsed = c.snapshot().queries().elapsedMs();
        assertEquals(2, elapsed.count());
        assertEquals(90, elapsed.sumMs());

        AnalyticsStats.LatencyStats planning = c.snapshot().queries().planningMs();
        assertEquals(2, planning.count());
        assertEquals(60, planning.sumMs());
    }

    public void testStageBucketsKeyedByExecutionType() {
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        QueryDAG dag = dag(
            stageOf(0, StageExecutionType.COORDINATOR_REDUCE, List.of(stageOf(1, StageExecutionType.SHARD_FRAGMENT, List.of())))
        );
        c.recordExecution(
            graphOf(
                execOf(0, StageExecution.State.SUCCEEDED, 100L, 103L, 50L, List.of()),
                execOf(1, StageExecution.State.SUCCEEDED, 100L, 108L, 200L, List.of())
            ),
            dag,
            10L
        );

        AnalyticsStats.StageBucket shard = c.snapshot().stagesByType().get("SHARD_FRAGMENT");
        assertNotNull(shard);
        assertEquals(1, shard.started());
        assertEquals(1, shard.succeeded());
        assertEquals(0, shard.failed());
        assertEquals(200, shard.rowsProcessedTotal());
        assertEquals(8, shard.elapsedMs().sumMs());

        AnalyticsStats.StageBucket reduce = c.snapshot().stagesByType().get("COORDINATOR_REDUCE");
        assertNotNull(reduce);
        assertEquals(1, reduce.started());
        assertEquals(50, reduce.rowsProcessedTotal());
        assertEquals(3, reduce.elapsedMs().sumMs());
    }

    public void testStageStatesRouteToCorrectCounter() {
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        QueryDAG dag = dag(stageOf(0, StageExecutionType.SHARD_FRAGMENT, List.of()));
        c.recordExecution(
            graphOf(
                execOf(0, StageExecution.State.SUCCEEDED, 100L, 101L, 0L, List.of()),
                execOf(0, StageExecution.State.FAILED, 100L, 101L, 0L, List.of()),
                execOf(0, StageExecution.State.CANCELLED, 100L, 101L, 0L, List.of())
            ),
            dag,
            1L
        );

        AnalyticsStats.StageBucket shard = c.snapshot().stagesByType().get("SHARD_FRAGMENT");
        assertEquals(3, shard.started());
        assertEquals(1, shard.succeeded());
        assertEquals(1, shard.failed());
        assertEquals(1, shard.cancelled());
    }

    public void testFragmentCountersFromTasks() {
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        QueryDAG dag = dag(stageOf(0, StageExecutionType.SHARD_FRAGMENT, List.of()));
        c.recordExecution(
            graphOf(
                execOf(
                    0,
                    StageExecution.State.SUCCEEDED,
                    100L,
                    110L,
                    5L,
                    List.of(
                        taskOf(StageTaskState.FINISHED, 100L, 106L),
                        taskOf(StageTaskState.FINISHED, 100L, 104L),
                        taskOf(StageTaskState.FAILED, 100L, 102L)
                    )
                )
            ),
            dag,
            1L
        );

        AnalyticsStats.Fragments f = c.snapshot().fragments();
        assertEquals(3, f.total());
        assertEquals(2, f.succeeded());
        assertEquals(1, f.failed());
        assertEquals(3, f.elapsedMs().count());
        assertEquals(12, f.elapsedMs().sumMs());
    }

    public void testSnapshotIsCumulative() {
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        QueryDAG dag = dag(stageOf(0, StageExecutionType.SHARD_FRAGMENT, List.of()));

        c.recordExecution(graphOf(execOf(0, StageExecution.State.SUCCEEDED, 100L, 110L, 0L, List.of())), dag, 1L);
        AnalyticsStats first = c.snapshot();
        assertEquals(1, first.queries().elapsedMs().count());

        c.recordExecution(graphOf(execOf(0, StageExecution.State.SUCCEEDED, 200L, 230L, 0L, List.of())), dag, 1L);
        AnalyticsStats second = c.snapshot();
        assertEquals("count cumulative across snapshots", 2, second.queries().elapsedMs().count());
        assertEquals("sum cumulative across snapshots", 40, second.queries().elapsedMs().sumMs());
    }

    public void testNullGraphRecordsOnlyPlanning() {
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        c.recordExecution(null, null, 5L);

        AnalyticsStats stats = c.snapshot();
        assertEquals(1, stats.queries().planningMs().count());
        assertEquals(5, stats.queries().planningMs().sumMs());
        assertEquals(0, stats.queries().elapsedMs().count());
        assertTrue(stats.stagesByType().isEmpty());
        assertEquals(0, stats.fragments().total());
    }

    public void testStageWithUnknownTypeIdRoutesToFallback() {
        // Graph has a stage with id=99 that the DAG doesn't know about — fall back to the
        // StageExecution implementation class name rather than dropping the record.
        AnalyticsStatsCollector c = new AnalyticsStatsCollector();
        QueryDAG dag = dag(stageOf(0, StageExecutionType.SHARD_FRAGMENT, List.of()));
        StageExecution exec = execOf(99, StageExecution.State.SUCCEEDED, 100L, 105L, 0L, List.of());
        c.recordExecution(graphOf(exec), dag, 1L);

        // The default exec mock's getClass().getSimpleName() is the Mockito-generated proxy name;
        // we just assert the bucket is populated rather than asserting on the specific key.
        assertEquals(1, c.snapshot().stagesByType().size());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static Stage stageOf(int id, StageExecutionType type, List<Stage> children) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getExecutionType()).thenReturn(type);
        when(stage.getChildStages()).thenReturn(children);
        return stage;
    }

    private static QueryDAG dag(Stage rootStage) {
        return new QueryDAG("q-test", rootStage);
    }

    private static StageExecution execOf(
        int stageId,
        StageExecution.State state,
        long startMs,
        long endMs,
        long rows,
        List<StageTask> tasks
    ) {
        StageExecution exec = mock(StageExecution.class);
        when(exec.getStageId()).thenReturn(stageId);
        when(exec.getState()).thenReturn(state);
        // StageMetrics records nanoTime internally; mock it so tests can drive exact ms values.
        StageMetrics m = mock(StageMetrics.class);
        when(m.getStartTimeMs()).thenReturn(startMs);
        when(m.getEndTimeMs()).thenReturn(endMs);
        when(m.getRowsProcessed()).thenReturn(rows);
        when(exec.getMetrics()).thenReturn(m);
        when(exec.tasks()).thenReturn(tasks);
        return exec;
    }

    private static StageTask taskOf(StageTaskState state, long startMs, long endMs) {
        StageTask t = mock(StageTask.class);
        when(t.state()).thenReturn(state);
        when(t.startedAtMs()).thenReturn(startMs);
        when(t.finishedAtMs()).thenReturn(endMs);
        return t;
    }

    private static ExecutionGraph graphOf(StageExecution... execs) {
        ExecutionGraph graph = mock(ExecutionGraph.class);
        when(graph.allExecutions()).thenReturn(List.of(execs));
        when(graph.queryId()).thenReturn("q-test");
        return graph;
    }
}
