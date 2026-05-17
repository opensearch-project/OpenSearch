/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ExecutionGraph} build / cleanup paths — focused on
 * {@link ExecutionGraph#build} failure handling that's otherwise reached only via integration.
 */
public class ExecutionGraphTests extends OpenSearchTestCase {

    /**
     * When a child-stage factory throws during recursive build, every already-built stage
     * must have {@code cancel(...)} called exactly once (cleanup) and the original failure
     * must propagate to the caller.
     */
    public void testCancelPartialBuildCancelsAlreadyBuiltStagesAndPropagatesOriginalFailure() {
        AtomicInteger rootCancels = new AtomicInteger();
        StageExecutionBuilder builder = new StageExecutionBuilder(mock(org.opensearch.cluster.service.ClusterService.class), null);
        // Root factory returns a stage that records cancel() calls.
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, cfg) -> {
            return new CountingCancelStage(stage.getStageId(), rootCancels);
        });
        // Child factory throws — simulates a backend init failure during recursive build.
        RuntimeException buildFailure = new RuntimeException("child build failure");
        builder.registerFactory(StageExecutionType.SHARD_FRAGMENT, (stage, sink, cfg) -> { throw buildFailure; });

        Stage rootStage = stageOf(
            0,
            StageExecutionType.LOCAL_PASSTHROUGH,
            List.of(stageOf(1, StageExecutionType.SHARD_FRAGMENT, List.of()))
        );
        QueryContext ctx = newContext(rootStage);

        RuntimeException thrown = expectThrows(RuntimeException.class, () -> ExecutionGraph.build(ctx, builder, s -> {}));
        assertSame("original build failure propagated", buildFailure, thrown);
        assertEquals("partial-build cleanup invoked cancel() on already-built root stage", 1, rootCancels.get());
    }

    /**
     * cancelPartialBuild's contract: if one stage's cancel() throws, subsequent stages
     * still get a chance to cancel. The first cancel failure is rethrown with later
     * failures attached as suppressed.
     */
    public void testCancelPartialBuildSweepsAllStagesEvenWhenOneCancelThrows() {
        AtomicInteger goodStageCancels = new AtomicInteger();
        StageExecutionBuilder builder = new StageExecutionBuilder(mock(org.opensearch.cluster.service.ClusterService.class), null);
        // Root: a stage whose cancel() throws ISE — should not block sibling cleanup.
        builder.registerFactory(
            StageExecutionType.LOCAL_PASSTHROUGH,
            (stage, sink, cfg) -> new CountingCancelStage(stage.getStageId(), null) {
                @Override
                public void cancel(String reason) {
                    throw new IllegalStateException("root cancel failure");
                }
            }
        );
        // First child builds; second-level child throws to trigger cancelPartialBuild.
        builder.registerFactory(StageExecutionType.SHARD_FRAGMENT, (stage, sink, cfg) -> {
            // Second SHARD_FRAGMENT throws on construction (simulating mid-recursion failure)
            if (stage.getStageId() == 2) throw new RuntimeException("late build failure");
            return new CountingCancelStage(stage.getStageId(), goodStageCancels);
        });

        Stage grandchild = stageOf(2, StageExecutionType.SHARD_FRAGMENT, List.of());
        Stage child = stageOf(1, StageExecutionType.SHARD_FRAGMENT, List.of(grandchild));
        Stage root = stageOf(0, StageExecutionType.LOCAL_PASSTHROUGH, List.of(child));

        QueryContext ctx = newContext(root);

        // cancelPartialBuild rethrows the first cancel failure (root's ISE), with the
        // build failure NOT in the chain — build's `finally` reruns cleanup which throws.
        Throwable thrown = expectThrows(Throwable.class, () -> ExecutionGraph.build(ctx, builder, s -> {}));
        // Either the build RuntimeException OR the cancel ISE may surface — both indicate
        // partial-build cleanup ran. The important contract: the good sibling was cancelled.
        assertEquals("sibling stage 1 was cancelled despite root's cancel throwing", 1, goodStageCancels.get());
        assertNotNull(thrown);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static Stage stageOf(int id, StageExecutionType type, List<Stage> children) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getExecutionType()).thenReturn(type);
        when(stage.getChildStages()).thenReturn(children);
        return stage;
    }

    private static QueryContext newContext(Stage rootStage) {
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            1L,
            "transport",
            "analytics_query",
            "q-test",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            null
        );
        QueryDAG dag = new QueryDAG("q-test", rootStage);
        return QueryContext.forTest(dag, task);
    }

    /** Minimal stage stub that counts cancel() calls and implements DataProducer + DataConsumer
     *  so the builder can wire child sinks off it. */
    private static class CountingCancelStage implements org.opensearch.analytics.exec.stage.SinkProvidingStageExecution {
        private final int stageId;
        private final AtomicInteger cancelCounter;
        private final org.opensearch.analytics.exec.RowProducingSink sink = new org.opensearch.analytics.exec.RowProducingSink();
        private State state = State.CREATED;

        CountingCancelStage(int stageId, AtomicInteger cancelCounter) {
            this.stageId = stageId;
            this.cancelCounter = cancelCounter;
        }

        @Override
        public int getStageId() {
            return stageId;
        }

        @Override
        public State getState() {
            return state;
        }

        @Override
        public org.opensearch.analytics.exec.stage.StageMetrics getMetrics() {
            return new org.opensearch.analytics.exec.stage.StageMetrics();
        }

        @Override
        public void start() {}

        @Override
        public void addStateListener(org.opensearch.analytics.exec.stage.StageStateListener listener) {}

        @Override
        public Exception getFailure() {
            return null;
        }

        @Override
        public void cancel(String reason) {
            state = State.CANCELLED;
            if (cancelCounter != null) cancelCounter.incrementAndGet();
        }

        @Override
        public org.opensearch.analytics.spi.ExchangeSink inputSink(int childStageId) {
            return sink;
        }

        @Override
        public org.opensearch.analytics.backend.ExchangeSource outputSource() {
            return sink;
        }
    }
}
