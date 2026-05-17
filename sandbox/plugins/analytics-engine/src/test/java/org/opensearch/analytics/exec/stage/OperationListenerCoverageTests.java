/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end coverage of {@link AnalyticsOperationListener} coordinator-side callbacks
 * across happy / failure / cancellation paths. Wires a {@link RecordingListener} through
 * {@link QueryContext#operationListeners()}, drives the full {@link QueryScheduler} →
 * {@link AbstractStageExecution} pipeline, and asserts the fired-set + ordering for each
 * lifecycle.
 *
 * <p>Lives in the {@code .stage} package so the test stage can extend the package-private
 * {@link AbstractStageExecution} and exercise its real listener-firing wiring.
 *
 * <p>Out of scope: data-node fragment callbacks ({@code onPreFragmentExecution} etc.) —
 * those need a plugin SPI to inject a fragment listener into {@code AnalyticsSearchService}.
 * See the TODO in {@code AnalyticsOperationListener}.
 */
public class OperationListenerCoverageTests extends OpenSearchTestCase {

    private StageExecutionBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = new StageExecutionBuilder(mock(ClusterService.class), null);
    }

    /**
     * Happy path: {@code onQueryStart → onStageStart → onQuerySuccess → onStageSuccess}.
     *
     * <p>The query terminal callback fires <i>before</i> the stage's own operation callback
     * because {@code AbstractStageExecution.transitionTo} fires state listeners (which include
     * the QueryExecution root-state mirror that drives the query terminal inline) before
     * firing operation listeners. The test pins this order — if the order ever changes,
     * update it explicitly rather than silently.
     */
    public void testListenerHappyPath() {
        RecordingListener recording = new RecordingListener();
        AtomicReference<TestableRootExecution> stageRef = new AtomicReference<>();
        Stage rootStage = stageWithId(0);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, cfg) -> {
            TestableRootExecution exec = new TestableRootExecution(stage, new NoopSink(), cfg);
            stageRef.set(exec);
            return exec;
        });

        executeViaScheduler(rootStage, List.of(recording), ActionListener.wrap(r -> {}, e -> {}));
        stageRef.get().doSucceed();

        assertEquals(List.of("onQueryStart:1", "onStageStart:0", "onQuerySuccess", "onStageSuccess:0"), recording.events);
    }

    /** Failure path: same ordering invariant — query terminal fires inline before stage operation callback. */
    public void testListenerFailurePath() {
        RecordingListener recording = new RecordingListener();
        AtomicReference<TestableRootExecution> stageRef = new AtomicReference<>();
        Stage rootStage = stageWithId(0);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, cfg) -> {
            TestableRootExecution exec = new TestableRootExecution(stage, new NoopSink(), cfg);
            stageRef.set(exec);
            return exec;
        });

        executeViaScheduler(rootStage, List.of(recording), ActionListener.wrap(r -> fail("unexpected success"), e -> {}));
        stageRef.get().doFail(new RuntimeException("stage failed"));

        assertEquals(List.of("onQueryStart:1", "onStageStart:0", "onQueryFailure", "onStageFailure:0"), recording.events);
    }

    /**
     * Cancellation path: stage CANCELLED surfaces as onStageCancelled. The wrapper-listener
     * fires onQueryFailure (cancellation is an abnormal termination from the user-facing
     * listener's perspective) — same query-terminal-before-stage-terminal ordering.
     */
    public void testListenerCancellationPath() {
        RecordingListener recording = new RecordingListener();
        AtomicReference<TestableRootExecution> stageRef = new AtomicReference<>();
        Stage rootStage = stageWithId(0);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, cfg) -> {
            TestableRootExecution exec = new TestableRootExecution(stage, new NoopSink(), cfg);
            stageRef.set(exec);
            return exec;
        });

        executeViaScheduler(rootStage, List.of(recording), ActionListener.wrap(r -> fail("unexpected success"), e -> {}));
        stageRef.get().doCancel();

        assertEquals(List.of("onQueryStart:1", "onStageStart:0", "onQueryFailure", "onStageCancelled:0"), recording.events);
    }

    /**
     * Direct {@code CREATED → terminal} (e.g. empty target resolution skipping RUNNING)
     * still surfaces an {@code onStageStart} synthetically — observers never see a
     * terminal event for a stage they didn't see start. Pins the B1 fix.
     */
    public void testListenerSynthesisesStageStartForDirectCreatedToTerminal() {
        RecordingListener recording = new RecordingListener();
        Stage rootStage = stageWithId(0);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, cfg) -> {
            // Stage whose start() goes CREATED → SUCCEEDED directly (no RUNNING).
            return new TestableRootExecution(stage, new NoopSink(), cfg) {
                @Override
                public void start() {
                    doSucceed();
                }
            };
        });

        executeViaScheduler(rootStage, List.of(recording), ActionListener.wrap(r -> {}, e -> {}));

        assertEquals(List.of("onQueryStart:1", "onQuerySuccess", "onStageStart:0", "onStageSuccess:0"), recording.events);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private void executeViaScheduler(
        Stage rootStage,
        List<AnalyticsOperationListener> listeners,
        ActionListener<Iterable<VectorSchemaRoot>> userListener
    ) {
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
        QueryContext ctx = QueryContext.forTest(dag, task, listeners);
        new QueryScheduler(builder).execute(ctx, userListener);
    }

    private static Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        when(stage.getExecutionType()).thenReturn(StageExecutionType.LOCAL_PASSTHROUGH);
        return stage;
    }

    /**
     * Records every fired callback as a short {@code name[:detail]} label so tests assert
     * fired-set + ordering against a single {@code List<String>}.
     */
    private static final class RecordingListener implements AnalyticsOperationListener {
        final List<String> events = new ArrayList<>();

        @Override
        public void onQueryStart(String queryId, int stageCount) {
            events.add("onQueryStart:" + stageCount);
        }

        @Override
        public void onQuerySuccess(String queryId, long tookInNanos, long totalRows) {
            events.add("onQuerySuccess");
        }

        @Override
        public void onQueryFailure(String queryId, Exception cause) {
            events.add("onQueryFailure");
        }

        @Override
        public void onStageStart(String queryId, int stageId, String stageType) {
            events.add("onStageStart:" + stageId);
        }

        @Override
        public void onStageSuccess(String queryId, int stageId, long tookInNanos, long rowsProcessed) {
            events.add("onStageSuccess:" + stageId);
        }

        @Override
        public void onStageFailure(String queryId, int stageId, Exception cause) {
            events.add("onStageFailure:" + stageId);
        }

        @Override
        public void onStageCancelled(String queryId, int stageId, String reason) {
            events.add("onStageCancelled:" + stageId);
        }
    }

    /**
     * Test stage that extends {@link AbstractStageExecution} so its state transitions
     * fire {@code AnalyticsOperationListener} callbacks through the real production wiring
     * ({@code fireOperationListeners}). Tests drive transitions via the {@code do*}
     * methods — the dispatcher/task path is unused.
     */
    private static class TestableRootExecution extends AbstractStageExecution implements DataProducer {
        private final ExchangeSource source;

        TestableRootExecution(Stage stage, ExchangeSource source, QueryContext cfg) {
            super(stage, cfg.queryId(), cfg.operationListeners());
            this.source = source;
        }

        @Override
        public void start() {
            transitionTo(State.RUNNING);
        }

        @Override
        public void cancel(String reason) {
            transitionTo(State.CANCELLED);
        }

        @Override
        public void onTaskTerminal(StageTask task, Exception cause) {}

        @Override
        public ExchangeSource outputSource() {
            return source;
        }

        void doSucceed() {
            transitionTo(State.SUCCEEDED);
        }

        void doFail(Exception cause) {
            captureFailure(cause);
            transitionTo(State.FAILED);
        }

        void doCancel() {
            transitionTo(State.CANCELLED);
        }
    }

    /** Empty terminal sink that doubles as a row-producing source — read returns empty. */
    private static final class NoopSink implements org.opensearch.analytics.spi.ExchangeSink, ExchangeSource {
        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {}

        @Override
        public Iterable<VectorSchemaRoot> readResult() {
            return Collections.emptyList();
        }

        @Override
        public long getRowCount() {
            return 0;
        }
    }
}
