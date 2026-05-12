/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageMetrics;
import org.opensearch.analytics.exec.stage.StageStateListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PlanWalker}'s terminal-sink close-on-failure contract.
 * Covers gaps not exercised through {@code RowProducingSink}: non-closeable sources,
 * throwing closes, and the close-fires-exactly-once invariant.
 */
public class PlanWalkerTests extends OpenSearchTestCase {

    private StageExecutionBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = new StageExecutionBuilder(mock(org.opensearch.cluster.service.ClusterService.class), null);
    }

    public void testFailedTransitionClosesClosableTerminalSinkExactlyOnce() {
        CountingCloseSink sink = new CountingCloseSink();
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerScheduler(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        PlanWalker walker = new PlanWalker(queryCtx(rootStage), builder, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));
        walker.build();
        walker.wireCompletion();

        RuntimeException rootCause = new RuntimeException("stage failed");
        root.failWith(rootCause);

        assertEquals("terminal sink close fires exactly once on FAILED", 1, sink.closeCalls.get());
        assertSame("original failure propagated to completion listener", rootCause, onFailure.get());
    }

    public void testFailedTransitionSkipsCloseWhenSourceIsNotASink() {
        // Source doesn't implement ExchangeSink — walker must skip close, still fire onFailure.
        ExchangeSource source = new ExchangeSource() {
            @Override
            public Iterable<VectorSchemaRoot> readResult() {
                return Collections.emptyList();
            }

            @Override
            public long getRowCount() {
                return 0;
            }
        };
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, source);
        builder.registerScheduler(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        PlanWalker walker = new PlanWalker(queryCtx(rootStage), builder, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));
        walker.build();
        walker.wireCompletion();

        RuntimeException rootCause = new RuntimeException("non-sink source path");
        root.failWith(rootCause);

        assertSame("original failure propagated despite non-sink source", rootCause, onFailure.get());
    }

    public void testFailedTransitionStillFiresOriginalFailureWhenTerminalSinkCloseThrows() {
        ThrowingCloseSink sink = new ThrowingCloseSink();
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerScheduler(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        PlanWalker walker = new PlanWalker(queryCtx(rootStage), builder, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));
        walker.build();
        walker.wireCompletion();

        RuntimeException rootCause = new RuntimeException("original failure");
        root.failWith(rootCause);

        assertSame(
            "original stage failure must reach the listener even when terminal sink close throws",
            rootCause,
            onFailure.get()
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private static Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        when(stage.getExecutionType()).thenReturn(StageExecutionType.LOCAL_PASSTHROUGH);
        return stage;
    }

    private static QueryContext queryCtx(Stage rootStage) {
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            1L,
            "transport",
            "analytics_query",
            "q-test",
            TaskId.EMPTY_TASK_ID,
            java.util.Map.of(),
            null
        );
        QueryDAG dag = new QueryDAG("q-test", rootStage);
        return new QueryContext(dag, Runnable::run, task, 1, Long.MAX_VALUE);
    }

    /**
     * Minimal {@link StageExecution} + {@link DataProducer} for driving PlanWalker's
     * completion listener. Implemented against the public interface so we don't
     * depend on package-private {@code AbstractStageExecution}.
     */
    private static final class TestRootExecution implements StageExecution, DataProducer {
        private final Stage stage;
        private final ExchangeSource source;
        private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final List<StageStateListener> listeners = new ArrayList<>();
        private final StageMetrics metrics = new StageMetrics();

        TestRootExecution(Stage stage, ExchangeSource source) {
            this.stage = stage;
            this.source = source;
        }

        @Override
        public int getStageId() {
            return stage.getStageId();
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
            return failure.get();
        }

        @Override
        public boolean failFromChild(Exception cause) {
            failure.compareAndSet(null, cause);
            State prev = state.getAndSet(State.FAILED);
            if (prev == State.FAILED || prev == State.SUCCEEDED || prev == State.CANCELLED) {
                return false;
            }
            for (StageStateListener l : listeners) {
                l.onStateChange(prev, State.FAILED);
            }
            return true;
        }

        @Override
        public void cancel(String reason) {
            State prev = state.getAndSet(State.CANCELLED);
            if (prev == State.FAILED || prev == State.SUCCEEDED || prev == State.CANCELLED) {
                return;
            }
            for (StageStateListener l : listeners) {
                l.onStateChange(prev, State.CANCELLED);
            }
        }

        @Override
        public ExchangeSource outputSource() {
            return source;
        }

        void failWith(Exception cause) {
            failFromChild(cause);
        }
    }

    /** Sink+Source that counts close() invocations. */
    private static final class CountingCloseSink implements ExchangeSink, ExchangeSource {
        final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        @Override
        public Iterable<VectorSchemaRoot> readResult() {
            return Collections.emptyList();
        }

        @Override
        public long getRowCount() {
            return 0;
        }
    }

    /** Sink+Source whose close() throws — models a misbehaving terminal collector. */
    private static final class ThrowingCloseSink implements ExchangeSink, ExchangeSource {
        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {
            throw new IllegalStateException("close() throws by design");
        }

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
