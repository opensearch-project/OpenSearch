/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.OpenSearchException;
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
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link QueryExecution}'s terminal-sink close-on-failure contract.
 * Covers gaps not exercised through {@code RowProducingSink}: non-closeable sources,
 * throwing closes, and the close-fires-exactly-once invariant.
 */
public class QueryExecutionTests extends OpenSearchTestCase {

    private StageExecutionBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = new StageExecutionBuilder(mock(org.opensearch.cluster.service.ClusterService.class), null);
    }

    public void testQueryStateStartsAsCreated() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));

        assertEquals(QueryExecution.State.CREATED, qe.getState());
    }

    public void testStartTransitionsToRunning() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();

        assertEquals(QueryExecution.State.RUNNING, qe.getState());
    }

    public void testRootStageSuccessTransitionsQueryToSucceeded() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();
        root.succeed();

        assertEquals(QueryExecution.State.SUCCEEDED, qe.getState());
    }

    public void testCloseRunsTerminalSinkOnSuccess() {
        Stage rootStage = stageWithId(0);
        CountingCloseSink sink = new CountingCloseSink();
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();
        root.succeed();

        assertEquals("terminal sink closed exactly once on success", 1, sink.closeCalls.get());
    }

    public void testCancelAllAfterRootFailureKeepsFailedAsTerminal() {
        // Query was already in FAILED before cancelAll. cancelAll's CANCELLED transition
        // must be rejected by the CAS — terminal state is sticky.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();
        root.failWith(new RuntimeException("primary"));
        qe.cancelAll("late cancel");

        assertEquals("first terminal wins", QueryExecution.State.FAILED, qe.getState());
    }

    public void testCancelAllTransitionsToCancelledIdempotently() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicInteger failureCount = new AtomicInteger();
        QueryExecution qe = newQueryExecution(
            rootStage,
            ActionListener.wrap(r -> fail("unexpected success"), e -> failureCount.incrementAndGet())
        );
        qe.start();
        qe.cancelAll("first");
        qe.cancelAll("second");

        assertEquals(QueryExecution.State.CANCELLED, qe.getState());
        assertEquals("listener fired exactly once across two cancelAll calls", 1, failureCount.get());
    }

    public void testFailedTransitionClosesClosableTerminalSinkExactlyOnce() {
        CountingCloseSink sink = new CountingCloseSink();
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

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
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        RuntimeException rootCause = new RuntimeException("non-sink source path");
        root.failWith(rootCause);

        assertSame("original failure propagated despite non-sink source", rootCause, onFailure.get());
    }

    public void testFailedTransitionStillFiresOriginalFailureWhenTerminalSinkCloseThrows() {
        ThrowingCloseSink sink = new ThrowingCloseSink();
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        RuntimeException rootCause = new RuntimeException("original failure");
        root.failWith(rootCause);

        assertSame("original stage failure must reach the listener even when terminal sink close throws", rootCause, onFailure.get());
    }

    public void testBareBreakerMasqueradingAsCancelSurfacesAs429() {
        // The exact shape reproduced live on 91-d: the breaker is the root stage's captured failure
        // (DatafusionReduceSink.reduce → onTaskTerminal → captureFailure), and the same trip then
        // cancels the parent task via the child cancel sweep so isCancelled() is already true.
        // terminalCause must surface the breaker (HTTP 429), not mask it as TaskCancelledException (500).
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AnalyticsQueryTask task = newTask();
        task.cancel("circuit breaker sweep cancelled the parent task");

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set), task);

        CircuitBreakingException breaker = new CircuitBreakingException(
            "[analytics_backend_datafusion] Failed to allocate 64 bytes (limit: 7101482993)",
            64,
            7101482993L,
            CircuitBreaker.Durability.TRANSIENT
        );
        root.failWith(breaker);

        Exception surfaced = onFailure.get();
        assertSame("breaker must be surfaced, not masked by the self-cancel", breaker, surfaced);
        assertEquals("breaker maps to HTTP 429", RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
    }

    public void testWrappedBreakerUnderCancelStillUnwrapsTo429() {
        // Defends the cause-walk: even if the breaker arrives nested (e.g. a CompletionException from
        // an async→sync future.join() bridge) under a cancelled parent task, it must still be unwrapped.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AnalyticsQueryTask task = newTask();
        task.cancel("circuit breaker sweep cancelled the parent task");

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set), task);

        CircuitBreakingException breaker = new CircuitBreakingException(
            "[analytics_backend_datafusion] Failed to allocate 327680 bytes (limit: 4194304)",
            327680,
            4194304,
            CircuitBreaker.Durability.TRANSIENT
        );
        root.failWith(new CompletionException(breaker));

        Exception surfaced = onFailure.get();
        assertSame("nested breaker must be unwrapped, not masked by cancellation", breaker, surfaced);
        assertEquals("breaker maps to HTTP 429", RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
    }

    public void testBareBreakerFailureSurfacesAs429() {
        // A breaker captured directly as the root failure (parent task NOT cancelled) is surfaced as-is
        // via the unchanged non-cancel path.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        CircuitBreakingException breaker = new CircuitBreakingException("Failed to allocate", CircuitBreaker.Durability.TRANSIENT);
        root.failWith(breaker);

        Exception surfaced = onFailure.get();
        assertSame(breaker, surfaced);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
    }

    public void testGenuineCancelWithNoStageFailureStaysTaskCancelled() {
        // No stage recorded a failure (getFailure()==null) and the parent task is cancelled: this is a
        // real top-down cancel and must remain a TaskCancelledException (HTTP 500) — proving the fix
        // does not turn genuine cancellations into 429.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AnalyticsQueryTask task = newTask();
        task.cancel("user aborted the request");

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set), task);
        qe.cancelAll("user aborted the request");

        Exception surfaced = onFailure.get();
        assertTrue("genuine cancel must stay TaskCancelledException", surfaced instanceof TaskCancelledException);
        assertEquals("cancellation maps to HTTP 500", RestStatus.INTERNAL_SERVER_ERROR, ((OpenSearchException) surfaced).status());
    }

    public void testNonBreakerFailureWithCancelledParentStaysTaskCancelled() {
        // A non-breaker stage failure that also cancelled the parent task keeps the original cancel-first
        // behavior: the peek finds no breaker, so we report TaskCancelledException (HTTP 500). Option B
        // only diverts the breaker case; every other cancelled path is unchanged.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AnalyticsQueryTask task = newTask();
        task.cancel("sibling sweep cancelled the parent task");

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set), task);

        root.failWith(new RuntimeException("non-memory stage failure"));

        Exception surfaced = onFailure.get();
        assertTrue("non-breaker failure under a cancel stays TaskCancelledException", surfaced instanceof TaskCancelledException);
    }

    // ── Arrow OutOfMemoryException translation ──────────────────────────
    // Same shape as Garov's CircuitBreakingException tests, but for Arrow Java's allocator-budget
    // refusal (org.apache.arrow.memory.OutOfMemoryException — NOT a JVM OOM). Arrow OOM is the
    // signal that an allocation was rejected against native.allocator.pool.query.max; it belongs
    // in the same back-pressure / 429 class as CircuitBreakingException so FailAwareWeightedRouting
    // skips replica retry. terminalCause translates it to CircuitBreakingException at the coordinator.

    public void testBareArrowOomMasqueradingAsCancelSurfacesAs429() {
        // Mirrors the live failure on dev (332-big5): an Arrow allocator trip fails the reduce stage
        // and the same sweep cancels the parent task, so isCancelled() is already true by the time we
        // report. terminalCause must translate the Arrow OOM to CircuitBreakingException (HTTP 429),
        // not mask it as TaskCancelledException (HTTP 500).
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AnalyticsQueryTask task = newTask();
        task.cancel("arrow allocator sweep cancelled the parent task");

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set), task);

        OutOfMemoryException arrowOom = new OutOfMemoryException(
            "Unable to allocate buffer of size 128 (rounded from 80) due to memory limit. Current allocation: 277821480"
        );
        root.failWith(arrowOom);

        Exception surfaced = onFailure.get();
        assertTrue("Arrow OOM must be translated to CircuitBreakingException", surfaced instanceof CircuitBreakingException);
        assertEquals("translated breaker maps to HTTP 429", RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
        assertSame("original Arrow OOM must be preserved as cause", arrowOom, surfaced.getCause());
        assertTrue("translated message must preserve the Arrow OOM detail", surfaced.getMessage().contains("Unable to allocate buffer"));
    }

    public void testWrappedArrowOomUnderCancelStillUnwrapsTo429() {
        // Mirrors the in-process Arrow OOM path: ArrayImporter wraps the OOM in IllegalArgumentException
        // ("Could not load buffers for field cnt[hll_registers]") before it propagates up. Even nested
        // under a cancelled parent task, the cause-walk must surface it as 429.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AnalyticsQueryTask task = newTask();
        task.cancel("arrow allocator sweep cancelled the parent task");

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set), task);

        OutOfMemoryException arrowOom = new OutOfMemoryException("Unable to allocate buffer of size 98304");
        IllegalArgumentException wrapped = new IllegalArgumentException("Could not load buffers for field cnt[hll_registers]", arrowOom);
        root.failWith(wrapped);

        Exception surfaced = onFailure.get();
        assertTrue("wrapped Arrow OOM must still translate to CircuitBreakingException", surfaced instanceof CircuitBreakingException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
        assertTrue(surfaced.getMessage().contains("Unable to allocate buffer"));
    }

    public void testBareArrowOomFailureSurfacesAs429() {
        // Coordinator-local Arrow OOM (no cancel sweep): a direct in-process materialization
        // OOM with parent task NOT cancelled must still translate to 429 via the non-cancel
        // path so client/router treat it as back-pressure.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        OutOfMemoryException arrowOom = new OutOfMemoryException("Unable to allocate buffer of size 4194304");
        root.failWith(arrowOom);

        Exception surfaced = onFailure.get();
        assertTrue(surfaced instanceof CircuitBreakingException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
        assertSame(arrowOom, surfaced.getCause());
    }

    public void testStreamExceptionCarryingArrowOomMessageTranslatesTo429() {
        // The cross-Flight-RPC path: shard-side Arrow OOM travels over Flight, the wire envelope strips
        // the original OutOfMemoryException class, coordinator receives StreamException[INTERNAL] whose
        // message contains Arrow's "Unable to allocate buffer ..." marker. terminalCause must still
        // surface this as CircuitBreakingException (HTTP 429), not as the raw 500 StreamException.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        StreamException se = new StreamException(
            StreamErrorCode.INTERNAL,
            "Could not load buffers for field cnt[hll_registers]: Binary not null. error message: "
                + "Unable to allocate buffer of size 98304 due to memory limit. Current allocation: 197104"
        );
        root.failWith(se);

        Exception surfaced = onFailure.get();
        assertTrue("post-RPC Arrow OOM must translate to CircuitBreakingException", surfaced instanceof CircuitBreakingException);
        assertEquals("translated breaker maps to HTTP 429", RestStatus.TOO_MANY_REQUESTS, ((OpenSearchException) surfaced).status());
        assertSame("original StreamException must be preserved as cause", se, surfaced.getCause());
        assertTrue("translated message must preserve the Arrow OOM detail", surfaced.getMessage().contains("Unable to allocate buffer"));
    }

    public void testStreamExceptionWithoutArrowOomMessageSurfacesUnchanged() {
        // INTERNAL-code StreamException whose message is NOT an Arrow allocator failure (e.g. a Rust
        // panic surfaced via cross_rt_stream.rs) must NOT be re-classified as back-pressure — operators
        // need 500 so it pages and FailAwareWeightedRouting can retry on a replica.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        StreamException se = new StreamException(
            StreamErrorCode.INTERNAL,
            "java.lang.RuntimeException: Execution error: Panic: byte array offset overflow"
        );
        root.failWith(se);

        Exception surfaced = onFailure.get();
        assertSame("non-Arrow-OOM StreamException must pass through unchanged", se, surfaced);
        assertFalse(surfaced instanceof CircuitBreakingException);
    }

    public void testNonArrowOomFailureSurfacesUnchanged() {
        // An unrelated stage failure (e.g. engine-bug-class — a Rust panic surfaced as a
        // RuntimeException) must NOT be re-classified as back-pressure. It needs HTTP 500
        // so operators see a real "fix this" signal and FailAwareWeightedRouting can retry
        // it on a replica.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        RuntimeException panic = new RuntimeException("Execution error: Panic: byte array offset overflow");
        root.failWith(panic);

        Exception surfaced = onFailure.get();
        assertSame("non-Arrow-OOM failure must pass through unchanged", panic, surfaced);
        assertFalse(surfaced instanceof CircuitBreakingException);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private QueryExecution newQueryExecution(Stage rootStage, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        QueryContext ctx = queryCtx(rootStage);
        ExecutionGraph graph = ExecutionGraph.build(ctx, builder, StageExecution::start);
        return new QueryExecution(ctx, graph, StageExecution::start, listener);
    }

    private QueryExecution newQueryExecution(
        Stage rootStage,
        ActionListener<Iterable<VectorSchemaRoot>> listener,
        AnalyticsQueryTask task
    ) {
        QueryDAG dag = new QueryDAG("q-test", rootStage);
        QueryContext ctx = QueryContext.forTest(dag, task);
        ExecutionGraph graph = ExecutionGraph.build(ctx, builder, StageExecution::start);
        return new QueryExecution(ctx, graph, StageExecution::start, listener);
    }

    private static AnalyticsQueryTask newTask() {
        return new AnalyticsQueryTask(1L, "transport", "analytics_query", "q-test", TaskId.EMPTY_TASK_ID, java.util.Map.of(), null);
    }

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
        return QueryContext.forTest(dag, task);
    }

    /**
     * Minimal {@link StageExecution} + {@link DataProducer} for driving QueryExecution's
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
        public java.util.List<org.opensearch.analytics.exec.stage.StageTask> tasks() {
            return java.util.List.of();
        }

        @Override
        public void onTaskTerminal(org.opensearch.analytics.exec.stage.StageTask task, Exception cause) {
            // no-op
        }

        @Override
        public void addStateListener(StageStateListener listener) {
            listeners.add(listener);
        }

        @Override
        public Exception getFailure() {
            return failure.get();
        }

        @Override
        public boolean failWithCause(Exception cause) {
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
            failWithCause(cause);
        }

        void succeed() {
            State prev = state.getAndSet(State.SUCCEEDED);
            if (prev == State.FAILED || prev == State.SUCCEEDED || prev == State.CANCELLED) {
                return;
            }
            for (StageStateListener l : listeners) {
                l.onStateChange(prev, State.SUCCEEDED);
            }
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
