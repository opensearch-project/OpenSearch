/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PassThroughStageExecution}. */
public class PassThroughStageExecutionTests extends OpenSearchTestCase {

    public void testCtorRejectsNonRowProducingSink() {
        ExchangeSink notRowProducing = new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {}

            @Override
            public void close() {}
        };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PassThroughStageExecution(stageWithId(0), mockContext(), notRowProducing)
        );
        assertTrue(e.getMessage().contains("RowProducingSink"));
    }

    public void testInputSinkReturnsOwnedSinkForAnyChildId() {
        RowProducingSink sink = new RowProducingSink();
        PassThroughStageExecution exec = new PassThroughStageExecution(stageWithId(0), mockContext(), sink);
        assertSame(sink, exec.inputSink(1));
        assertSame(sink, exec.inputSink(42));
    }

    public void testOutputSourceReturnsOwnedSink() {
        RowProducingSink sink = new RowProducingSink();
        PassThroughStageExecution exec = new PassThroughStageExecution(stageWithId(0), mockContext(), sink);
        assertSame(sink, exec.outputSource());
    }

    /** start() materialises one no-op LOCAL task and transitions to RUNNING. */
    public void testStartMaterialisesSingleLocalTask() {
        PassThroughStageExecution exec = new PassThroughStageExecution(stageWithId(0), mockContext(), new RowProducingSink());
        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());
        assertEquals(1, exec.tasks().size());
        assertTrue("local task variant", exec.tasks().get(0) instanceof LocalStageTask);
    }

    /** Re-entry guard: second start() after terminal is a no-op (no task re-materialisation). */
    public void testStartIsNoopAfterTerminalTransition() {
        PassThroughStageExecution exec = new PassThroughStageExecution(stageWithId(0), mockContext(), new RowProducingSink());
        exec.start();
        // Drive to a terminal directly via cancel; subsequent start() must not re-materialise tasks.
        exec.cancel("test");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        int tasksBefore = exec.tasks().size();
        exec.start();
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("tasks list unchanged after second start()", tasksBefore, exec.tasks().size());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        return stage;
    }

    private static QueryContext mockContext() {
        QueryContext ctx = mock(QueryContext.class);
        Executor inline = Runnable::run;
        when(ctx.localTaskExecutor()).thenReturn(java.util.concurrent.Executors.newThreadPerTaskExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }));
        return ctx;
    }
}
