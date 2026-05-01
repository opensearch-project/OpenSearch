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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LocalStageExecution} covering the placeholder
 * lifecycle: inputSink delegates to the backend sink, start closes both
 * sinks and transitions, failFromChild/cancel close the backend sink.
 */
public class LocalStageExecutionTests extends OpenSearchTestCase {

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

    public void testStartClosesBackendSinkAndTransitionsToSucceeded() {
        CapturingSink backend = new CapturingSink();
        CapturingSink downstream = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), backend, downstream);

        exec.start();

        assertTrue("backend sink closed", backend.closed);
        // Downstream is NOT closed by start() — its lifecycle is owned by the walker,
        // which still needs to read the buffered batches via outputSource().readResult().
        assertFalse("downstream must not be closed by LocalStageExecution.start()", downstream.closed);
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    public void testInputSinkReturnsBackendSinkForAnyChildId() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), backend, new CapturingSink());

        assertSame(backend, exec.inputSink(0));
        assertSame(backend, exec.inputSink(7));
        assertSame(backend, exec.inputSink(42));
    }

    public void testOutputSourceReturnsDownstreamWhenItImplementsExchangeSource() {
        RowProducingSink downstream = new RowProducingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), new CapturingSink(), downstream);
        assertSame(downstream, exec.outputSource());
    }

    public void testOutputSourceThrowsWhenDownstreamDoesNotImplementExchangeSource() {
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), new CapturingSink(), new CapturingSink());
        expectThrows(UnsupportedOperationException.class, exec::outputSource);
    }

    public void testStartTransitionsToFailedWhenCloseThrows() {
        RuntimeException boom = new RuntimeException("close blew up");
        ExchangeSink backend = new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {}

            @Override
            public void close() {
                throw boom;
            }
        };
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), backend, new CapturingSink());

        exec.start();

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(boom, exec.getFailure());
    }

    public void testStartIsNoopAfterTerminalTransition() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), backend, new CapturingSink());

        exec.cancel("test cancellation");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);

        backend.closed = false;
        exec.start();
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertFalse("backend sink not re-closed by start()", backend.closed);
    }

    public void testFailFromChildClosesBackendSinkAndTransitions() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), backend, new CapturingSink());

        Exception cause = new RuntimeException("child failed");
        boolean transitioned = exec.failFromChild(cause);

        assertTrue(transitioned);
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(cause, exec.getFailure());
        assertTrue(backend.closed);
    }

    public void testCancelClosesBackendSink() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = new LocalStageExecution(stageWithId(0), backend, new CapturingSink());

        exec.cancel("user requested");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        return stage;
    }

    /** ExchangeSink that records feed/close calls and releases held batches on close. */
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
