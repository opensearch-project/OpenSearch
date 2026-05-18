/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.MultiInputExchangeSink;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * COORDINATOR_REDUCE stage. Routes child output into a backend-provided sink via
 * {@link #inputSink(int)}; the reduce drain ({@code backendSink.close()}) runs as
 * a single LOCAL task on the per-query virtual-thread executor.
 *
 * @opensearch.internal
 */
final class LocalStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private final ExchangeSink backendSink;
    private final ExchangeSink downstream;
    /** Single-fire gate — task body and cancel / pre-fail paths race to close. */
    private final AtomicBoolean backendSinkClosed = new AtomicBoolean(false);

    public LocalStageExecution(Stage stage, QueryContext config, ExchangeSink backendSink, ExchangeSink downstream) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.runner = new LocalTaskRunner(config.localTaskExecutor());
    }

    /** Multi-input shapes (Union) → per-child partition; single-input → unchanged backend sink. */
    @Override
    public ExchangeSink inputSink(int childStageId) {
        if (backendSink instanceof MultiInputExchangeSink multi) {
            return multi.sinkForChild(childStageId);
        }
        return backendSink;
    }

    /** {@code backendSink.close()} drains into this downstream; results buffered by readResult. */
    @Override
    public ExchangeSource outputSource() {
        if (downstream instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException(
            "downstream sink " + downstream.getClass().getSimpleName() + " does not implement ExchangeSource"
        );
    }

    @Override
    protected List<StageTask> materializeTasks() {
        // Task body IS the work — closing the sink drives the reduce drain. Exceptions
        // propagate to the runner, which routes via onFailure → stage FAILED.
        return List.of(new LocalStageTask(new StageTaskId(getStageId(), 0), this::closeBackendSinkOnce));
    }

    /**
     * Defensive cleanup for FAILED / CANCELLED paths where the task body didn't get
     * to close the sink. Single-fire via {@link #backendSinkClosed} so the happy-path
     * task body (which already closed) is a no-op here. Swallows because we're
     * already terminal — exceptions can't change the outcome and would be lost anyway.
     */
    @Override
    protected void onTerminalTransition(State terminal) {
        try {
            closeBackendSinkOnce();
        } catch (Exception ignore) {}
    }

    private void closeBackendSinkOnce() {
        if (backendSinkClosed.compareAndSet(false, true)) {
            backendSink.close();
        }
    }
}
