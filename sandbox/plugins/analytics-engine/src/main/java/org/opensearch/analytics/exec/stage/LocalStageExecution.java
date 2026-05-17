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
        super(stage, config.queryId(), config.operationListeners());
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
    public void start() {
        // Task body — exceptions propagate to the runner, which calls handle.onFailed.
        Runnable r = () -> {
            if (backendSinkClosed.compareAndSet(false, true)) {
                backendSink.close();
            }
        };
        LocalStageTask task = new LocalStageTask(new StageTaskId(getStageId(), 0), r);
        publishTasksAndStart(List.of(task));
    }

    /**
     * Close backend sink BEFORE transitioning — transitionTo fires terminal listeners
     * inline; the query mirror tears down the per-query allocator; an in-flight import
     * would race a closed allocator.
     */
    @Override
    public boolean failWithCause(Exception cause) {
        captureFailure(cause);
        closeBackendSinkSilently();
        return transitionTo(State.FAILED);
    }

    /** Same ordering rationale as {@link #failWithCause}: close sink before terminal listeners fire. */
    @Override
    public void cancel(String reason) {
        closeBackendSinkSilently();
        super.cancel(reason);
    }

    /** Single-fire close — swallows: caller's primary cause is what matters. */
    private void closeBackendSinkSilently() {
        if (backendSinkClosed.compareAndSet(false, true)) {
            try {
                backendSink.close();
            } catch (Exception ignore) {}
        }
    }
}
