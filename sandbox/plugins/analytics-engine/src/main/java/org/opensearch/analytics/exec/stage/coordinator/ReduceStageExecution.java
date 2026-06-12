/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.SinkProvidingStageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.InputSinkDecorator;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.CancellableExchangeSink;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.analytics.spi.ReducingExchangeSink;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Coordinator-side reduce stage execution. Task invokes {@link ReducingExchangeSink#reduce};
 * {@link #onTerminalTransition} invokes {@link ExchangeSink#close} (idempotent) so
 * cancel-before-reduce paths still release resources. Scheduling mode (eager vs buffered)
 * is delegated to {@link ReducingExchangeSink#supportsEagerScheduling()}.
 *
 * <p>Dispatched on the scheduler pool (lightweight, handles wait/orchestration) which then
 * forks the actual reduce computation to the SEARCH pool. This prevents deadlocking SEARCH
 * (where fragment execution runs) while keeping reduce compute off the scheduler threads.
 *
 * <p>Releasing the Java buffers DataFusion borrows for its reduce working set is handled natively:
 * {@code backendSink.close()} → {@code df_stream_close} joins the plan-driving CPU task before
 * returning, so every borrowed batch is dropped before the terminal transition closes the allocator.
 *
 * @opensearch.internal
 */
public final class ReduceStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(ReduceStageExecution.class);

    private final ReducingExchangeSink backendSink;
    private final ExchangeSink downstream;
    private final Executor reduceExecutor;
    private final BufferAllocator allocator;

    public ReduceStageExecution(Stage stage, QueryContext config, ReducingExchangeSink backendSink, ExchangeSink downstream) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.reduceExecutor = config.reduceExecutor();
        this.allocator = config.bufferAllocator();
        this.runner = new LocalTaskRunner(config.schedulerExecutor());
    }

    @Override
    public boolean schedulesEagerly() {
        return backendSink.supportsEagerScheduling();
    }

    @Override
    public void closeChildInput(int childStageId) {
        if (backendSink instanceof MultiInputExchangeSink multi) {
            multi.sinkForChild(childStageId).close();
        }
    }

    @Override
    public ExchangeSink inputSink(int childStageId) {
        InputSinkDecorator decorator = stage.getInputSinkDecorator();
        // sinkForChild routing only applies for Union/Join shapes with multiple child stages.
        if (stage.getChildStages().size() > 1) {
            if (decorator != null) {
                throw new IllegalStateException(
                    "InputSinkDecorator on a multi-input reducer (stageId=" + getStageId() + ") is not supported"
                );
            }
            return ((MultiInputExchangeSink) backendSink).sinkForChild(childStageId);
        }
        return decorator != null ? decorator.decorate(backendSink, allocator) : backendSink;
    }

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
        return List.of(new LocalStageTask(new StageTaskId(getStageId(), 0), listener -> reduceExecutor.execute(() -> {
            try {
                backendSink.reduce(listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        })));
    }

    @Override
    protected void onTerminalTransition(State terminal) {
        if (terminal == State.CANCELLED || terminal == State.FAILED) {
            if (backendSink instanceof CancellableExchangeSink cancellable) {
                try {
                    cancellable.cancel();
                } catch (Exception e) {
                    logger.warn("[ReduceStageExecution] cancel() threw for stage " + getStageId(), e);
                }
            }
        }
        // Joins the native plan-driving task, releasing borrowed buffers before the allocator closes.
        try {
            backendSink.close();
        } catch (Exception ignore) {}
    }
}
