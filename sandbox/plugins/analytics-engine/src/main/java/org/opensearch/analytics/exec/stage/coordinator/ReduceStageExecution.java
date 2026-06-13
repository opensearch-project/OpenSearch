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
import org.opensearch.core.action.ActionListener;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Coordinator-side reduce stage execution. Task invokes {@link ReducingExchangeSink#reduce};
 * {@link #onTerminalTransition} invokes {@link ExchangeSink#close} (idempotent) so
 * cancel-before-reduce paths still release resources. Scheduling mode (eager vs buffered)
 * is delegated to {@link ReducingExchangeSink#supportsEagerScheduling()}.
 *
 * <p><b>Virtual-thread reduce.</b> The reduce body runs on the per-query virtual-thread
 * executor ({@link QueryContext#localTaskExecutor()}), the same executor
 * {@code LateMaterializationStageExecution} uses for LOCAL stage bodies. The drain's
 * data-flow waits (the native batch pull behind {@code DatafusionResultStream.BatchIterator}
 * and the input push behind {@code DatafusionPartitionSender}) are now plain Java
 * {@code CompletableFuture} parks — on a virtual thread each park unmounts its carrier, so no
 * platform pool thread is held for the duration of a reduce. A {@code cancel()} reaches the
 * Java-parked drain via the cancellation token → error-completion → unwind path; no thread
 * interruption is involved.
 *
 * <p>{@code localTaskExecutor()} is a raw {@code newThreadPerTaskExecutor} that does not
 * propagate OpenSearch {@code ThreadContext}. The reduce body and its listener completion do
 * not read {@code ThreadContext} (no task-header / tracing reads on the transitive
 * {@code reduce()} path), so no context-preserving wrapper is required — matching
 * {@code LateMaterializationStageExecution}'s use of the same executor.
 *
 * @opensearch.internal
 */
public final class ReduceStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(ReduceStageExecution.class);

    private final ReducingExchangeSink backendSink;
    private final ExchangeSink downstream;
    private final ExecutorService localTaskExecutor;
    private final BufferAllocator allocator;
    private final boolean profile;

    public ReduceStageExecution(Stage stage, QueryContext config, ReducingExchangeSink backendSink, ExchangeSink downstream) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.localTaskExecutor = config.localTaskExecutor();
        this.allocator = config.bufferAllocator();
        this.runner = new LocalTaskRunner(config.schedulerExecutor());
        this.profile = config.profile();
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
        final StageTask[] holder = new StageTask[1];
        holder[0] = new LocalStageTask(new StageTaskId(getStageId(), 0), listener -> {
            localTaskExecutor.execute(() -> {
                try {
                    backendSink.reduce(ActionListener.wrap(v -> {
                        if (profile) {
                            byte[] metrics = backendSink.getExecutionMetrics();
                            if (metrics != null) {
                                holder[0].setDataNodeMetrics(metrics);
                            }
                        }
                        listener.onResponse(v);
                    }, listener::onFailure));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        });
        return List.of(holder[0]);
    }

    @Override
    protected void onTerminalTransition(State terminal) {
        if (terminal == State.CANCELLED || terminal == State.FAILED) {
            if (backendSink instanceof CancellableExchangeSink cancellable) {
                logger.warn("[ReduceStageExecution] stage {} terminal={}, firing cancellable.cancel()", getStageId(), terminal);
                try {
                    cancellable.cancel();
                } catch (Exception e) {
                    logger.warn("[ReduceStageExecution] cancel() threw for stage " + getStageId(), e);
                }
            }
        }
        try {
            backendSink.close();
        } catch (Exception ignore) {}
    }
}
