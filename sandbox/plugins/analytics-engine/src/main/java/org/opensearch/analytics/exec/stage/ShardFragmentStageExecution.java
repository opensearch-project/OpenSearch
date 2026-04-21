/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Per-stage execution for row-producing DATA_NODE stages (scans, filters,
 * partial aggregates). Dispatches shard requests via
 * {@link AnalyticsSearchTransportService#dispatchFragment}, decodes streaming
 * responses through a {@link ResponseCodec}, and feeds the resulting Arrow
 * batches into the stage's output {@link ExchangeSink}.
 *
 * <p>The codec abstracts the wire format: the current {@link RowResponseCodec}
 * converts {@code Object[]} rows to Arrow; a future Arrow IPC codec would
 * import IPC buffers directly with zero conversion. The stage execution logic
 * is format-agnostic.
 *
 * <p>Implements {@link DataProducer} because it writes batches into a sink
 * owned by its parent stage. Does not implement {@link DataConsumer} because
 * it is a leaf stage with no children.
 *
 * <p>Lifecycle: {@code CREATED → RUNNING → SUCCEEDED | FAILED | CANCELLED}.
 * Instances are one-shot: constructed, {@link #start()} called once,
 * listener signaled once, discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final AtomicInteger inFlight = new AtomicInteger(0);

    // Immutable config
    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;
    private final Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final ResponseCodec<FragmentExecutionResponse> responseCodec;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher,
        ResponseCodec<FragmentExecutionResponse> responseCodec
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.requestBuilder = requestBuilder;
        this.dispatcher = dispatcher;
        this.responseCodec = responseCodec;
    }

    @Override
    public void start() {
        // Resolve targets lazily at dispatch time. For shuffle/broadcast reads this is
        // where the child stage's manifest would be passed instead of null.
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        if (resolved.isEmpty()) {
            // CREATED → SUCCEEDED directly. transitionTo stamps both start and end.
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        inFlight.set(resolved.size());
        for (ExecutionTarget target : resolved) {
            dispatchShardTask((ShardExecutionTarget) target);
        }
    }

    private void dispatchShardTask(ShardExecutionTarget target) {
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        dispatcher.dispatchFragment(request, target.node(), new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
                config.searchExecutor().execute(() -> {
                    if (isDone()) return;

                    VectorSchemaRoot vsr = responseCodec.decode(response, config.bufferAllocator());
                    outputSink.feed(vsr);
                    metrics.addRowsProcessed(vsr.getRowCount());

                    if (isLast) {
                        metrics.incrementTasksCompleted();
                        onShardTerminated();
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onShardTerminated();
            }
        }, config.parentTask(), pending);
    }

    private void onShardTerminated() {
        if (inFlight.decrementAndGet() == 0) {
            Exception captured = getFailure();
            transitionTo(captured != null ? StageExecution.State.FAILED : StageExecution.State.SUCCEEDED);
        }
    }

    @Override
    public void cancel(String reason) {
        if (transitionTo(StageExecution.State.CANCELLED) == false) return;
        // Bridge to task framework: cancel the parent task so data nodes
        // see the cancellation via TaskCancellationService ban propagation.
        // AnalyticsQueryTask.shouldCancelChildrenOnCancellation() == true
        // ensures child shard tasks on data nodes are cancelled.
        org.opensearch.tasks.Task parentTask = config.parentTask();
        if (parentTask instanceof org.opensearch.tasks.CancellableTask ct && ct.isCancelled() == false) {
            ct.cancel(reason);
        }
    }

    @Override
    public ExchangeSink outputSink() {
        return outputSink;
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    private boolean isDone() {
        StageExecution.State s = getState();
        return s == StageExecution.State.SUCCEEDED || s == StageExecution.State.FAILED || s == StageExecution.State.CANCELLED;
    }

    private PendingExecutions pendingFor(ShardExecutionTarget target) {
        return pendingPerNode.computeIfAbsent(
            target.node().getId(),
            n -> new PendingExecutions(config.maxConcurrentShardRequests())
        );
    }
}
