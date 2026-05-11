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
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Leaf stage execution that dispatches fragment work to data-node shards.
 *
 * <p>Handles both Arrow streaming and row (codec-decoded) responses, feeding
 * resulting batches into the parent stage's {@link ExchangeSink}.
 *
 * <p>One-shot: constructed, {@link #start()} called once, listener
 * signaled on completion, then discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final AtomicInteger inFlight = new AtomicInteger(0);

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

    private boolean useArrowStreaming() {
        return dispatcher.isStreamingEnabled();
    }

    @Override
    public void start() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        if (resolved.isEmpty()) {
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
        if (useArrowStreaming()) {
            dispatcher.dispatchFragmentStreaming(
                request,
                target.node(),
                responseListener(FragmentExecutionArrowResponse::getRoot),
                config.parentTask(),
                pending
            );
        } else {
            dispatcher.dispatchFragment(
                request,
                target.node(),
                responseListener(r -> responseCodec.decode(r, config.bufferAllocator())),
                config.parentTask(),
                pending
            );
        }
    }

    private <T extends ActionResponse> StreamingResponseListener<T> responseListener(Function<T, VectorSchemaRoot> toVsr) {
        return new StreamingResponseListener<>() {
            // Runs inline on the per-stream virtual thread driving handleStreamResponse.
            // Must NOT offload to a thread pool: reordering across batches would let the
            // isLast=true task race ahead, flip state to SUCCEEDED, and drop queued
            // earlier batches via the isDone() short-circuit.
            @Override
            public void onStreamResponse(T response, boolean isLast) {
                if (isDone()) {
                    releaseResponseResources(response);
                    return;
                }

                VectorSchemaRoot vsr = toVsr.apply(response);
                outputSink.feed(vsr);
                metrics.addRowsProcessed(vsr.getRowCount());

                if (isLast) {
                    metrics.incrementTasksCompleted();
                    onShardTerminated();
                }
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onShardTerminated();
            }
        };
    }

    private static <T> void releaseResponseResources(T response) {
        if (response instanceof ArrowBatchResponse arrowResp && arrowResp.getRoot() != null) {
            arrowResp.getRoot().close();
        }
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
        // Cancelling the parent task propagates to data-node shard tasks via TaskCancellationService.
        org.opensearch.tasks.Task parentTask = config.parentTask();
        if (parentTask instanceof org.opensearch.tasks.CancellableTask ct && ct.isCancelled() == false) {
            ct.cancel(reason);
        }
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
        return pendingPerNode.computeIfAbsent(target.node().getId(), n -> new PendingExecutions(config.maxConcurrentShardRequests()));
    }
}
