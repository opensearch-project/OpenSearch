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
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Leaf stage execution that dispatches fragment work to data-node shards via
 * Arrow streaming, feeding resulting batches into the parent stage's
 * {@link ExchangeSink}.
 *
 * <p>One-shot: constructed, {@link #start()} called once, listener
 * signaled on completion, then discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;
    private final Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.requestBuilder = requestBuilder;
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        if (resolved.isEmpty()) {
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }
        if (transitionTo(StageExecution.State.SCHEDULING) == false) return;
        // Materialise one StageTask per target and register with the per-query
        // TaskTracker before any transport call — so if a dispatch fails mid-loop the
        // tracker still carries every task we're about to kick off. The profile
        // builder later reads per-partition state and timing from here.
        TaskTracker tracker = config.taskTracker();
        List<StageTask> tasks = new ArrayList<>(resolved.size());
        for (int i = 0; i < resolved.size(); i++) {
            StageTask t = new StageTask(new StageTaskId(stage.getStageId(), i), resolved.get(i));
            tasks.add(t);
            tracker.register(t);
        }
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        for (StageTask task : tasks) {
            task.transitionTo(StageTaskState.RUNNING);
            dispatchShardTask(task);
        }
    }

    private void dispatchShardTask(StageTask task) {
        ShardExecutionTarget target = (ShardExecutionTarget) task.target();
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        dispatcher.dispatchFragmentStreaming(request, target.node(), responseListener(task), config.parentTask(), pending);
    }

    private StreamingResponseListener<FragmentExecutionArrowResponse> responseListener(StageTask task) {
        return new StreamingResponseListener<>() {
            // Runs inline on the per-stream virtual thread driving handleStreamResponse.
            // Must NOT offload to a thread pool: reordering across batches would let the
            // isLast=true task race ahead, flip state to SUCCEEDED, and drop queued
            // earlier batches via the isDone() short-circuit.
            @Override
            public void onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                if (isDone()) {
                    VectorSchemaRoot root = response.getRoot();
                    if (root != null) {
                        root.close();
                    }
                    return;
                }

                VectorSchemaRoot vsr = response.getRoot();
                try {
                    outputSink.feed(vsr);
                } catch (Exception e) {
                    // Without this guard the exception only surfaces on the stream's virtual
                    // thread; the task never terminates and the stage hangs to QUERY_TIMEOUT.
                    captureFailure(new RuntimeException("Stage " + stage.getStageId() + " sink feed failed", e));
                    metrics.incrementTasksFailed();
                    onTaskTerminated(task, StageTaskState.FAILED);
                    return;
                }
                metrics.addRowsProcessed(vsr.getRowCount());

                if (isLast) {
                    metrics.incrementTasksCompleted();
                    onTaskTerminated(task, StageTaskState.FINISHED);
                }
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onTaskTerminated(task, StageTaskState.FAILED);
            }
        };
    }

    private void onTaskTerminated(StageTask task, StageTaskState terminalState) {
        // transitionTo no-ops if the task is already terminal — safe to call twice if
        // the transport fires a late onFailure after a successful isLast=true.
        task.transitionTo(terminalState);
        // Stage terminal derives from TaskTracker instead of a local in-flight counter.
        // Concurrent terminal-firing tasks may both see "all terminal" and both attempt
        // the stage transition — transitionTo is CAS-guarded so only one wins.
        if (config.taskTracker().allTasksTerminalForStage(stage.getStageId())) {
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
