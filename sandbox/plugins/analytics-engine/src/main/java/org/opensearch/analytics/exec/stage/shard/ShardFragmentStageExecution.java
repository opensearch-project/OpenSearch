/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.ContextAwareExecutor;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchFilterSerializer;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Leaf stage: dispatches fragment work to data-node shards via Arrow streaming,
 * one {@link StageTask} per resolved target. Transport owned by {@link ShardTaskRunner};
 * data-arrival behavior by {@link #responseListenerFor}.
 *
 * @opensearch.internal
 */
public class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private static final Logger logger = LogManager.getLogger(ShardFragmentStageExecution.class);
    private static final TimeValue CAN_MATCH_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService dispatcher;

    public ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.dispatcher = dispatcher;
        this.runner = new ShardTaskRunner(this, config, dispatcher, requestBuilder);
    }

    @Override
    protected List<StageTask> materializeTasks() {
        return buildTasks(stage.getTargetResolver().resolve(clusterService.state(), null));
    }

    @Override
    protected void materializeTasksAsync(ActionListener<List<StageTask>> listener) {
        final List<ExecutionTarget> resolved;
        try {
            resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (resolved.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }

        List<CanMatchFilter> filters = stage.getCanMatchFilters();
        if (dispatcher == null || filters == null || filters.isEmpty()) {
            listener.onResponse(buildTasks(resolved));
            return;
        }

        byte[] filterBytes;
        try {
            filterBytes = CanMatchFilterSerializer.serialize(filters);
        } catch (Exception e) {
            logger.debug("can-match: filter serialization failed, skipping prune: {}", e.getMessage());
            listener.onResponse(buildTasks(resolved));
            return;
        }

        String backendId = resolveBackendId();
        if (backendId == null) {
            listener.onResponse(buildTasks(resolved));
            return;
        }
        CanMatchPreFilterPhase canMatchPhase = new CanMatchPreFilterPhase(dispatcher.getTransportService());
        dispatchWithTimeoutAsync(
            resolved,
            filterBytes,
            backendId,
            canMatchPhase,
            CAN_MATCH_TIMEOUT,
            ActionListener.wrap(filtered -> listener.onResponse(buildTasks(filtered)), e -> listener.onResponse(buildTasks(resolved)))
        );
    }

    /**
     * Dispatches can-match with a timeout. Ensures exactly one listener invocation:
     * either the filtered result on success, or the full target list on timeout/error.
     */
    private void dispatchWithTimeoutAsync(
        List<ExecutionTarget> targets,
        byte[] filterBytes,
        String backendId,
        CanMatchPreFilterPhase phase,
        TimeValue timeout,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        long startNanos = System.nanoTime();
        AtomicBoolean fired = new AtomicBoolean(false);

        // Can-match responses complete on the transport thread (handler executor="same").
        // Resume the completion — which chains through publishTasksAndStart into the
        // fragment-dispatch fan-out — OFF the transport thread. Running the dispatch chain
        // inline on the transport thread starves the pool: the thread can't return to
        // process the remaining can-match responses, the fan-in never completes, and the
        // node deadlocks. See resumeOnPool for the pool choice.
        ThreadPool threadPool = dispatcher.getStreamingTransportService().getThreadPool();

        Scheduler.ScheduledCancellable scheduled = threadPool.schedule(() -> {
            if (fired.compareAndSet(false, true)) {
                logger.warn("can-match timed out after {} — fail-open, using all targets", timeout);
                resumeOnPool(threadPool, () -> listener.onResponse(targets));
            }
        }, timeout, ThreadPool.Names.SAME);

        phase.filter(targets, filterBytes, backendId, ActionListener.wrap(filtered -> {
            if (fired.compareAndSet(false, true)) {
                scheduled.cancel();
                long elapsed = (System.nanoTime() - startNanos) / 1_000_000;
                logger.debug(
                    "can-match complete: {} shards checked, {} pruned, {}ms",
                    targets.size(),
                    targets.size() - filtered.size(),
                    elapsed
                );
                resumeOnPool(threadPool, () -> listener.onResponse(filtered));
            }
        }, e -> {
            if (fired.compareAndSet(false, true)) {
                scheduled.cancel();
                logger.debug("can-match failed, using all targets: {}", e.getMessage());
                resumeOnPool(threadPool, () -> listener.onResponse(targets));
            }
        }));
    }

    /**
     * Runs the can-match completion on the SEARCH pool (same pool the fragment/fetch handlers use),
     * off the transport thread. Wrapped in ContextAwareExecutor to carry the opaque id into logs.
     * On SEARCH rejection the stage fails ({@code failWithCause}) rather than running inline.
     */
    private void resumeOnPool(ThreadPool threadPool, Runnable completion) {
        try {
            ContextAwareExecutor.wrap(threadPool.executor(ThreadPool.Names.SEARCH), threadPool).execute(completion);
        } catch (OpenSearchRejectedExecutionException rejected) {
            logger.warn("can-match: SEARCH pool rejected completion, failing stage", rejected);
            failWithCause(rejected);
        }
    }

    /** Pulls the backend id off the first plan alternative; {@code null} when none present. */
    private String resolveBackendId() {
        List<StagePlan> plans = stage.getPlanAlternatives();
        if (plans == null || plans.isEmpty()) {
            return null;
        }
        return plans.get(0).backendId();
    }

    private List<StageTask> buildTasks(List<ExecutionTarget> targets) {
        List<StageTask> tasks = new ArrayList<>(targets.size());
        List<ShardExecutionTarget> shardTargets = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            ExecutionTarget target = targets.get(i);
            tasks.add(new ShardStageTask(new StageTaskId(getStageId(), i), target));
            shardTargets.add((ShardExecutionTarget) target);
        }
        config.recordResolvedTargets(getStageId(), shardTargets);
        return tasks;
    }

    /**
     * Replica failover: on dispatch failure, advance to the next copy of the same shard via
     * {@link ShardExecutionTarget#nextCopy(Exception)}, which delegates to
     * {@link org.opensearch.cluster.routing.FailAwareWeightedRouting#findNext} — same iterator
     * walk + weighted-routing skip + fail-open semantics the search API uses in
     * {@code AbstractSearchAsyncAction.onShardFailure}.
     *
     * <p>Returns empty when the iterator is exhausted; the scheduler then propagates the cause
     * via {@code onTaskTerminal} and the stage fails. Cancellation short-circuit lives one
     * layer up in {@code QueryScheduler.handleFor} — it applies uniformly to every stage type.
     */
    @Override
    public Optional<StageTask> retargetForRetry(StageTask failed, Exception cause) {
        if (!(failed instanceof ShardStageTask shardTask)) {
            return Optional.empty();
        }
        if (!(shardTask.target() instanceof ShardExecutionTarget shardTarget)) {
            return Optional.empty();
        }
        ShardExecutionTarget nextCopy = shardTarget.nextCopy(cause);
        if (nextCopy == null) {
            return Optional.empty();
        }
        // Update the resolved target so downstream stages (LM fetch) route to the node
        // that will run the retry, not the original primary that failed.
        config.updateResolvedTarget(getStageId(), shardTarget.ordinal(), nextCopy);
        return Optional.of(new ShardStageTask(shardTask.id(), nextCopy));
    }

    // FOLLOW-UP: per-stage cancel granularity. Today AbstractStageExecution.cancel cancels
    // the whole parent task (via ct.cancel) to terminate in-flight data-node Flight streams.
    // That's coarse — fine for current query shapes (one failure means the query fails) but
    // it masks the real failure cause as "TaskCancelledException" in QueryExecution.terminalCause,
    // and forecloses speculative-execution / per-stage abort. Surgical alternative: track
    // per-task child-task-ids in ShardTaskRunner; cancel just those when this stage's
    // onTerminalTransition fires CANCELLED.

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    /**
     * Runs inline on the per-stream virtual thread driving handleStreamResponse — must NOT
     * offload: reordering would let isLast race ahead and drop earlier batches via the
     * stage-terminal short-circuit. Inline also preserves end-to-end backpressure.
     */
    StreamingResponseListener<FragmentExecutionArrowResponse> responseListenerFor(ShardStageTask task, ActionListener<Void> listener) {
        final int sourceOrdinal = ((ShardExecutionTarget) task.target()).ordinal();
        return new StreamingResponseListener<>() {
            @Override
            public boolean onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                VectorSchemaRoot vsr = response.getRoot();
                if (getState().isTerminal()) {
                    if (vsr != null) vsr.close();
                    return false; // stage already settled — stop draining, let the caller cancel the stream
                }
                if (vsr == null) {
                    if (isLast) listener.onResponse(null);
                    return true;
                }
                try {
                    outputSink.feed(vsr, sourceOrdinal);
                } catch (Exception e) {
                    // Sink didn't take ownership — close the VSR before surfacing.
                    RuntimeException wrapped = new RuntimeException("Stage " + getStageId() + " sink feed failed", e);
                    try {
                        vsr.close();
                    } catch (IllegalStateException closeFailure) {
                        wrapped.addSuppressed(closeFailure);
                    }
                    listener.onFailure(wrapped);
                    return false;
                }
                metrics.addRowsProcessed(vsr.getRowCount());
                // Downstream consumer satisfied (e.g. a LimitExec above the reduce finished and dropped
                // this input's receiver). Settle this task as success and tell the caller to cancel the
                // stream so this shard stops scanning instead of feeding batches that will be discarded.
                // Each input reacts independently on its own stream.
                if (outputSink.isConsumerDone()) {
                    listener.onResponse(null);
                    return false;
                }
                if (isLast) listener.onResponse(null);
                return true;
            }

            @Override
            public void onStreamComplete(byte[] trailingMetadata) {
                if (trailingMetadata != null) {
                    task.setDataNodeMetrics(trailingMetadata);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new RuntimeException("Stage " + getStageId() + " failed", e));
            }
        };
    }
}
