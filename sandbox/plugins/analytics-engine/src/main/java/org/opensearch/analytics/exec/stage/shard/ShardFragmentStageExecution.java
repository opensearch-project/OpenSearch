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
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;
import java.util.function.Function;

/**
 * Leaf stage: dispatches fragment work to data-node shards via Arrow streaming,
 * one {@link StageTask} per resolved target. Transport owned by {@link ShardTaskRunner};
 * data-arrival behavior by {@link #responseListenerFor}.
 *
 * @opensearch.internal
 */
public class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private static final Logger LOGGER = LogManager.getLogger(ShardFragmentStageExecution.class);

    /** Bound on the synchronous wait for the can-match dispatch. Conservative — long enough
     *  to absorb a slow per-shard response, short enough that a stuck data node doesn't park
     *  the whole stage forever. On timeout we fail-open (return all targets). */
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

    /**
     * Synchronous path is unused now ({@link #materializeTasksAsync} drives the lifecycle),
     * but the abstract template still requires the method. Implements the no-canMatch path
     * — direct target resolution + task build — kept for unit tests that exercise the base
     * template via mocks.
     */
    @Override
    protected List<StageTask> materializeTasks() {
        return buildTasks(stage.getTargetResolver().resolve(clusterService.state(), null));
    }

    /**
     * Async materialise: resolve targets, dispatch can-match concurrently to each target,
     * publish only the surviving subset.
     *
     * <p>Failure modes (timeout, transport error, no extractable filters, unknown backend)
     * fall back to the full target list — pruning is best-effort and must never produce
     * incorrect results.
     *
     * <p>TODO(canmatch-as-stage): lift can-match into its own coordinator-side stage that
     * produces a target manifest consumed via {@code TargetResolver.resolve(state, manifest)}.
     * Wins: stage-level metrics + lifecycle (start/end, cancel cascade, retry); composes
     * naturally with future pre-filter phases (bloom, partition pruning); separates "what
     * shards exist" from "what shards can match" inside this class. Defer until either a
     * second pre-filter phase lands, can-match metrics become load-bearing, or
     * cancellation correctness needs to abort in-flight dispatch deterministically. The
     * {@code TargetResolver.resolve(state, manifest)} signature already accepts a manifest
     * argument — it's half-built for this.
     */
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
        // Short-circuit when there's nothing to prune against, BEFORE touching the
        // transport service — keeps the no-canmatch path mock-friendly for existing
        // ShardFragmentStageExecutionTests that stub a bare dispatcher.
        List<CanMatchFilter> filters = stage.getCanMatchFilters();
        String backendId = resolveBackendId();
        if (filters == null || filters.isEmpty() || backendId == null) {
            listener.onResponse(buildTasks(resolved));
            return;
        }
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(dispatcher.streamTransportService());
        ThreadPool threadPool = dispatcher.streamTransportService().getThreadPool();
        dispatchWithTimeoutAsync(
            resolved,
            filters,
            backendId,
            phase,
            threadPool,
            CAN_MATCH_TIMEOUT,
            ActionListener.wrap(matching -> {
                LOGGER.debug("can-match pruned {} → {} targets", resolved.size(), matching.size());
                listener.onResponse(buildTasks(matching));
            }, e -> {
                // dispatchWithTimeoutAsync swallows transport failures into fail-open, so this
                // path is unreachable in practice — kept for completeness.
                LOGGER.warn("can-match dispatch failed; falling back to all targets", e);
                listener.onResponse(buildTasks(resolved));
            })
        );
    }

    /**
     * Async can-match dispatch with a fail-open timeout. Wraps the supplied {@code phase}'s
     * dispatch with a scheduled fail-open: if neither the response nor the failure callback
     * arrives within {@code timeout}, fires {@code listener.onResponse(targets)} (the full
     * input list, no pruning). Late responses after timeout are no-ops (CAS-guarded).
     *
     * <p>Short-circuits without dispatching when the input lacks anything to prune against
     * (empty targets / filters / null backend) or when the filter list fails to serialise.
     */
    static void dispatchWithTimeoutAsync(
        List<ExecutionTarget> targets,
        List<CanMatchFilter> filters,
        String backendId,
        CanMatchPreFilterPhase phase,
        ThreadPool threadPool,
        TimeValue timeout,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        if (targets.isEmpty() || filters == null || filters.isEmpty() || backendId == null) {
            listener.onResponse(targets);
            return;
        }
        final byte[] filterBytes;
        try {
            filterBytes = CanMatchFilter.listToBytes(filters);
        } catch (IOException e) {
            LOGGER.warn("can-match filter serialization failed; skipping prune", e);
            listener.onResponse(targets);
            return;
        }
        final AtomicBoolean fired = new AtomicBoolean(false);
        final Scheduler.ScheduledCancellable scheduled = threadPool.schedule(() -> {
            if (fired.compareAndSet(false, true)) {
                LOGGER.warn("can-match timed out after {}; falling back to all targets", timeout);
                listener.onResponse(targets);
            }
        }, timeout, ThreadPool.Names.GENERIC);
        phase.filter(targets, filterBytes, backendId, new ActionListener<>() {
            @Override
            public void onResponse(List<ExecutionTarget> matching) {
                if (fired.compareAndSet(false, true)) {
                    scheduled.cancel();
                    listener.onResponse(matching);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (fired.compareAndSet(false, true)) {
                    scheduled.cancel();
                    LOGGER.warn("can-match dispatch failed; falling back to all targets", e);
                    listener.onResponse(targets);
                }
            }
        });
    }

    private List<StageTask> buildTasks(List<ExecutionTarget> targets) {
        List<StageTask> tasks = new ArrayList<>(targets.size());
        List<ShardExecutionTarget> shardTargets = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            ExecutionTarget target = targets.get(i);
            tasks.add(new ShardStageTask(new StageTaskId(getStageId(), i), target));
            shardTargets.add((ShardExecutionTarget) target);
        }
        // Side-table for cross-stage routing (e.g. QTF Phase C maps ___ugsi → target).
        // See QueryContext.resolvedTargetsByStage Javadoc for HACK rationale. Recorded here,
        // the funnel for both the sync and the can-match-pruned async paths, so the routing
        // table reflects exactly the targets that survive pruning and get dispatched.
        config.recordResolvedTargets(getStageId(), shardTargets);
        return tasks;
    }

    /**
     * Sync convenience used by {@code ShardFragmentStageExecutionCanMatchTests}; identical
     * semantics to the async path but blocks on a future. Kept for unit-test coverage of
     * every short-circuit branch.
     */
    static List<ExecutionTarget> applyCanMatchFilter(
        List<ExecutionTarget> targets,
        List<CanMatchFilter> filters,
        String backendId,
        CanMatchPreFilterPhase phase,
        TimeValue timeout
    ) {
        if (targets.isEmpty() || filters == null || filters.isEmpty() || backendId == null) {
            return targets;
        }
        byte[] filterBytes;
        try {
            filterBytes = CanMatchFilter.listToBytes(filters);
        } catch (IOException e) {
            LOGGER.warn("can-match filter serialization failed; skipping prune", e);
            return targets;
        }
        org.opensearch.action.support.PlainActionFuture<List<ExecutionTarget>> future = new org.opensearch.action.support.PlainActionFuture<>();
        try {
            phase.filter(targets, filterBytes, backendId, future);
            List<ExecutionTarget> matching = future.actionGet(timeout);
            LOGGER.debug("can-match pruned {} → {} targets", targets.size(), matching.size());
            return matching;
        } catch (Exception e) {
            LOGGER.warn("can-match dispatch failed; falling back to all targets", e);
            return targets;
        }
    }

    /** Pulls the backend id off the first plan alternative; {@code null} when none present. */
    private String resolveBackendId() {
        List<org.opensearch.analytics.planner.dag.StagePlan> plans = stage.getPlanAlternatives();
        if (plans == null || plans.isEmpty()) {
            return null;
        }
        return plans.get(0).backendId();
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
    StreamingResponseListener<FragmentExecutionArrowResponse> responseListenerFor(int sourceOrdinal, ActionListener<Void> listener) {
        return new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                VectorSchemaRoot vsr = response.getRoot();
                if (getState().isTerminal()) {
                    if (vsr != null) vsr.close();
                    return;
                }
                if (vsr == null) {
                    if (isLast) listener.onResponse(null);
                    return;
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
                    return;
                }
                metrics.addRowsProcessed(vsr.getRowCount());
                if (isLast) listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new RuntimeException("Stage " + getStageId() + " failed", e));
            }
        };
    }
}
