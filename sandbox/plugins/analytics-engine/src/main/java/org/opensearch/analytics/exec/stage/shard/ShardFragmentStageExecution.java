/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
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

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;

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
        this.runner = new ShardTaskRunner(this, config, dispatcher, requestBuilder);
    }

    @Override
    protected List<StageTask> materializeTasks() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        // Empty list → base short-circuits to SUCCEEDED (nothing to dispatch).
        List<StageTask> tasks = new ArrayList<>(resolved.size());
        List<ShardExecutionTarget> shardTargets = new ArrayList<>(resolved.size());
        for (int i = 0; i < resolved.size(); i++) {
            ExecutionTarget target = resolved.get(i);
            tasks.add(new ShardStageTask(new StageTaskId(getStageId(), i), target));
            shardTargets.add((ShardExecutionTarget) target);
        }
        // Side-table for cross-stage routing (e.g. QTF Phase C maps ___ugsi → target).
        // See QueryContext.resolvedTargetsByStage Javadoc for HACK rationale.
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
