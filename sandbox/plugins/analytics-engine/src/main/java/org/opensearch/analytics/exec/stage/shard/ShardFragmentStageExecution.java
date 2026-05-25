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

    // TODO: override retargetForRetry for replica failover — needs TargetResolver.alternateReplica
    // and per-task attempt tracking. Scheduler-side wiring is already in place.
    //
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
