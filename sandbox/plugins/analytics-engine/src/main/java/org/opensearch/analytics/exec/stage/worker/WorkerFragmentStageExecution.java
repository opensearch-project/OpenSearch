/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.worker;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.WorkerFragmentRequest;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.WorkerExecutionTarget;
import org.opensearch.analytics.spi.DataConsumer;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Hash-shuffle worker stage execution. Materializes one {@link WorkerStageTask} per
 * {@link WorkerExecutionTarget}; transport owned by {@link WorkerTaskRunner}; data-arrival
 * behavior by {@link #responseListenerFor}.
 *
 * <p>Sibling of {@link org.opensearch.analytics.exec.stage.shard.ShardFragmentStageExecution}
 * — same {@link AbstractStageExecution} contract, different fan-out shape (per-partition
 * targets resolved by {@link org.opensearch.analytics.planner.dag.WorkerTargetResolver}) and
 * different transport ({@link WorkerFragmentRequest} on the worker action channel).
 *
 * @opensearch.internal
 */
public class WorkerFragmentStageExecution extends AbstractStageExecution implements DataProducer, DataConsumer {

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;

    public WorkerFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<WorkerExecutionTarget, WorkerFragmentRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.runner = new WorkerTaskRunner(this, config, dispatcher, requestBuilder);
    }

    @Override
    protected List<StageTask> materializeTasks() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        List<StageTask> tasks = new ArrayList<>(resolved.size());
        for (int i = 0; i < resolved.size(); i++) {
            ExecutionTarget target = resolved.get(i);
            if (!(target instanceof WorkerExecutionTarget worker)) {
                throw new IllegalStateException(
                    "WorkerFragmentStageExecution: expected WorkerExecutionTarget, got " + target.getClass().getSimpleName()
                );
            }
            tasks.add(new WorkerStageTask(new StageTaskId(getStageId(), i), worker));
        }
        return tasks;
    }

    /**
     * Producers (shuffle scan stages) feed worker tasks via the peer-to-peer shuffle transport
     * ({@code AnalyticsShuffleDataAction}), not through the standard in-process child-to-parent
     * sink pipe. The producer's response stream is empty by design — the partitioned sink
     * intercepts batches before they reach the response handler. We therefore return a no-op
     * sink so the framework's child-input plumbing has something to wire, but no data ever
     * flows through it.
     */
    @Override
    public ExchangeSink inputSink(int childStageId) {
        return new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {
                if (batch != null) {
                    batch.close();
                }
            }

            @Override
            public void close() {
                // No-op.
            }
        };
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    /**
     * Same response semantics as the shard variant. Inline-fed batches preserve ordering with
     * the {@code isLast} marker, and the stage-terminal short-circuit closes any post-terminal
     * batches.
     */
    StreamingResponseListener<FragmentExecutionArrowResponse> responseListenerFor(ActionListener<Void> listener) {
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
                    outputSink.feed(vsr);
                } catch (Exception e) {
                    RuntimeException wrapped = new RuntimeException("Worker stage " + getStageId() + " sink feed failed", e);
                    try {
                        vsr.close();
                    } catch (IllegalStateException closeFailure) {
                        wrapped.addSuppressed(closeFailure);
                    }
                    listener.onFailure(wrapped);
                    return false;
                }
                metrics.addRowsProcessed(vsr.getRowCount());
                if (isLast) listener.onResponse(null);
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new RuntimeException("Worker stage " + getStageId() + " failed", e));
            }
        };
    }
}
