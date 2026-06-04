/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.worker;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.WorkerFragmentRequest;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.WorkerExecutionTarget;
import org.opensearch.core.action.ActionListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Worker-kind task runner: opens an Arrow Flight stream per task. Sibling of
 * {@link org.opensearch.analytics.exec.stage.shard.ShardTaskRunner}, but routes through
 * {@link AnalyticsSearchTransportService#dispatchWorkerFragmentStreaming} so the request is a
 * shardId-free {@link WorkerFragmentRequest}.
 *
 * @opensearch.internal
 */
public final class WorkerTaskRunner implements TaskRunner<WorkerStageTask> {

    private final WorkerFragmentStageExecution stage;
    private final QueryContext config;
    private final AnalyticsSearchTransportService transport;
    private final Function<WorkerExecutionTarget, WorkerFragmentRequest> requestBuilder;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    public WorkerTaskRunner(
        WorkerFragmentStageExecution stage,
        QueryContext config,
        AnalyticsSearchTransportService transport,
        Function<WorkerExecutionTarget, WorkerFragmentRequest> requestBuilder
    ) {
        this.stage = stage;
        this.config = config;
        this.transport = transport;
        this.requestBuilder = requestBuilder;
    }

    @Override
    public void run(WorkerStageTask task, ActionListener<Void> listener) {
        WorkerExecutionTarget target = task.target();
        WorkerFragmentRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        transport.dispatchWorkerFragmentStreaming(
            request,
            target.node(),
            stage.responseListenerFor(listener),
            config.parentTask(),
            pending
        );
    }

    private PendingExecutions pendingFor(WorkerExecutionTarget target) {
        return pendingPerNode.computeIfAbsent(
            target.node().getId(),
            n -> new PendingExecutions(config.maxConcurrentShardRequestsPerNode())
        );
    }
}
