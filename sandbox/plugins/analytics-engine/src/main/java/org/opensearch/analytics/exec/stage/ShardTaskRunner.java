/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.core.action.ActionListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * SHARD-kind task runner: opens an Arrow Flight stream per task. Response handling
 * lives on the stage via {@link ShardFragmentStageExecution#responseListenerFor}.
 * Per-node concurrency is gated by {@link PendingExecutions}.
 *
 * @opensearch.internal
 */
final class ShardTaskRunner implements TaskRunner<ShardStageTask> {

    private final ShardFragmentStageExecution stage;
    private final QueryContext config;
    private final AnalyticsSearchTransportService transport;
    private final Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    ShardTaskRunner(
        ShardFragmentStageExecution stage,
        QueryContext config,
        AnalyticsSearchTransportService transport,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder
    ) {
        this.stage = stage;
        this.config = config;
        this.transport = transport;
        this.requestBuilder = requestBuilder;
    }

    @Override
    public void run(ShardStageTask task, ActionListener<Void> listener) {
        ShardExecutionTarget target = (ShardExecutionTarget) task.target();
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        transport.dispatchFragmentStreaming(request, target.node(), stage.responseListenerFor(listener), config.parentTask(), pending);
    }

    private PendingExecutions pendingFor(ShardExecutionTarget target) {
        return pendingPerNode.computeIfAbsent(target.node().getId(), n -> new PendingExecutions(config.maxConcurrentShardRequests()));
    }
}
