/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.TopNGate;
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
public final class ShardTaskRunner implements TaskRunner<ShardStageTask> {

    private static final Logger logger = LogManager.getLogger(ShardTaskRunner.class);

    private final ShardFragmentStageExecution stage;
    private final QueryContext config;
    private final AnalyticsSearchTransportService transport;
    private final Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    public ShardTaskRunner(
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
        transport.dispatchFragmentStreaming(
            request,
            target.node(),
            stage.responseListenerFor(task, listener),
            config.parentTask(),
            pending,
            () -> stillNeeded(task, target)
        );
    }

    /**
     * Top-N early termination: {@code false} skips this shard, because it provably cannot place a
     * row in the top-{@code K} the coordinator already holds — so the scan is wasted work.
     * Checked right before the request goes out, not when the task was queued
     */
    private boolean stillNeeded(ShardStageTask task, ShardExecutionTarget target) {
        TopNGate gate = stage.topNGate();
        if (gate == null || gate.canEliminate(stage.sortBounds().get(target)) == false) {
            return true;
        }
        // bottom() is defined here: canEliminate only says yes once K keys are in hand, and the
        // count never shrinks.
        logger.debug("sort-et: skipping shard {} — its whole range is worse than the bar {}", target.shardId(), gate.bottom());
        stage.skipTask(task);
        return false;
    }

    private PendingExecutions pendingFor(ShardExecutionTarget target) {
        return pendingPerNode.computeIfAbsent(
            target.node().getId(),
            n -> new PendingExecutions(config.maxConcurrentShardRequestsPerNode())
        );
    }
}
