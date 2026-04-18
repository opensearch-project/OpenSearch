/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builds a {@link ShardFragmentStageExecution} that fans out shard requests via
 * {@link AnalyticsSearchTransportService}. Takes a pre-resolved {@link ExchangeSink}
 * and doesn't care whether it is a root sink or a parent-provided child sink
 * — {@link StageExecutionBuilder} resolves that distinction before calling.
 *
 * <p>Injects a {@link ResponseCodec} into the execution to decouple the wire
 * format from stage logic. The default codec ({@link RowResponseCodec}) handles
 * the current {@code Object[]} row format; a future Arrow IPC codec would be
 * swapped in here.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageScheduler implements StageScheduler {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;
    private final ResponseCodec<FragmentExecutionResponse> responseCodec;

    ShardFragmentStageScheduler(ClusterService clusterService, AnalyticsSearchTransportService transport) {
        this(clusterService, transport, RowResponseCodec.INSTANCE);
    }

    ShardFragmentStageScheduler(
        ClusterService clusterService,
        AnalyticsSearchTransportService transport,
        ResponseCodec<FragmentExecutionResponse> responseCodec
    ) {
        this.clusterService = clusterService;
        this.transport = transport;
        this.responseCodec = responseCodec;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        List<ShardTarget> targets = TargetResolver.resolveTargets(stage, clusterService, config);
        targets = stage.getShardFilterPhase().filter(targets, stage);

        final String queryId = config.queryId();
        final int stageId = stage.getStageId();
        Function<ShardTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            queryId,
            stageId,
            target.shardId(),
            planAlternatives
        );

        return new ShardFragmentStageExecution(stage, config, sink, targets, requestBuilder, transport, responseCodec);
    }

    private static List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            alternatives.add(new FragmentExecutionRequest.PlanAlternative(plan.backendId(), plan.convertedBytes()));
        }
        return alternatives;
    }
}
