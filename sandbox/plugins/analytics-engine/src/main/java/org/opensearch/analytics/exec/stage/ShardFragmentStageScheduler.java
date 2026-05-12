/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
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
 * @opensearch.internal
 */
final class ShardFragmentStageScheduler implements StageScheduler {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;

    ShardFragmentStageScheduler(ClusterService clusterService, AnalyticsSearchTransportService transport) {
        this.clusterService = clusterService;
        this.transport = transport;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        final String queryId = config.queryId();
        final int stageId = stage.getStageId();
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            queryId,
            stageId,
            target.shardId(),
            planAlternatives
        );
        // Execution pulls the resolver off `stage` and calls resolve() lazily at start().
        // This keeps target resolution out of the build phase so cancellation before
        // dispatch doesn't pay for cluster-state routing, and leaves room for shuffle
        // reads whose targets depend on child manifests only available at dispatch time.
        return new ShardFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, transport);
    }

    private static List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            DelegationDescriptor delegationDescriptor = buildDelegationDescriptor(plan);
            alternatives.add(
                new FragmentExecutionRequest.PlanAlternative(
                    plan.backendId(),
                    plan.convertedBytes(),
                    plan.instructions(),
                    delegationDescriptor
                )
            );
        }
        return alternatives;
    }

    private static DelegationDescriptor buildDelegationDescriptor(StagePlan plan) {
        if (plan.delegatedExpressions().isEmpty()) {
            return null;
        }
        // Extract treeShape and count from the ShardScanWithDelegationInstructionNode
        for (InstructionNode node : plan.instructions()) {
            if (node instanceof ShardScanWithDelegationInstructionNode delegationNode) {
                return new DelegationDescriptor(
                    delegationNode.getTreeShape(),
                    delegationNode.getDelegatedPredicateCount(),
                    plan.delegatedExpressions()
                );
            }
        }
        return null;
    }
}
