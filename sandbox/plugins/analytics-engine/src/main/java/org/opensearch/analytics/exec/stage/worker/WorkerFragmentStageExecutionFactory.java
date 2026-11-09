/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.worker;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.WorkerFragmentRequest;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionFactory;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.dag.WorkerExecutionTarget;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builds a {@link WorkerFragmentStageExecution} that fans out one fragment request per
 * resolved {@link WorkerExecutionTarget}. Each per-task request carries only the two
 * {@link ShuffleScanInstructionNode}s for that target's partition — the consumer-stage's
 * full instruction list (with all 2N partition-side scans appended) is filtered down so
 * each worker session registers only its own per-partition streaming tables.
 *
 * @opensearch.internal
 */
public final class WorkerFragmentStageExecutionFactory implements StageExecutionFactory {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;

    public WorkerFragmentStageExecutionFactory(ClusterService clusterService, AnalyticsSearchTransportService transport) {
        this.clusterService = clusterService;
        this.transport = transport;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        final String queryId = config.queryId();
        final int stageId = stage.getStageId();
        Function<WorkerExecutionTarget, WorkerFragmentRequest> requestBuilder = target -> {
            int partitionIndex = target.partitionIndex();
            List<FragmentExecutionRequest.PlanAlternative> filtered = filterPlanAlternativesForPartition(stage, partitionIndex);
            return new WorkerFragmentRequest(queryId, stageId, partitionIndex, filtered);
        };
        return new WorkerFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, transport);
    }

    /**
     * Filters every {@link StagePlan}'s instruction list to keep only the ShuffleScan
     * instructions for {@code partitionIndex}. Replaces the partition-agnostic
     * {@link ShuffleWorkerSetupInstructionNode} placeholder with a partition-specific copy so
     * the setup handler can eagerly register both sides' expected sender counts on the buffer
     * BEFORE any ShuffleScanHandler calls awaitReady.
     *
     * <p>The returned alternatives are the wire payload sent to the worker task; the
     * data-node-side handler chain runs setup → register-left-stream → register-right-stream →
     * execute.
     */
    private static List<FragmentExecutionRequest.PlanAlternative> filterPlanAlternativesForPartition(Stage stage, int partitionIndex) {
        List<FragmentExecutionRequest.PlanAlternative> alts = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            // Compute per-partition setup parameters from the partition's own scan instructions
            // (left + right). The expected sender counts come from the ShuffleScan instructions
            // (each scan carries the count for its side); the placeholder setup at the head
            // doesn't have them yet because it was added before per-partition info was known.
            // Default to -1 ("leave unchanged" in setExpectedSenders) and let the placeholder's
            // own value be the fallback for sides with no ShuffleScan — that's how the M3
            // single-side aggregate path tells the worker buffer that the right side has zero
            // expected senders (placeholder carries rightExpectedSenders=0).
            int leftExpected = -1;
            int rightExpected = -1;
            String queryId = null;
            int targetStageId = -1;
            boolean preferHashJoin = true;
            ShuffleWorkerSetupInstructionNode placeholderSetup = null;
            for (InstructionNode node : plan.instructions()) {
                if (node instanceof ShuffleScanInstructionNode scan && scan.getShufflePartitionIndex() == partitionIndex) {
                    if ("left".equals(scan.getSide())) {
                        leftExpected = scan.getExpectedSenders();
                    } else if ("right".equals(scan.getSide())) {
                        rightExpected = scan.getExpectedSenders();
                    }
                    if (queryId == null) {
                        queryId = scan.getQueryId();
                        targetStageId = scan.getTargetStageId();
                    }
                } else if (node instanceof ShuffleWorkerSetupInstructionNode setup) {
                    placeholderSetup = setup;
                }
            }
            // For sides with no ShuffleScan instruction (M3 agg shuffle has only "left" scans),
            // fall back to the placeholder's stored value so a configured "rightExpected=0"
            // pre-fires the right-side latch and doesn't get clobbered to -1.
            if (placeholderSetup != null) {
                if (leftExpected < 0) leftExpected = placeholderSetup.getLeftExpectedSenders();
                if (rightExpected < 0) rightExpected = placeholderSetup.getRightExpectedSenders();
                if (queryId == null) {
                    queryId = placeholderSetup.getQueryId();
                    targetStageId = placeholderSetup.getTargetStageId();
                }
                // Carry the coordinator's per-worker-stage sort-merge-join decision through the
                // placeholder → partition-specific rebuild below.
                preferHashJoin = placeholderSetup.getPreferHashJoin();
            }

            List<InstructionNode> filtered = new ArrayList<>();
            for (InstructionNode node : plan.instructions()) {
                if (node instanceof ShuffleScanInstructionNode scan) {
                    if (scan.getShufflePartitionIndex() == partitionIndex) {
                        filtered.add(scan);
                    }
                } else if (node instanceof ShuffleWorkerSetupInstructionNode) {
                    // Replace the placeholder setup with a partition-specific copy carrying
                    // both sides' expected counts.
                    if (queryId != null) {
                        filtered.add(
                            new ShuffleWorkerSetupInstructionNode(
                                queryId,
                                targetStageId,
                                partitionIndex,
                                leftExpected,
                                rightExpected,
                                preferHashJoin
                            )
                        );
                    } else {
                        filtered.add(node);
                    }
                } else {
                    filtered.add(node);
                }
            }
            alts.add(
                new FragmentExecutionRequest.PlanAlternative(
                    plan.backendId(),
                    plan.convertedBytes(),
                    filtered,
                    /* delegationDescriptor */ null
                )
            );
        }
        return alts;
    }
}
