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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Builds a {@link WorkerFragmentStageExecution} that fans out one fragment request per
 * resolved {@link WorkerExecutionTarget}. Each per-task request carries only the
 * {@link ShuffleScanInstructionNode}s for that target's partition — the consumer-stage's
 * full instruction list (with every partition × slot scan appended) is filtered down so
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
     * the setup handler can eagerly declare EVERY slot's expected sender count on the buffer
     * BEFORE any ShuffleScanHandler calls awaitReady.
     *
     * <p>The returned alternatives are the wire payload sent to the worker task; the
     * data-node-side handler chain runs setup → one register-stream per slot → execute.
     */
    private static List<FragmentExecutionRequest.PlanAlternative> filterPlanAlternativesForPartition(Stage stage, int partitionIndex) {
        List<FragmentExecutionRequest.PlanAlternative> alts = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            // Compute per-partition setup parameters from the partition's own scan instructions —
            // one per slot the consumer reads. The expected sender counts come from the ShuffleScan
            // instructions (each carries the count for its own slot); the placeholder setup at the
            // head doesn't have them yet because it was added before per-partition info was known.
            Map<String, Integer> expectedBySlot = new LinkedHashMap<>();
            String queryId = null;
            int targetStageId = -1;
            boolean preferHashJoin = true;
            ShuffleWorkerSetupInstructionNode placeholderSetup = null;
            for (InstructionNode node : plan.instructions()) {
                if (node instanceof ShuffleScanInstructionNode scan && scan.getShufflePartitionIndex() == partitionIndex) {
                    expectedBySlot.put(scan.getSide(), scan.getExpectedSenders());
                    if (queryId == null) {
                        queryId = scan.getQueryId();
                        targetStageId = scan.getTargetStageId();
                    }
                } else if (node instanceof ShuffleWorkerSetupInstructionNode setup) {
                    placeholderSetup = setup;
                }
            }
            // Merge in any slot the placeholder declares that has NO ShuffleScan instruction. That is
            // how the M3 agg-shuffle path (only a "left" scan) tells the worker buffer the right slot
            // has zero expected senders, pre-firing its latch. putIfAbsent so a slot's own scan
            // instruction always wins over the placeholder's pre-partition estimate.
            if (placeholderSetup != null) {
                placeholderSetup.getExpectedSendersBySlot().forEach(expectedBySlot::putIfAbsent);
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
                    // Replace the placeholder setup with a partition-specific copy carrying every
                    // slot's expected count.
                    if (queryId != null) {
                        filtered.add(
                            new ShuffleWorkerSetupInstructionNode(queryId, targetStageId, partitionIndex, expectedBySlot, preferHashJoin)
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
