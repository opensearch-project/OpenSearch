/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.agg;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.DataTransferCapability;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Single-pass orchestrator for hash-shuffle aggregate execution. Sibling of
 * {@link org.opensearch.analytics.exec.join.HashShuffleDispatch}, but with only one producer
 * side instead of two — there is no left/right semantics for aggregate, just one stream of
 * partial-aggregate buffers per partition.
 *
 * <p>Wire-up steps before handoff to {@link QueryScheduler#execute}:
 * <ol>
 *   <li>Resolve {@code targetWorkerNodeIds} — one cluster data node per partition.</li>
 *   <li>Lift the FINAL aggregate into a worker stage via
 *       {@link HashShuffleAggregateDAGRewriter}.</li>
 *   <li>Append {@link ShuffleProducerInstructionNode} to every plan alternative on the producer
 *       stage with {@code side="left"} (the shuffle infrastructure uses the side label as the
 *       data-node-side routing key — for aggregate we only use one side).</li>
 *   <li>Append a {@link ShuffleWorkerSetupInstructionNode} (one placeholder per partition,
 *       expanded by {@code WorkerFragmentStageExecutionFactory}) plus {@link
 *       ShuffleScanInstructionNode}s — one per partition with {@code side="left"}. The setup
 *       passes {@code rightExpectedSenders=0} so the buffer's right-side latch fires
 *       immediately, leaving {@code awaitReady} to gate solely on the producer side.</li>
 *   <li>Hand the rewritten DAG to the scheduler.</li>
 * </ol>
 *
 * @opensearch.internal
 */
public final class HashShuffleAggregateDispatch {

    private static final Logger LOGGER = LogManager.getLogger(HashShuffleAggregateDispatch.class);

    private final QueryScheduler scheduler;
    private final ClusterService clusterService;
    private final ShuffleBufferManager shuffleBufferManager;
    private final CapabilityRegistry capabilityRegistry;

    public HashShuffleAggregateDispatch(
        QueryScheduler scheduler,
        ClusterService clusterService,
        ShuffleBufferManager shuffleBufferManager,
        CapabilityRegistry capabilityRegistry
    ) {
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.shuffleBufferManager = shuffleBufferManager;
        this.capabilityRegistry = capabilityRegistry;
    }

    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Stage producer,
        Stage consumer,
        java.util.function.Consumer<org.opensearch.analytics.exec.QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        assert producer.getRole() == Stage.StageRole.SHUFFLE_SCAN_AGG
            : "HashShuffleAggregateDispatch: producer role must be SHUFFLE_SCAN_AGG";

        int partitionCount = producer.getExchangeInfo().partitionCount();
        if (partitionCount < 1) {
            terminal.onFailure(
                new IllegalStateException("HashShuffleAggregateDispatch: partitionCount must be >= 1, got " + partitionCount)
            );
            return;
        }

        List<String> targetWorkerNodeIds = resolveTargetWorkerNodeIds(partitionCount);
        if (targetWorkerNodeIds.size() != partitionCount) {
            terminal.onFailure(
                new IllegalStateException(
                    "HashShuffleAggregateDispatch: resolved "
                        + targetWorkerNodeIds.size()
                        + " worker nodes but partitionCount="
                        + partitionCount
                )
            );
            return;
        }

        HashShuffleAggregateDAGRewriter.Rewritten rewritten = HashShuffleAggregateDAGRewriter.rewrite(
            dag,
            consumer,
            producer,
            targetWorkerNodeIds,
            capabilityRegistry
        );
        Stage workerStage = rewritten.worker();

        int expectedSenders = expectedSendersFor(producer);

        enrichProducerAlternatives(
            producer,
            ctx.queryId(),
            workerStage.getStageId(),
            partitionCount,
            targetWorkerNodeIds,
            capabilityRegistry
        );
        enrichWorkerAlternatives(workerStage, partitionCount, expectedSenders, ctx.queryId(), producer.getStageId());

        LOGGER.debug(
            "[HashShuffleAggregateDispatch] dispatching: query={}, workerStage={}, consumerStage={}, partitions={}, senders={}, targets={}",
            ctx.queryId(),
            workerStage.getStageId(),
            consumer.getStageId(),
            partitionCount,
            expectedSenders,
            targetWorkerNodeIds
        );

        org.opensearch.analytics.exec.QueryExecution exec = scheduler.execute(ctx.withDag(rewritten.dag()), terminal);
        if (queryExecutionSink != null) {
            queryExecutionSink.accept(exec);
        }
    }

    /** Round-robin one target node per partition across the cluster's data nodes. */
    private List<String> resolveTargetWorkerNodeIds(int partitionCount) {
        Map<String, DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes();
        if (dataNodes == null || dataNodes.isEmpty()) {
            return List.of();
        }
        List<String> nodeIds = new ArrayList<>(dataNodes.keySet());
        List<String> targets = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            targets.add(nodeIds.get(p % nodeIds.size()));
        }
        return targets;
    }

    /** Each producer task (one per shard) reports {@code isLast} once per partition. So the
     *  buffer's expected-sender count per partition equals the producer task count, which is
     *  the producer stage's resolved target count. */
    private int expectedSendersFor(Stage producer) {
        if (producer.getTargetResolver() == null) {
            return 1;
        }
        java.util.List<org.opensearch.analytics.planner.dag.ExecutionTarget> targets = producer.getTargetResolver()
            .resolve(clusterService.state(), null);
        return Math.max(targets.size(), 1);
    }

    static void enrichProducerAlternatives(
        Stage producerStage,
        String queryId,
        int consumerStageId,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        CapabilityRegistry registry
    ) {
        List<Integer> hashKeys = producerStage.getExchangeInfo().partitionKeyIndices();
        List<StagePlan> enriched = new ArrayList<>(producerStage.getPlanAlternatives().size());
        for (StagePlan sp : producerStage.getPlanAlternatives()) {
            // Drop scan-only alternatives (e.g. Lucene under prefer_metadata_driver on a keyword
            // key): only a backend declaring DataTransferCapability(PRODUCER) can run
            // SHUFFLE_PRODUCER. See HashShuffleDispatch.enrichProducerAlternatives.
            boolean canProduce = registry.getBackend(sp.backendId())
                .getCapabilityProvider()
                .dataTransferCapabilities()
                .stream()
                .anyMatch(cap -> cap.kind() == DataTransferCapability.Kind.PRODUCER);
            if (canProduce == false) {
                continue;
            }
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(existing.size() + 1);
            merged.addAll(existing);
            merged.add(new ShuffleProducerInstructionNode(hashKeys, partitionCount, targetWorkerNodeIds, queryId, consumerStageId, "left"));
            enriched.add(sp.withInstructions(merged));
        }
        if (enriched.isEmpty()) {
            throw new IllegalStateException(
                "No shuffle-producer-capable plan alternative on aggregate producer stage "
                    + producerStage.getStageId()
                    + "; none of its backends declare DataTransferCapability(PRODUCER)."
            );
        }
        producerStage.setPlanAlternatives(enriched);
    }

    static void enrichWorkerAlternatives(Stage workerStage, int partitionCount, int expectedSenders, String queryId, int producerStageId) {
        int workerStageId = workerStage.getStageId();
        // Convertor rewrites OpenSearchStageInputScan to "input-<producerStageId>" NamedScan.
        String inputId = canonicalInputId(producerStageId);
        List<StagePlan> enriched = new ArrayList<>(workerStage.getPlanAlternatives().size());
        for (StagePlan sp : workerStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(1 + existing.size() + partitionCount);
            // Placeholder setup with partition=-1 + rightExpectedSenders=0 (no right side).
            // WorkerFragmentStageExecutionFactory's per-partition filter replaces the placeholder
            // with a partition-specific copy carrying the partition index. The 0 right-side count
            // pre-fires the right latch in the buffer, leaving awaitReady gated solely on the
            // producer (left) side.
            merged.add(new ShuffleWorkerSetupInstructionNode(queryId, workerStageId, -1, expectedSenders, 0));
            merged.addAll(existing);
            for (int p = 0; p < partitionCount; p++) {
                merged.add(new ShuffleScanInstructionNode(inputId, p, expectedSenders, queryId, workerStageId, "left"));
            }
            enriched.add(sp.withInstructions(merged));
        }
        workerStage.setPlanAlternatives(enriched);
    }

    public static String canonicalInputId(int producerStageId) {
        return "input-" + producerStageId;
    }
}
