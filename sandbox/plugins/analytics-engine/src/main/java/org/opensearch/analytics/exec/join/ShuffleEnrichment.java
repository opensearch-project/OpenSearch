/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.DataTransferCapability;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared hash-shuffle worker-tier primitives used by the general MPP scheduler (Option B — see
 * {@code MPP-GENERAL-SCHEDULING-DESIGN.md}). Extracted into a neutral home so the general scheduler
 * ({@link GeneralShuffleDAGRewriter} / {@link UnifiedDispatch}) depends
 * on these primitives directly rather than on the now-removed enumerated shape dispatchers.
 *
 * <p>Holds three things, all formerly living in the deleted cascade/hash-shuffle dispatchers:
 * <ul>
 *   <li>{@link WorkerLevel} — the per-join-level descriptor (worker stage + its two shuffle producers +
 *       per-side hash keys + partition count + target node list);</li>
 *   <li>{@link #enrichLevels} — attaches each level's producer/scan/worker shuffle instructions bottom-up;</li>
 *   <li>{@link #enrichProducerAlternatives} / {@link #enrichWorkerAlternatives} / {@link #canonicalInputId}
 *       — the per-stage instruction attachment + the canonical {@code input-<producerStageId>} naming the
 *       fragment convertor emits.</li>
 * </ul>
 *
 * <p>An INTERMEDIATE worker is BOTH a shuffle consumer (of the level below — gets setup + scan
 * instructions) AND a shuffle producer (to the level above — gets a producer instruction); the two
 * enrichments compose in the order {@code [setup, scan…, producer]} so the worker reads its children's
 * partitions, runs its join, and ships the result to its parent worker's partitions. Only the top worker
 * gathers (SINGLETON) to the coordinator's reduce.
 *
 * @opensearch.internal
 */
public final class ShuffleEnrichment {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleEnrichment.class);

    private ShuffleEnrichment() {}

    /**
     * One join level's worker-tier descriptor: the promoted worker stage, its two shuffle producer
     * stages, the per-side hash keys the producers must partition their output on, the partition count,
     * and the resolved target node list (one node per partition).
     */
    public record WorkerLevel(Stage worker, Stage leftProducer, Stage rightProducer, List<Integer> leftKeys, List<Integer> rightKeys,
        int partitionCount, List<String> targetNodeIds) {
    }

    /**
     * Enriches each worker level bottom-up with its shuffle producer / scan / worker instructions. A
     * producer feeding an intermediate worker may itself be a worker (its instructions already carry
     * setup+scan); the producer instruction is appended so the order stays {@code [setup, scan…, producer]}.
     */
    public static void enrichLevels(
        List<WorkerLevel> levels,
        QueryContext ctx,
        ClusterService clusterService,
        CapabilityRegistry capabilityRegistry,
        long sortMergeJoinMinRows
    ) {
        for (WorkerLevel level : levels) {
            Stage worker = level.worker();
            int workerStageId = worker.getStageId();
            List<String> targets = level.targetNodeIds();
            int partitionCount = level.partitionCount();

            // Fail fast if the resolved target list doesn't have exactly one node per partition
            // (empty cluster / undersized resolution). Without this, a worker tier could be built
            // with zero/too-few tasks or producers could ship to a short node list — silently
            // wrong results or an IndexOutOfBounds deep in dispatch.
            if (targets.size() != partitionCount) {
                throw new IllegalStateException(
                    "ShuffleEnrichment: worker stage "
                        + workerStageId
                        + " resolved "
                        + targets.size()
                        + " target nodes but partitionCount="
                        + partitionCount
                );
            }

            int leftExpected = expectedSendersFor(level.leftProducer(), partitionCount, clusterService);
            int rightExpected = expectedSendersFor(level.rightProducer(), partitionCount, clusterService);

            // Producers ship to THIS worker's partitions (its node list), tagged with the side. The hash
            // keys are THIS join level's per-side keys — passed explicitly because an intermediate-worker
            // producer's own exchange info is SINGLETON (empty keys); it must partition its join OUTPUT on
            // the parent join's keys, not its (gathered) input's.
            enrichProducerAlternatives(
                level.leftProducer(),
                level.leftKeys(),
                ctx.queryId(),
                workerStageId,
                partitionCount,
                targets,
                "left",
                capabilityRegistry
            );
            enrichProducerAlternatives(
                level.rightProducer(),
                level.rightKeys(),
                ctx.queryId(),
                workerStageId,
                partitionCount,
                targets,
                "right",
                capabilityRegistry
            );
            // Cost decision (Spark-style, made where the stats live — on the coordinator): if the build side
            // (right input) is estimated to exceed the sort-merge-join floor, tell the worker to use a
            // spillable sort-merge join instead of the non-spillable hash-join build. Estimated from the
            // right producer's largest scan subtree (the build feeds from the right producer's shuffle).
            long buildRows = subtreeMaxScanRows(level.rightProducer().getFragment());
            boolean preferHashJoin = buildRows < sortMergeJoinMinRows;

            // The worker consumes its two producers' partitions. enrichWorkerAlternatives prepends a setup
            // placeholder and appends per-(partition,side) scans; a producer instruction (added above when
            // this worker also feeds a higher level) stays AFTER the scans because enrichProducerAlternatives
            // appended it to the worker's own alternatives.
            enrichWorkerAlternatives(
                worker,
                partitionCount,
                leftExpected,
                rightExpected,
                ctx.queryId(),
                level.leftProducer().getStageId(),
                level.rightProducer().getStageId(),
                preferHashJoin
            );

            LOGGER.debug(
                "[ShuffleEnrichment] level worker={} left={} right={} partitions={} leftSenders={} rightSenders={} "
                    + "buildRows={} preferHashJoin={} targets={}",
                workerStageId,
                level.leftProducer().getStageId(),
                level.rightProducer().getStageId(),
                partitionCount,
                leftExpected,
                rightExpected,
                buildRows,
                preferHashJoin,
                targets
            );
        }
    }

    /**
     * Per-(partition, side) sender count the consumer worker expects. A producer task — whether a leaf
     * shard scan (one task per shard) or an intermediate worker (one task per partition) — ships to ALL
     * partitions and marks isLast once per partition, so the count is the producer's task count. Falls back
     * to the worker's own {@code partitionCount} when the producer has no resolver.
     */
    private static int expectedSendersFor(Stage producer, int fallbackPartitionCount, ClusterService clusterService) {
        if (producer.getTargetResolver() == null) {
            return Math.max(fallbackPartitionCount, 1);
        }
        int n = producer.getTargetResolver().resolve(clusterService.state(), null).size();
        return Math.max(n, 1);
    }

    /**
     * Appends a {@link ShuffleProducerInstructionNode} to every shuffle-producer-capable plan alternative
     * on the producer stage, partitioning its output on {@code hashKeys} (the consumer join level's per-side
     * keys — an intermediate worker producer's own exchange info is SINGLETON, so the keys it must partition
     * on are threaded in by the caller). A scan-only alternative (no {@code DataTransferCapability(PRODUCER)})
     * is dropped so {@code PlanAlternativeSelector} can't pick a driver that throws at execution.
     */
    public static void enrichProducerAlternatives(
        Stage producerStage,
        List<Integer> hashKeys,
        String queryId,
        int consumerStageId,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        String side,
        CapabilityRegistry registry
    ) {
        List<StagePlan> enriched = new ArrayList<>(producerStage.getPlanAlternatives().size());
        for (StagePlan sp : producerStage.getPlanAlternatives()) {
            if (canDriveShuffleProducer(registry, sp.backendId()) == false) {
                continue;
            }
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(existing.size() + 1);
            merged.addAll(existing);
            merged.add(new ShuffleProducerInstructionNode(hashKeys, partitionCount, targetWorkerNodeIds, queryId, consumerStageId, side));
            enriched.add(sp.withInstructions(merged));
        }
        if (enriched.isEmpty()) {
            throw new IllegalStateException(
                "No shuffle-producer-capable plan alternative on producer stage "
                    + producerStage.getStageId()
                    + " (side="
                    + side
                    + "); none of its backends declare DataTransferCapability(PRODUCER)."
            );
        }
        producerStage.setPlanAlternatives(enriched);
    }

    private static boolean canDriveShuffleProducer(CapabilityRegistry registry, String backendId) {
        return registry.getBackend(backendId)
            .getCapabilityProvider()
            .dataTransferCapabilities()
            .stream()
            .anyMatch(cap -> cap.kind() == DataTransferCapability.Kind.PRODUCER);
    }

    /**
     * Appends every (partition × side) {@link ShuffleScanInstructionNode} to the worker stage's plan
     * alternatives, prefixed by a {@link ShuffleWorkerSetupInstructionNode} that bootstraps a worker-mode
     * session context. The {@code WorkerFragmentStageExecutionFactory} filters this list down to the two
     * ShuffleScan instructions for each task's partition before sending the per-task request — the setup
     * instruction passes through unfiltered.
     */
    public static void enrichWorkerAlternatives(
        Stage workerStage,
        int partitionCount,
        int leftExpectedSenders,
        int rightExpectedSenders,
        String queryId,
        int leftProducerStageId,
        int rightProducerStageId,
        boolean preferHashJoin
    ) {
        int workerStageId = workerStage.getStageId();
        // The fragment convertor strips OpenSearchShuffleExchange, so the worker fragment ends up with two
        // OpenSearchStageInputScan leaves the convertor rewrites to "input-<producerStageId>" NamedScans.
        // The handler must register its streaming table under that exact name.
        String leftInputId = canonicalInputId(leftProducerStageId);
        String rightInputId = canonicalInputId(rightProducerStageId);
        List<StagePlan> enriched = new ArrayList<>(workerStage.getPlanAlternatives().size());
        for (StagePlan sp : workerStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(1 + existing.size() + 2 * partitionCount);
            // Placeholder setup with partition=-1 — the per-task filter in
            // WorkerFragmentStageExecutionFactory replaces this with a partition-specific copy carrying both
            // sides' expected sender counts. We don't know the partition at this step (one alternative serves
            // all partitions; per-task filtering picks the right one).
            merged.add(
                new ShuffleWorkerSetupInstructionNode(queryId, workerStageId, -1, leftExpectedSenders, rightExpectedSenders, preferHashJoin)
            );
            merged.addAll(existing);
            for (int p = 0; p < partitionCount; p++) {
                merged.add(new ShuffleScanInstructionNode(leftInputId, p, leftExpectedSenders, queryId, workerStageId, "left"));
                merged.add(new ShuffleScanInstructionNode(rightInputId, p, rightExpectedSenders, queryId, workerStageId, "right"));
            }
            enriched.add(sp.withInstructions(merged));
        }
        workerStage.setPlanAlternatives(enriched);
    }

    /** Canonical {@code "input-<producerStageId>"} name the fragment convertor emits when it rewrites the
     *  consumer fragment's {@code OpenSearchStageInputScan} leaves. The handler must register streaming
     *  tables under this exact name so the worker plan's NamedScan binds correctly. */
    public static String canonicalInputId(int producerStageId) {
        return "input-" + producerStageId;
    }

    /**
     * Largest {@link OpenSearchTableScan} row count in {@code node}'s subtree (0 when no scan / unknown).
     * Used to estimate a worker join's build-side size for the sort-merge-join decision — mirrors the
     * estimate {@code DistributionEnforcementPass} uses for the distribute floor.
     */
    static long subtreeMaxScanRows(RelNode node) {
        if (node == null) {
            return 0L;
        }
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n instanceof OpenSearchTableScan scan) {
            return Math.max(0L, (long) scan.getTable().getRowCount());
        }
        long max = 0L;
        for (RelNode input : n.getInputs()) {
            max = Math.max(max, subtreeMaxScanRows(input));
        }
        return max;
    }
}
