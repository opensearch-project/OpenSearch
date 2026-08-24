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
import org.opensearch.analytics.planner.dag.GeneralShuffleDAGRewriter;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.DataTransferCapability;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleSlots;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared hash-shuffle worker-tier primitives used by the general MPP scheduler (Option B — see
 * {@code MPP-GENERAL-SCHEDULING-DESIGN.md}). Extracted into a neutral home so the general scheduler
 * ({@link GeneralShuffleDAGRewriter} / {@link UnifiedDispatch}) depends
 * on these primitives directly rather than on the now-removed enumerated shape dispatchers.
 *
 * <p>Holds three things, all formerly living in the deleted cascade/hash-shuffle dispatchers:
 * <ul>
 *   <li>{@link WorkerLevel} — the per-level descriptor (worker stage + its shuffle {@link WorkerInput}s,
 *       each a producer + hash keys + slot label, plus partition count + target node list);</li>
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
     * One shuffle input of a worker level: the producer stage feeding it, the hash keys that producer
     * must partition its output on, and the slot label the two sides meet on (see {@link ShuffleSlots}).
     */
    public record WorkerInput(Stage producer, List<Integer> hashKeys, String slot) {
    }

    /**
     * One worker level's tier descriptor: the promoted worker stage, its shuffle inputs (two for a
     * hash join — more once an N-way consumer is promoted), the partition count, and the resolved
     * target node list (one node per partition).
     */
    public record WorkerLevel(Stage worker, List<WorkerInput> inputs, int partitionCount, List<String> targetNodeIds) {

        /** Binary convenience ctor: {@code left} → the {@code left} slot, {@code right} → {@code right}. */
        public WorkerLevel(
            Stage worker,
            Stage leftProducer,
            Stage rightProducer,
            List<Integer> leftKeys,
            List<Integer> rightKeys,
            int partitionCount,
            List<String> targetNodeIds
        ) {
            this(
                worker,
                List.of(
                    new WorkerInput(leftProducer, leftKeys, ShuffleSlots.LEFT),
                    new WorkerInput(rightProducer, rightKeys, ShuffleSlots.RIGHT)
                ),
                partitionCount,
                targetNodeIds
            );
        }

        /** The input on {@code slot}, or null if this level has none. */
        public WorkerInput input(String slot) {
            for (WorkerInput in : inputs) {
                if (in.slot().equals(slot)) {
                    return in;
                }
            }
            return null;
        }

        /** The {@code left}-slot producer stage (binary accessor retained for tests / logging). */
        public Stage leftProducer() {
            WorkerInput in = input(ShuffleSlots.LEFT);
            return in == null ? null : in.producer();
        }

        /** The {@code right}-slot producer stage (binary accessor retained for tests / logging). */
        public Stage rightProducer() {
            WorkerInput in = input(ShuffleSlots.RIGHT);
            return in == null ? null : in.producer();
        }
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

            // Producers ship to THIS worker's partitions (its node list), tagged with their slot. The hash
            // keys are THIS level's per-input keys — passed explicitly because an intermediate-worker
            // producer's own exchange info is SINGLETON (empty keys); it must partition its join OUTPUT on
            // the parent join's keys, not its (gathered) input's.
            Map<String, Integer> expectedBySlot = new LinkedHashMap<>();
            Map<String, Integer> producerStageIdBySlot = new LinkedHashMap<>();
            Map<String, byte[]> producerPlanBytesBySlot = new LinkedHashMap<>();
            for (WorkerInput input : level.inputs()) {
                expectedBySlot.put(input.slot(), expectedSendersFor(input.producer(), partitionCount, clusterService));
                producerStageIdBySlot.put(input.slot(), input.producer().getStageId());
                byte[] partialPlanBytes = partialAggregatePlanBytes(input.producer());
                if (partialPlanBytes != null) {
                    producerPlanBytesBySlot.put(input.slot(), partialPlanBytes);
                }
                enrichProducerAlternatives(
                    input.producer(),
                    input.hashKeys(),
                    ctx.queryId(),
                    workerStageId,
                    partitionCount,
                    targets,
                    input.slot(),
                    capabilityRegistry
                );
            }
            // Cost decision (Spark-style, made where the stats live — on the coordinator): if the build side
            // is estimated to exceed the sort-merge-join floor, tell the worker to use a spillable
            // sort-merge join instead of the non-spillable hash-join build. The build feeds from the LAST
            // input's shuffle (the right side of a binary join), so estimate from that producer's largest
            // scan subtree.
            Stage buildProducer = level.inputs().getLast().producer();
            long buildRows = subtreeMaxScanRows(buildProducer.getFragment());
            boolean preferHashJoin = buildRows < sortMergeJoinMinRows;

            // The worker consumes every producer's partitions. enrichWorkerAlternatives prepends a setup
            // placeholder and appends per-(partition,slot) scans; a producer instruction (added above when
            // this worker also feeds a higher level) stays AFTER the scans because enrichProducerAlternatives
            // appended it to the worker's own alternatives.
            enrichWorkerAlternatives(
                worker,
                partitionCount,
                expectedBySlot,
                ctx.queryId(),
                producerStageIdBySlot,
                preferHashJoin,
                producerPlanBytesBySlot
            );

            LOGGER.debug(
                "[ShuffleEnrichment] level worker={} producers={} partitions={} expectedSenders={} "
                    + "buildRows={} preferHashJoin={} targets={}",
                workerStageId,
                producerStageIdBySlot,
                partitionCount,
                expectedBySlot,
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
     * Appends every (partition × slot) {@link ShuffleScanInstructionNode} to the worker stage's plan
     * alternatives, prefixed by a {@link ShuffleWorkerSetupInstructionNode} that bootstraps a worker-mode
     * session context. The {@code WorkerFragmentStageExecutionFactory} filters this list down to the
     * ShuffleScan instructions for each task's partition before sending the per-task request — the setup
     * instruction passes through unfiltered.
     *
     * <p>{@code expectedSendersBySlot} and {@code producerStageIdBySlot} must have the SAME key set —
     * one entry per input stream the worker reads.
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
        // LinkedHashMap built left-then-right: slot order drives the per-partition scan emission order, so
        // Map.of (unordered) would make the interleaving non-deterministic.
        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put(ShuffleSlots.LEFT, leftExpectedSenders);
        expected.put(ShuffleSlots.RIGHT, rightExpectedSenders);
        Map<String, Integer> producers = new LinkedHashMap<>();
        producers.put(ShuffleSlots.LEFT, leftProducerStageId);
        producers.put(ShuffleSlots.RIGHT, rightProducerStageId);
        enrichWorkerAlternatives(workerStage, partitionCount, expected, queryId, producers, preferHashJoin);
    }

    public static void enrichWorkerAlternatives(
        Stage workerStage,
        int partitionCount,
        Map<String, Integer> expectedSendersBySlot,
        String queryId,
        Map<String, Integer> producerStageIdBySlot,
        boolean preferHashJoin
    ) {
        enrichWorkerAlternatives(
            workerStage,
            partitionCount,
            expectedSendersBySlot,
            queryId,
            producerStageIdBySlot,
            preferHashJoin,
            Map.of()
        );
    }

    /**
     * {@link #enrichWorkerAlternatives(Stage, int, Map, String, Map, boolean)} plus, per slot, the producer's
     * converted PARTIAL-aggregate plan bytes.
     *
     * <p>Only an AGGREGATE consumer needs them. A PARTIAL aggregate producer ships physical batches whose
     * state columns are named {@code <alias>[<state>]} (e.g. {@code total[sum]}), while the consumer FINAL's
     * Substrait {@code base_schema} declares Calcite's LOGICAL names ({@code total}); DataFusion's Substrait
     * consumer binds base_schema to the registered provider BY NAME, so registering the raw-IPC (physical)
     * schema fails the FINAL with {@code No field named total}. Given these bytes, {@code ShuffleScanHandler}
     * registers via {@code registerPartitionStreamOnSessionContextFromPartialPlan}, which re-lowers the
     * producer's plan to recover the logical names; the physically-named batches still feed in positionally.
     *
     * <p>A slot absent from the map gets {@code null}, which keeps the raw-IPC path — correct for a JOIN
     * producer, whose rows are raw and whose names already match the consumer.
     */
    public static void enrichWorkerAlternatives(
        Stage workerStage,
        int partitionCount,
        Map<String, Integer> expectedSendersBySlot,
        String queryId,
        Map<String, Integer> producerStageIdBySlot,
        boolean preferHashJoin,
        Map<String, byte[]> producerPlanBytesBySlot
    ) {
        if (!expectedSendersBySlot.keySet().equals(producerStageIdBySlot.keySet())) {
            throw new IllegalArgumentException(
                "ShuffleEnrichment: expectedSenders slots "
                    + expectedSendersBySlot.keySet()
                    + " disagree with producer-stage slots "
                    + producerStageIdBySlot.keySet()
            );
        }
        int workerStageId = workerStage.getStageId();
        // The fragment convertor strips OpenSearchShuffleExchange, so the worker fragment ends up with one
        // OpenSearchStageInputScan leaf per input, which the convertor rewrites to an
        // "input-<producerStageId>" NamedScan. The handler must register its streaming table under that
        // exact name.
        int slotCount = producerStageIdBySlot.size();
        List<StagePlan> enriched = new ArrayList<>(workerStage.getPlanAlternatives().size());
        for (StagePlan sp : workerStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(1 + existing.size() + slotCount * partitionCount);
            // Placeholder setup with partition=-1 — the per-task filter in
            // WorkerFragmentStageExecutionFactory replaces this with a partition-specific copy carrying every
            // slot's expected sender count. We don't know the partition at this step (one alternative serves
            // all partitions; per-task filtering picks the right one).
            merged.add(new ShuffleWorkerSetupInstructionNode(queryId, workerStageId, -1, expectedSendersBySlot, preferHashJoin));
            merged.addAll(existing);
            for (int p = 0; p < partitionCount; p++) {
                for (Map.Entry<String, Integer> e : producerStageIdBySlot.entrySet()) {
                    String slot = e.getKey();
                    merged.add(
                        new ShuffleScanInstructionNode(
                            canonicalInputId(e.getValue()),
                            p,
                            expectedSendersBySlot.get(slot),
                            queryId,
                            workerStageId,
                            slot,
                            producerPlanBytesBySlot.get(slot)
                        )
                    );
                }
            }
            enriched.add(sp.withInstructions(merged));
        }
        workerStage.setPlanAlternatives(enriched);
    }

    /**
     * The producer's converted plan bytes when it ships PARTIAL AGGREGATE state, else {@code null}.
     *
     * <p>Gated on the producer's fragment actually containing a {@code PARTIAL} {@link OpenSearchAggregate}:
     * that is what makes its output columns state-named ({@code total[sum]}) and so requires the consumer to
     * register by the re-lowered LOGICAL schema. A join producer ships raw rows and must keep the raw-IPC
     * path, so it returns {@code null}.
     *
     * <p>Returns {@code null} when the producer has no converted bytes yet (an un-forked stage in a unit
     * test), which degrades to the existing behaviour rather than failing enrichment.
     */
    private static byte[] partialAggregatePlanBytes(Stage producer) {
        if (!containsPartialAggregate(producer.getFragment())) {
            return null;
        }
        if (producer.getPlanAlternatives().isEmpty()) {
            return null;
        }
        return producer.getPlanAlternatives().getFirst().convertedBytes();
    }

    /** True when {@code node}'s subtree contains a {@code PARTIAL} aggregate. */
    private static boolean containsPartialAggregate(RelNode node) {
        if (node == null) {
            return false;
        }
        RelNode unwrapped = RelNodeUtils.unwrapHep(node);
        if (unwrapped instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
            return true;
        }
        for (RelNode input : unwrapped.getInputs()) {
            if (containsPartialAggregate(input)) {
                return true;
            }
        }
        return false;
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
