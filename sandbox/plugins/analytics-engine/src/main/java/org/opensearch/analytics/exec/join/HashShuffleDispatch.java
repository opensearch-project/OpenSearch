/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

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
 * Single-pass orchestrator for hash-shuffle join execution. Sibling of {@link BroadcastDispatch},
 * but unlike broadcast there is no build-then-probe sequencing: producers (the two
 * {@link Stage.StageRole#SHUFFLE_SCAN_LEFT} / {@link Stage.StageRole#SHUFFLE_SCAN_RIGHT} stages)
 * and the consumer/worker stage dispatch concurrently. Each producer hash-partitions its scan
 * output and ships partition {@code i} to {@code targetWorkerNodeIds[i]}; the consumer's worker
 * tasks block on the per-partition {@link ShuffleBufferManager.ShuffleBuffer} until both sides
 * complete, then run the hash-join on the gathered buckets.
 *
 * <p>Wire-up steps before handoff to {@link QueryScheduler#execute}:
 * <ol>
 *   <li>Resolve {@code targetWorkerNodeIds} — one cluster data node per partition. The list
 *       length is exactly {@code partitionCount}.</li>
 *   <li>Pre-allocate a {@link ShuffleBufferManager.ShuffleBuffer} on each target node keyed by
 *       {@code (queryId, consumerStageId, partitionIndex)} so the consumer's
 *       {@code ShuffleBuffer.awaitReady} succeeds even when the consumer task starts before the
 *       producer's first batch lands. (Today {@link ShuffleBufferManager#getOrCreateBuffer}
 *       handles the on-demand case lazily; the dispatcher pre-allocates so we have a place to
 *       set {@code expectedSenders} before any producer fires.)</li>
 *   <li>Append {@link ShuffleProducerInstructionNode} to every plan alternative on each
 *       producer stage, carrying the keys, partition count, target node list, and
 *       {@code "left"}/{@code "right"} side label.</li>
 *   <li>Append {@link ShuffleScanInstructionNode}s to the consumer stage's plan alternatives —
 *       one per side — so the worker registers a {@code NamedScan} per side under
 *       {@code "shuffle-<consumerStageId>-left-<partition>"} /
 *       {@code "shuffle-<consumerStageId>-right-<partition>"}.</li>
 *   <li>Hand the unchanged DAG to {@link QueryScheduler#execute}; the standard walker
 *       dispatches all three stages concurrently because none has a parent dependency the
 *       walker can't satisfy locally.</li>
 * </ol>
 *
 * <p>Cancellation: {@link QueryScheduler#execute} already wires the
 * {@code AnalyticsQueryTask.onCancelCallback} to drive the walker-level cancel cascade, so we
 * don't install a separate one — every stage execution registered through the standard
 * builder respects task cancellation.
 *
 * @opensearch.internal
 */
public final class HashShuffleDispatch {

    private static final Logger LOGGER = LogManager.getLogger(HashShuffleDispatch.class);

    private final QueryScheduler scheduler;
    private final ClusterService clusterService;
    private final ShuffleBufferManager shuffleBufferManager;
    private final CapabilityRegistry capabilityRegistry;

    public HashShuffleDispatch(
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

    /**
     * Drives the hash-shuffle DAG end-to-end. The DAG has already been cut by {@code DAGBuilder}
     * with {@link Stage.StageRole#SHUFFLE_SCAN_LEFT} / {@code SHUFFLE_SCAN_RIGHT} on the producer
     * children of a consumer stage. Caller is responsible for selecting the right strategy
     * (this class is invoked only when {@link JoinStrategyAdvisor#observe} returns
     * {@link MppStrategy#HASH_SHUFFLE}).
     *
     * @param ctx the query context owning the DAG.
     * @param dag the post-CBO DAG produced by {@code DAGBuilder}; not mutated structurally.
     * @param leftProducer the SHUFFLE_SCAN_LEFT child stage.
     * @param rightProducer the SHUFFLE_SCAN_RIGHT child stage.
     * @param consumer the parent of both producers (the join's stage).
     * @param queryExecutionSink optional callback invoked with the {@link
     *     org.opensearch.analytics.exec.QueryExecution} returned by the inner
     *     {@code scheduler.execute} call — used by {@code DefaultPlanExecutor.executeWithProfile}
     *     to populate its {@code execRef} so the profile listener can snapshot stage timings.
     *     May be null when profiling is disabled.
     * @param terminal fires on overall completion — success when the root stage emits joined
     *     rows, failure on any producer-side or consumer-side error.
     */
    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Stage leftProducer,
        Stage rightProducer,
        Stage consumer,
        java.util.function.Consumer<org.opensearch.analytics.exec.QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        assert leftProducer.getRole() == Stage.StageRole.SHUFFLE_SCAN_LEFT
            : "HashShuffleDispatch: leftProducer role must be SHUFFLE_SCAN_LEFT";
        assert rightProducer.getRole() == Stage.StageRole.SHUFFLE_SCAN_RIGHT
            : "HashShuffleDispatch: rightProducer role must be SHUFFLE_SCAN_RIGHT";

        int partitionCount = leftProducer.getExchangeInfo().partitionCount();
        if (partitionCount != rightProducer.getExchangeInfo().partitionCount()) {
            terminal.onFailure(
                new IllegalStateException(
                    "HashShuffleDispatch: left/right producers disagree on partitionCount ("
                        + partitionCount
                        + " vs "
                        + rightProducer.getExchangeInfo().partitionCount()
                        + ")"
                )
            );
            return;
        }
        if (partitionCount < 1) {
            terminal.onFailure(new IllegalStateException("HashShuffleDispatch: partitionCount must be >= 1, got " + partitionCount));
            return;
        }

        List<String> targetWorkerNodeIds = resolveTargetWorkerNodeIds(partitionCount);
        if (targetWorkerNodeIds.size() != partitionCount) {
            terminal.onFailure(
                new IllegalStateException(
                    "HashShuffleDispatch: resolved " + targetWorkerNodeIds.size() + " worker nodes but partitionCount=" + partitionCount
                )
            );
            return;
        }

        // 1) Lift the join into a new SHUFFLE_WORKER stage between the producers and the existing
        // consumer. The rewriter rebuilds the DAG, swaps the consumer's child list, and
        // re-runs FragmentConversionDriver.convertAll on the rewritten graph.
        HashShuffleDAGRewriter.Rewritten rewritten = HashShuffleDAGRewriter.rewrite(
            dag,
            consumer,
            leftProducer,
            rightProducer,
            targetWorkerNodeIds,
            capabilityRegistry
        );
        Stage workerStage = rewritten.worker();

        // 2) Compute per-side expected sender counts: each producer task (one per shard) ships
        // to all N partitions and reports isLast once per (partition, side). The
        // consumer-side ShuffleScanHandler calls setExpectedSenders on the WORKER node's
        // buffer (where data actually lands). We thread the count through the
        // ShuffleScanInstructionNode as expectedSenders so the worker handler picks it up.
        int leftExpectedSenders = expectedSendersFor(leftProducer);
        int rightExpectedSenders = expectedSendersFor(rightProducer);

        // 3) Append shuffle instructions to plan alternatives. Producers learn the partition
        // targets and side label; the WORKER stage (not the original consumer) gets the
        // per-side ShuffleScan instructions plus a SETUP_SHUFFLE_WORKER instruction at the
        // head to bootstrap a worker-mode session context.
        enrichProducerAlternatives(
            leftProducer,
            ctx.queryId(),
            workerStage.getStageId(),
            partitionCount,
            targetWorkerNodeIds,
            "left",
            capabilityRegistry
        );
        enrichProducerAlternatives(
            rightProducer,
            ctx.queryId(),
            workerStage.getStageId(),
            partitionCount,
            targetWorkerNodeIds,
            "right",
            capabilityRegistry
        );
        enrichWorkerAlternatives(
            workerStage,
            partitionCount,
            leftExpectedSenders,
            rightExpectedSenders,
            ctx.queryId(),
            leftProducer.getStageId(),
            rightProducer.getStageId()
        );

        LOGGER.debug(
            "[HashShuffleDispatch] dispatching: query={}, workerStage={}, consumerStage={}, partitions={}, leftSenders={}, rightSenders={}, targets={}",
            ctx.queryId(),
            workerStage.getStageId(),
            consumer.getStageId(),
            partitionCount,
            leftExpectedSenders,
            rightExpectedSenders,
            targetWorkerNodeIds
        );

        // 4) Hand the rewritten DAG to the scheduler. Producers dispatch concurrently with the
        // worker stage; each worker task blocks on ShuffleBuffer.awaitReady until both
        // producer sides have shipped their isLast for that partition. Worker results stream
        // up to the coordinator's reduce sink as plain Arrow batches.
        org.opensearch.analytics.exec.QueryExecution exec = scheduler.execute(ctx.withDag(rewritten.dag()), terminal);
        if (queryExecutionSink != null) {
            queryExecutionSink.accept(exec);
        }
    }

    /**
     * Resolves one target worker node per partition. Round-robins across the cluster's data
     * nodes. With more partitions than data nodes some nodes host multiple partitions; the
     * shuffle buffer keys partitions independently so this is safe.
     */
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

    /**
     * Counts how many tasks the producer stage will run. For a leaf shard-scan stage this is the
     * shard count of the underlying scan. We delegate to the stage's plan-alternative count
     * (post-fork), which is one alternative per backend so doesn't tell us shard count;
     * instead we read the producer's exchange info as a proxy for "how many producer tasks will
     * mark isLast on each (partition, side)" — partitionCount itself is the right answer because
     * each producer task hashes its rows to all partitions and marks isLast once per partition.
     *
     * <p>TODO (M2 follow-up): once the producer handler is wired we can revisit — if isLast is
     * issued once per shard task (not once per partition per task), we'd want shardCount here
     * instead. The current value works because every producer task reports isLast for every
     * partition it produced rows for.
     */
    private int expectedSendersFor(Stage producer) {
        // Each producer task (one per shard) ships data to ALL N partitions and reports isLast
        // once per partition. So per-partition we expect exactly shardCount senders. Resolve via
        // the stage's TargetResolver — ShardTargetResolver returns one ExecutionTarget per shard.
        if (producer.getTargetResolver() == null) {
            return 1;
        }
        java.util.List<org.opensearch.analytics.planner.dag.ExecutionTarget> targets = producer.getTargetResolver()
            .resolve(clusterService.state(), null);
        return Math.max(targets.size(), 1);
    }

    /**
     * Appends a {@link ShuffleProducerInstructionNode} to every plan alternative on the producer
     * stage. Package-private for unit testing.
     */
    static void enrichProducerAlternatives(
        Stage producerStage,
        String queryId,
        int consumerStageId,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        String side,
        CapabilityRegistry registry
    ) {
        // Single-level path: the producer is a leaf shard stage whose own exchange info carries the
        // hash keys (DAGBuilder.cutShuffle set them to the join's per-side keys).
        enrichProducerAlternatives(
            producerStage,
            producerStage.getExchangeInfo().partitionKeyIndices(),
            queryId,
            consumerStageId,
            partitionCount,
            targetWorkerNodeIds,
            side,
            registry
        );
    }

    /**
     * Overload taking explicit {@code hashKeys}. The cascade dispatcher uses this because an
     * INTERMEDIATE worker producer's own exchange info is SINGLETON (it gathers its children) — the
     * keys it must PARTITION ITS OUTPUT on are the parent join's per-side keys, threaded in by the
     * caller (see {@code CascadeShuffleDispatch}). For leaf producers the keys equal the producer
     * stage's own exchange-info keys, so both call sites agree.
     */
    static void enrichProducerAlternatives(
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
            // Only a backend that can serialize+ship hash partitions (declares
            // DataTransferCapability(PRODUCER)) can run SHUFFLE_PRODUCER. A scan-only alternative
            // (e.g. Lucene, kept viable for a keyword scan under prefer_metadata_driver) is dropped
            // here — keeping it would let PlanAlternativeSelector pick a driver that throws
            // "Lucene driver does not handle instruction type: SHUFFLE_PRODUCER" at execution.
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
     * Appends one {@link ShuffleScanInstructionNode} per side per partition to every plan
     * alternative on the consumer stage. The consumer's worker plan references each side as a
     * separate {@code NamedScan}, and the handler resolves each name to a {@code StreamingTable}
     * backed by the corresponding partition's buffer slice. Package-private for unit testing.
     */
    /**
     * Appends every (partition × side) {@link ShuffleScanInstructionNode} to the worker
     * stage's plan alternatives, prefixed by a {@link ShuffleWorkerSetupInstructionNode} that
     * bootstraps a worker-mode session context. The
     * {@link org.opensearch.analytics.exec.stage.worker.WorkerFragmentStageExecutionFactory}
     * filters this list down to the two ShuffleScan instructions for each task's partition
     * before sending the per-task request — the setup instruction passes through unfiltered.
     */
    static void enrichWorkerAlternatives(
        Stage workerStage,
        int partitionCount,
        int leftExpectedSenders,
        int rightExpectedSenders,
        String queryId,
        int leftProducerStageId,
        int rightProducerStageId
    ) {
        int workerStageId = workerStage.getStageId();
        // The fragment convertor strips OpenSearchShuffleExchange, so the worker fragment ends
        // up with two OpenSearchStageInputScan leaves the convertor rewrites to
        // "input-<producerStageId>" NamedScans. The handler must register its streaming table
        // under that exact name.
        String leftInputId = canonicalInputId(leftProducerStageId);
        String rightInputId = canonicalInputId(rightProducerStageId);
        List<StagePlan> enriched = new ArrayList<>(workerStage.getPlanAlternatives().size());
        for (StagePlan sp : workerStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(1 + existing.size() + 2 * partitionCount);
            // Placeholder setup with partition=-1 — the per-task filter in
            // WorkerFragmentStageExecutionFactory replaces this with a partition-specific copy
            // carrying both sides' expected sender counts. We don't know the partition at this
            // step (one alternative serves all partitions; per-task filtering picks the right
            // one).
            merged.add(new ShuffleWorkerSetupInstructionNode(queryId, workerStageId, -1, leftExpectedSenders, rightExpectedSenders));
            merged.addAll(existing);
            for (int p = 0; p < partitionCount; p++) {
                merged.add(new ShuffleScanInstructionNode(leftInputId, p, leftExpectedSenders, queryId, workerStageId, "left"));
                merged.add(new ShuffleScanInstructionNode(rightInputId, p, rightExpectedSenders, queryId, workerStageId, "right"));
            }
            enriched.add(sp.withInstructions(merged));
        }
        workerStage.setPlanAlternatives(enriched);
    }

    /** Canonical {@code "input-<producerStageId>"} name the fragment convertor emits when it
     *  rewrites the consumer fragment's {@link org.opensearch.analytics.planner.rel.OpenSearchStageInputScan}
     *  leaves. The handler must register streaming tables under this exact name so the worker
     *  plan's NamedScan binds correctly. */
    public static String canonicalInputId(int producerStageId) {
        return "input-" + producerStageId;
    }
}
