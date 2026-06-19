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
import org.opensearch.analytics.exec.QueryExecution;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Recursive sibling of {@link HashShuffleDispatch} for CASCADED hash-shuffle joins — multi-way joins
 * where every join level shuffles (see {@link CascadeShufflePlanRewriter} for how the cascade plan
 * is formed). Where {@code HashShuffleDispatch} lifts ONE join into ONE worker tier, this drives a
 * chain: {@link CascadeShuffleDAGRewriter} converts each join level into a worker, then this class
 * enriches every level's producers + worker with the shuffle instructions, bottom-up.
 *
 * <p>The structurally interesting case is an INTERMEDIATE worker: it is BOTH a shuffle consumer (of
 * the level below — gets {@code ShuffleWorkerSetup} + {@code ShuffleScan} instructions) AND a shuffle
 * producer (to the level above — gets a {@code ShuffleProducer} instruction). The two enrichments
 * compose in the right order ({@code [setup, scan…, producer]}) so the worker reads its children's
 * partitions, runs its join, and ships the result to its parent worker's partitions — no coordinator
 * gather between levels. Only the top worker gathers (SINGLETON) to the coordinator's reduce.
 *
 * @opensearch.internal
 */
public final class CascadeShuffleDispatch {

    private static final Logger LOGGER = LogManager.getLogger(CascadeShuffleDispatch.class);

    private final QueryScheduler scheduler;
    private final ClusterService clusterService;
    private final ShuffleBufferManager shuffleBufferManager;
    private final CapabilityRegistry capabilityRegistry;
    private final boolean preferMetadataDriver;

    public CascadeShuffleDispatch(
        QueryScheduler scheduler,
        ClusterService clusterService,
        ShuffleBufferManager shuffleBufferManager,
        CapabilityRegistry capabilityRegistry,
        boolean preferMetadataDriver
    ) {
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.shuffleBufferManager = shuffleBufferManager;
        this.capabilityRegistry = capabilityRegistry;
        this.preferMetadataDriver = preferMetadataDriver;
    }

    /**
     * Drives the cascade. Resolves a target node list per level, rewrites the DAG into worker tiers,
     * enriches each level's instructions, then hands the rewritten DAG to the scheduler for
     * concurrent dispatch (every worker blocks on its children's shuffle buffers).
     */
    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        try {
            CascadeShuffleDAGRewriter.Rewritten rewritten = CascadeShuffleDAGRewriter.rewrite(
                dag,
                capabilityRegistry,
                preferMetadataDriver,
                (levelIndex, partitionCount) -> resolveTargetWorkerNodeIds(partitionCount)
            );

            // Enrich each worker level bottom-up (shuffle producer/scan/worker instructions).
            enrichLevels(rewritten.levels(), ctx, clusterService, capabilityRegistry);

            QueryExecution exec = scheduler.execute(ctx.withDag(rewritten.dag()), terminal);
            if (queryExecutionSink != null) {
                queryExecutionSink.accept(exec);
            }
        } catch (Exception e) {
            terminal.onFailure(e);
        }
    }

    /**
     * Enriches each cascade worker level bottom-up with its shuffle producer / scan / worker
     * instructions. Shared by {@link CascadeShuffleDispatch} and the distributed-agg-over-cascade
     * dispatcher ({@code DistributedAggOverJoinDispatch}), which reuses the exact same per-level
     * shuffle wiring (it only adds a broadcast build/inject phase on top). A producer feeding an
     * intermediate worker may itself be a worker (its instructions already carry setup+scan); the
     * producer instruction is appended so the order stays {@code [setup, scan…, producer]}.
     */
    static void enrichLevels(
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels,
        QueryContext ctx,
        ClusterService clusterService,
        CapabilityRegistry capabilityRegistry
    ) {
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            Stage worker = level.worker();
            int workerStageId = worker.getStageId();
            List<String> targets = level.targetNodeIds();
            int partitionCount = level.partitionCount();

            // Fail fast if the resolved target list doesn't have exactly one node per partition
            // (empty cluster / undersized resolution). Without this, a worker tier could be built
            // with zero/too-few tasks or producers could ship to a short node list — silently
            // wrong results or an IndexOutOfBounds deep in dispatch. Mirrors the guard in
            // HashShuffleDispatch. (codex review R5 should-fix)
            if (targets.size() != partitionCount) {
                throw new IllegalStateException(
                    "CascadeShuffleDispatch: worker stage "
                        + workerStageId
                        + " resolved "
                        + targets.size()
                        + " target nodes but partitionCount="
                        + partitionCount
                );
            }

            int leftExpected = expectedSendersFor(level.leftProducer(), partitionCount, clusterService);
            int rightExpected = expectedSendersFor(level.rightProducer(), partitionCount, clusterService);

            // Producers ship to THIS worker's partitions (its node list), tagged with the side.
            // The hash keys are THIS join level's per-side keys — passed explicitly because an
            // intermediate-worker producer's own exchange info is SINGLETON (empty keys); it must
            // partition its join OUTPUT on the parent join's keys, not its (gathered) input's.
            HashShuffleDispatch.enrichProducerAlternatives(
                level.leftProducer(),
                level.leftKeys(),
                ctx.queryId(),
                workerStageId,
                partitionCount,
                targets,
                "left",
                capabilityRegistry
            );
            HashShuffleDispatch.enrichProducerAlternatives(
                level.rightProducer(),
                level.rightKeys(),
                ctx.queryId(),
                workerStageId,
                partitionCount,
                targets,
                "right",
                capabilityRegistry
            );
            // The worker consumes its two producers' partitions. enrichWorkerAlternatives prepends
            // a setup placeholder and appends per-(partition,side) scans; a producer instruction
            // (added above when this worker also feeds a higher level) stays AFTER the scans
            // because enrichProducerAlternatives appended it to the worker's own alternatives.
            HashShuffleDispatch.enrichWorkerAlternatives(
                worker,
                partitionCount,
                leftExpected,
                rightExpected,
                ctx.queryId(),
                level.leftProducer().getStageId(),
                level.rightProducer().getStageId()
            );

            LOGGER.debug(
                "[CascadeShuffleDispatch] level worker={} left={} right={} partitions={} leftSenders={} rightSenders={} targets={}",
                workerStageId,
                level.leftProducer().getStageId(),
                level.rightProducer().getStageId(),
                partitionCount,
                leftExpected,
                rightExpected,
                targets
            );
        }
    }

    /** Round-robins one target worker node per partition across the cluster's data nodes — same
     *  policy as {@link HashShuffleDispatch}. Distinct levels may reuse the same nodes; shuffle
     *  buffers are keyed by {@code (queryId, stageId, partition)} so there is no collision. */
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
     * Per-(partition, side) sender count the consumer worker expects. A producer task — whether a
     * leaf shard scan ({@code ShardTargetResolver}, one task per shard) or an intermediate worker
     * ({@code WorkerTargetResolver}, one task per partition) — ships to ALL partitions and marks
     * isLast once per partition, so the count is the producer's task count. Both resolvers return
     * one {@code ExecutionTarget} per task, so {@code resolve(...).size()} is uniform. Falls back to
     * the worker's own {@code partitionCount} when the producer has no resolver.
     */
    private static int expectedSendersFor(Stage producer, int fallbackPartitionCount, ClusterService clusterService) {
        if (producer.getTargetResolver() == null) {
            return Math.max(fallbackPartitionCount, 1);
        }
        int n = producer.getTargetResolver().resolve(clusterService.state(), null).size();
        return Math.max(n, 1);
    }
}
