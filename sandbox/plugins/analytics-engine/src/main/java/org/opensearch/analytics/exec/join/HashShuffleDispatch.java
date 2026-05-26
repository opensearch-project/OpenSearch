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
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
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

    public HashShuffleDispatch(QueryScheduler scheduler, ClusterService clusterService, ShuffleBufferManager shuffleBufferManager) {
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.shuffleBufferManager = shuffleBufferManager;
    }

    /**
     * Drives the hash-shuffle DAG end-to-end. The DAG has already been cut by {@code DAGBuilder}
     * with {@link Stage.StageRole#SHUFFLE_SCAN_LEFT} / {@code SHUFFLE_SCAN_RIGHT} on the producer
     * children of a consumer stage. Caller is responsible for selecting the right strategy
     * (this class is invoked only when {@link JoinStrategyAdvisor#observe} returns
     * {@link JoinStrategy#HASH_SHUFFLE}).
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

        // Pre-allocate buffers and tell each its expected sender count. Each partition's buffer
        // expects exactly one sender per side per producer-stage's worker — and each producer
        // stage runs as one task per shard. The consumer pulls per-partition tasks and each
        // task drains exactly one buffer entry.
        //
        // expectedSenders here is the count of senders THIS partition expects on EACH side.
        // Each producer dispatches the partition's bucket from every shard task that holds rows
        // for that partition, so the count is partitionCount-independent — it's the number of
        // producer-side shard tasks that will report isLast for the side. With a partitioned
        // sink the producer marks isLast once per (partition, side) pair, so the count is the
        // number of producer shard tasks (= partitionCount of the producer's input scan).
        //
        // The exact count is wired in by the producer handler when it issues isLast markers; we
        // pre-set it here from the producer stages' shard counts so the buffer's awaitReady
        // succeeds. Today producer scan stages run one task per shard (ShardFragmentStageExecution).
        int leftExpectedSenders = expectedSendersFor(leftProducer);
        int rightExpectedSenders = expectedSendersFor(rightProducer);
        for (int p = 0; p < partitionCount; p++) {
            ShuffleBufferManager.ShuffleBuffer buf = shuffleBufferManager.getOrCreateBuffer(ctx.queryId(), consumer.getStageId(), p);
            buf.setExpectedSenders(leftExpectedSenders, rightExpectedSenders);
        }

        // Append shuffle instructions to plan alternatives. Producers learn the partition
        // targets and side label; consumer learns the per-side namedInputIds and per-partition
        // sender counts.
        enrichProducerAlternatives(leftProducer, ctx.queryId(), consumer.getStageId(), partitionCount, targetWorkerNodeIds, "left");
        enrichProducerAlternatives(rightProducer, ctx.queryId(), consumer.getStageId(), partitionCount, targetWorkerNodeIds, "right");
        enrichConsumerAlternatives(consumer, partitionCount, leftExpectedSenders, rightExpectedSenders, ctx.queryId());

        LOGGER.debug(
            "[HashShuffleDispatch] dispatching: query={}, consumerStage={}, partitions={}, leftSenders={}, rightSenders={}",
            ctx.queryId(),
            consumer.getStageId(),
            partitionCount,
            leftExpectedSenders,
            rightExpectedSenders
        );

        // The standard scheduler walks the DAG: producers and consumer dispatch concurrently;
        // the consumer's per-partition tasks block on ShuffleBuffer.awaitReady until both
        // producer sides mark isLast. Cancellation cascades through the walker's existing
        // task-tracking, so no separate onCancel installation is needed.
        org.opensearch.analytics.exec.QueryExecution exec = scheduler.execute(ctx, terminal);
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
    private static int expectedSendersFor(Stage producer) {
        // For now: every producer task hashes to all N partitions and reports isLast on the
        // partition path it sent rows for. With one producer task per shard, partition p's
        // buffer expects one isLast from each shard task. The shard count for the producer
        // stage is encoded in its TargetResolver (ShardTargetResolver) and not directly
        // accessible here; until the handler-side wiring is done, fall back to the partition
        // count as a conservative proxy. BufferAwaitReady refuses to count down past
        // expectedSenders, so over-estimating risks consumer hang on partitions a producer
        // didn't reach. Under-estimating risks early consumer wake-up. The handler must update
        // this number once the actual shard task count is known.
        return producer.getExchangeInfo().partitionCount();
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
        String side
    ) {
        List<Integer> hashKeys = producerStage.getExchangeInfo().partitionKeyIndices();
        List<StagePlan> enriched = new ArrayList<>(producerStage.getPlanAlternatives().size());
        for (StagePlan sp : producerStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(existing.size() + 1);
            merged.addAll(existing);
            merged.add(new ShuffleProducerInstructionNode(hashKeys, partitionCount, targetWorkerNodeIds, queryId, consumerStageId, side));
            enriched.add(sp.withInstructions(merged));
        }
        producerStage.setPlanAlternatives(enriched);
    }

    /**
     * Appends one {@link ShuffleScanInstructionNode} per side per partition to every plan
     * alternative on the consumer stage. The consumer's worker plan references each side as a
     * separate {@code NamedScan}, and the handler resolves each name to a {@code StreamingTable}
     * backed by the corresponding partition's buffer slice. Package-private for unit testing.
     */
    static void enrichConsumerAlternatives(
        Stage consumerStage,
        int partitionCount,
        int leftExpectedSenders,
        int rightExpectedSenders,
        String queryId
    ) {
        int consumerStageId = consumerStage.getStageId();
        List<StagePlan> enriched = new ArrayList<>(consumerStage.getPlanAlternatives().size());
        for (StagePlan sp : consumerStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(existing.size() + 2 * partitionCount);
            merged.addAll(existing);
            for (int p = 0; p < partitionCount; p++) {
                merged.add(
                    new ShuffleScanInstructionNode(
                        shuffleNamedInputId(consumerStageId, "left", p),
                        p,
                        leftExpectedSenders,
                        queryId,
                        consumerStageId
                    )
                );
                merged.add(
                    new ShuffleScanInstructionNode(
                        shuffleNamedInputId(consumerStageId, "right", p),
                        p,
                        rightExpectedSenders,
                        queryId,
                        consumerStageId
                    )
                );
            }
            enriched.add(sp.withInstructions(merged));
        }
        consumerStage.setPlanAlternatives(enriched);
    }

    /**
     * Canonical name for a shuffle-shipped partition stream. Both producer (when it ships) and
     * consumer (when it registers a {@code NamedScan}) compute the name from the same
     * {@code (consumerStageId, side, partition)} triple so they meet at the same buffer slice.
     */
    public static String shuffleNamedInputId(int consumerStageId, String side, int partition) {
        return "shuffle-" + consumerStageId + "-" + side + "-" + partition;
    }
}
