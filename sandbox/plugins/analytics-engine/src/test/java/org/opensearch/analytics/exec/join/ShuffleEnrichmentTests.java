/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DataTransferCapability;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ShuffleEnrichment}'s plan-mutation primitives — the per-stage instruction
 * attachment the general MPP scheduler relies on. These were ported from the deleted
 * {@code HashShuffleDispatchTests} when the enumerated dispatchers were folded into the single general
 * path: the methods moved verbatim into {@link ShuffleEnrichment}, so the regression coverage moves with
 * them. The end-to-end dispatch (cluster-wide producer/consumer concurrency, transport, native handlers)
 * is verified in {@code HashShuffleJoinIT} / {@code GeneralSchedulerJoinIT}.
 */
public class ShuffleEnrichmentTests extends OpenSearchTestCase {

    public void testCanonicalInputIdMatchesConvertorRewrite() {
        // The fragment convertor strips OpenSearchShuffleExchange and rewrites the StageInputScan leaf to a
        // NamedScan whose name follows the "input-<producerStageId>" convention; the consumer's handler must
        // register its streaming table under exactly this name or the join fails to bind its inputs.
        assertEquals("input-7", ShuffleEnrichment.canonicalInputId(7));
        assertNotEquals(
            "different producer stages must yield different names",
            ShuffleEnrichment.canonicalInputId(1),
            ShuffleEnrichment.canonicalInputId(2)
        );
    }

    public void testEnrichProducerAlternativesAppendsOneInstructionPerAlt() {
        Stage producer = newProducerStage(/* stageId */ 1, /* partitionCount */ 3, /* hashKeys */ List.of(0));
        StagePlan altA = new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()));
        StagePlan altB = new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()));
        producer.setPlanAlternatives(List.of(altA, altB));

        ShuffleEnrichment.enrichProducerAlternatives(
            producer,
            /* hashKeys */ List.of(0),
            "qid",
            /* consumerStageId */ 9,
            /* partitionCount */ 3,
            List.of("node-0", "node-1", "node-2"),
            "left",
            shuffleCapableRegistry("df")
        );

        List<StagePlan> enriched = producer.getPlanAlternatives();
        assertEquals(2, enriched.size());
        for (StagePlan plan : enriched) {
            List<InstructionNode> instr = plan.instructions();
            assertEquals("scan + producer = 2 instructions", 2, instr.size());
            assertTrue("scan stays first", instr.get(0) instanceof ShardScanInstructionNode);
            assertTrue("producer instruction appended last", instr.get(1) instanceof ShuffleProducerInstructionNode);
            ShuffleProducerInstructionNode pn = (ShuffleProducerInstructionNode) instr.get(1);
            assertEquals(List.of(0), pn.getHashKeyChannels());
            assertEquals(3, pn.getPartitionCount());
            assertEquals(List.of("node-0", "node-1", "node-2"), pn.getTargetWorkerNodeIds());
            assertEquals("qid", pn.getQueryId());
            assertEquals(9, pn.getTargetStageId());
            assertEquals("left", pn.getSide());
        }
    }

    public void testEnrichProducerAlternativesDropsNonShuffleCapableBackend() {
        // A producer alternative whose backend cannot drive a shuffle (no DataTransferCapability(PRODUCER))
        // must be dropped so PlanAlternativeSelector can't pick a driver that throws at execution. With NO
        // capable alternative left, enrich must fail fast rather than emit an un-runnable producer.
        Stage producer = newProducerStage(/* stageId */ 1, /* partitionCount */ 2, List.of(0));
        producer.setPlanAlternatives(List.of(new StagePlan(null, "lucene").withInstructions(List.of(new ShardScanInstructionNode()))));

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ShuffleEnrichment.enrichProducerAlternatives(
                producer,
                List.of(0),
                "qid",
                /* consumerStageId */ 9,
                /* partitionCount */ 2,
                List.of("node-0", "node-1"),
                "left",
                shuffleCapableRegistry("df") // only "df" is producer-capable; the "lucene" alt is dropped
            )
        );
        assertTrue(
            "must name the producer stage when no shuffle-capable alternative survives",
            ex.getMessage().contains("producer stage 1")
        );
    }

    public void testEnrichWorkerAlternativesAppendsTwoScansPerPartition() {
        // 3 partitions × 2 sides = 6 ShuffleScanInstructionNodes appended per plan alternative, prefixed by
        // one worker setup = 7 total.
        Stage consumer = new Stage(/* stageId */ 9, null, List.of(), null, null, null);
        StagePlan alt = new StagePlan(null, "df");
        consumer.setPlanAlternatives(List.of(alt));

        ShuffleEnrichment.enrichWorkerAlternatives(
            consumer,
            /* partitionCount */ 3,
            /* leftExpectedSenders */ 5,
            /* rightExpectedSenders */ 4,
            /* queryId */ "qid-test",
            /* leftProducerStageId */ 11,
            /* rightProducerStageId */ 12,
            /* preferHashJoin */ true,
            /* pipelined */ true
        );

        List<InstructionNode> instr = consumer.getPlanAlternatives().get(0).instructions();
        assertEquals("1 worker setup + 3 partitions × 2 sides = 7 instructions", 7, instr.size());
        assertTrue("first appended instruction is the worker setup", instr.get(0) instanceof ShuffleWorkerSetupInstructionNode);
        for (int i = 1; i < instr.size(); i++) {
            assertTrue("appended instruction is a shuffle-scan", instr.get(i) instanceof ShuffleScanInstructionNode);
        }
        // Spot-check naming and sender counts. The interleaving alternates left/right per partition so the
        // join's two named inputs resolve in lockstep; names match the convertor's "input-<producerStageId>".
        ShuffleScanInstructionNode left0 = (ShuffleScanInstructionNode) instr.get(1);
        ShuffleScanInstructionNode right0 = (ShuffleScanInstructionNode) instr.get(2);
        assertEquals("input-11", left0.getNamedInputId());
        assertEquals("input-12", right0.getNamedInputId());
        assertEquals("left", left0.getSide());
        assertEquals("right", right0.getSide());
        assertEquals(0, left0.getShufflePartitionIndex());
        assertEquals(0, right0.getShufflePartitionIndex());
        assertEquals(5, left0.getExpectedSenders());
        assertEquals(4, right0.getExpectedSenders());
        assertEquals("qid-test", left0.getQueryId());
        assertEquals(9, left0.getTargetStageId());
        assertEquals("qid-test", right0.getQueryId());
        assertEquals(9, right0.getTargetStageId());
        // Last partition (offsets shifted by 1 because of the prepended worker setup).
        ShuffleScanInstructionNode left2 = (ShuffleScanInstructionNode) instr.get(5);
        ShuffleScanInstructionNode right2 = (ShuffleScanInstructionNode) instr.get(6);
        assertEquals("input-11", left2.getNamedInputId());
        assertEquals("input-12", right2.getNamedInputId());
        assertEquals(2, left2.getShufflePartitionIndex());
        assertEquals(2, right2.getShufflePartitionIndex());
    }

    public void testWorkerSetupCarriesBothSidesExpectedSenders() {
        // The setup placeholder carries partition=-1 (filled per-task downstream) plus BOTH sides' expected
        // sender counts so each worker task knows how many producers to await before draining.
        Stage consumer = new Stage(/* stageId */ 4, null, List.of(), null, null, null);
        consumer.setPlanAlternatives(List.of(new StagePlan(null, "df")));

        ShuffleEnrichment.enrichWorkerAlternatives(
            consumer,
            /* partitionCount */ 2,
            7,
            3,
            "qid",
            1,
            2,
            /* preferHashJoin */ false,
            /* pipelined */ false
        );

        ShuffleWorkerSetupInstructionNode setup = (ShuffleWorkerSetupInstructionNode) consumer.getPlanAlternatives()
            .get(0)
            .instructions()
            .get(0);
        assertEquals("placeholder partition is -1 until the per-task filter sets it", -1, setup.getPartitionIndex());
        assertEquals(7, setup.getLeftExpectedSenders());
        assertEquals(3, setup.getRightExpectedSenders());
        assertEquals("qid", setup.getQueryId());
        assertEquals(4, setup.getTargetStageId());
        assertFalse("preferHashJoin flows through to the setup placeholder", setup.getPreferHashJoin());
        assertFalse("the shuffle shape flows through the same placeholder", setup.isPipelined());
    }

    public void testEnrichWorkerAlternativesForThreeSlots() {
        // N-ary transport: a 3-input worker gets 2 partitions × 3 slots = 6 scans + 1 setup. This is the
        // shape a collapsed N-way join tier needs; the binary case is asserted above.
        Stage consumer = new Stage(/* stageId */ 20, null, List.of(), null, null, null);
        consumer.setPlanAlternatives(List.of(new StagePlan(null, "df")));

        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("in0", 5);
        expected.put("in1", 4);
        expected.put("in2", 3);
        Map<String, Integer> producers = new LinkedHashMap<>();
        producers.put("in0", 31);
        producers.put("in1", 32);
        producers.put("in2", 33);

        ShuffleEnrichment.enrichWorkerAlternatives(consumer, /* partitionCount */ 2, expected, "qid-n", producers, true, /* pipelined */ true);

        List<InstructionNode> instr = consumer.getPlanAlternatives().get(0).instructions();
        assertEquals("1 setup + 2 partitions × 3 slots = 7 instructions", 7, instr.size());
        ShuffleWorkerSetupInstructionNode setup = (ShuffleWorkerSetupInstructionNode) instr.get(0);
        assertEquals("setup declares every slot in one call", expected, setup.getExpectedSendersBySlot());

        // Each (partition, slot) pair appears exactly once, bound to its own producer's named input.
        Set<String> seen = new java.util.HashSet<>();
        for (int i = 1; i < instr.size(); i++) {
            ShuffleScanInstructionNode scan = (ShuffleScanInstructionNode) instr.get(i);
            assertTrue("duplicate (partition,slot) scan", seen.add(scan.getShufflePartitionIndex() + ":" + scan.getSide()));
            assertEquals("input-" + producers.get(scan.getSide()), scan.getNamedInputId());
            assertEquals(expected.get(scan.getSide()).intValue(), scan.getExpectedSenders());
            assertEquals(20, scan.getTargetStageId());
            assertEquals("qid-n", scan.getQueryId());
        }
        assertEquals(6, seen.size());
    }

    public void testEnrichWorkerAlternativesRejectsSlotSetMismatch() {
        // expectedSenders and producer-stage maps must describe the SAME slots — a mismatch would emit a
        // scan with a null expected count (or omit a slot the buffer awaits) and hang the worker.
        Stage consumer = new Stage(/* stageId */ 21, null, List.of(), null, null, null);
        consumer.setPlanAlternatives(List.of(new StagePlan(null, "df")));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ShuffleEnrichment.enrichWorkerAlternatives(
                consumer,
                2,
                new LinkedHashMap<>(Map.of("in0", 1, "in1", 1)),
                "qid",
                new LinkedHashMap<>(Map.of("in0", 5)),
                true,
                /* pipelined */ true
            )
        );
        assertTrue("must name the disagreeing slot sets", ex.getMessage().contains("disagree"));
    }

    /**
     * The shuffle SHAPE decision. A worker join whose build is estimated not to fit in memory takes the
     * materialized shape (producers finish, then the consumer drains its spilling buffer), because there a
     * residency bound that cannot fail a query is worth more than overlapping the drain with its
     * producers. A build that does fit keeps the pipelined shape, which is the class whose latency
     * regressed when the in-flight window was squeezed.
     *
     * <p>Driven through {@code sortMergeJoinMinRows}: with null fragments every build estimates 0 rows, so
     * a floor of 0 makes the estimate fail it ({@code 0 < 0} is false) and a floor of 1 makes it pass.
     */
    public void testShuffleShapeFollowsTheBuildFitsInMemoryEstimate() {
        assertFalse("a build that does not fit takes the materialized shape", shapeFor(/* smjMinRows */ 0, /* inputs */ 2));
        assertTrue("a build that fits keeps the pipelined shape", shapeFor(/* smjMinRows */ 1, /* inputs */ 2));
    }

    /**
     * ...but only for a JOIN worker. A single-input worker — the FINAL half of an aggregate shuffle —
     * consumes its one stream continuously, so its drain never stalls and materializing would cost latency
     * for nothing. Its estimate is also the wrong quantity: it measures the scan under a PARTIAL aggregate,
     * not the bytes that aggregate ships.
     */
    public void testSingleInputWorkerStaysPipelinedEvenWhenTheEstimateFails() {
        assertTrue("an aggregate-shuffle worker must not be materialized by a join heuristic", shapeFor(0, /* inputs */ 1));
    }

    /** Runs {@code enrichLevels} for one worker level with {@code inputCount} producers and returns the
     *  shuffle shape it stamped on the setup placeholder. */
    private static boolean shapeFor(long sortMergeJoinMinRows, int inputCount) {
        Stage worker = new Stage(/* stageId */ 30, null, List.of(), null, null, null);
        worker.setPlanAlternatives(List.of(new StagePlan(null, "df")));
        List<ShuffleEnrichment.WorkerInput> inputs = new java.util.ArrayList<>();
        for (int i = 0; i < inputCount; i++) {
            Stage producer = newProducerStage(/* stageId */ 40 + i, /* partitionCount */ 1, List.of(0));
            producer.setPlanAlternatives(List.of(new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()))));
            inputs.add(new ShuffleEnrichment.WorkerInput(producer, List.of(0), inputCount == 1 ? "left" : (i == 0 ? "left" : "right")));
        }
        ShuffleEnrichment.WorkerLevel level = new ShuffleEnrichment.WorkerLevel(worker, inputs, 1, List.of("node-0"));

        QueryContext ctx = mock(QueryContext.class);
        when(ctx.queryId()).thenReturn("qid-shape");
        ShuffleEnrichment.enrichLevels(
            List.of(level),
            ctx,
            mock(ClusterService.class),
            shuffleCapableRegistry("df"),
            sortMergeJoinMinRows
        );

        for (InstructionNode node : worker.getPlanAlternatives().get(0).instructions()) {
            if (node instanceof ShuffleWorkerSetupInstructionNode setup) {
                return setup.isPipelined();
            }
        }
        throw new AssertionError("enrichLevels emitted no worker setup instruction");
    }

    private static Stage newProducerStage(int stageId, int partitionCount, List<Integer> hashKeys) {
        return new Stage(stageId, null, List.of(), ExchangeInfo.hashDistributed(hashKeys, partitionCount), null, null);
    }

    /** A registry whose named backend declares DataTransferCapability(PRODUCER), so its plan alternatives
     *  survive enrichProducerAlternatives' shuffle-capability filter. Any other backend id resolves to a
     *  mock with no PRODUCER capability (dropped). */
    private static CapabilityRegistry shuffleCapableRegistry(String producerBackendName) {
        BackendCapabilityProvider producerCaps = mock(BackendCapabilityProvider.class);
        when(producerCaps.dataTransferCapabilities()).thenReturn(
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.PRODUCER, "arrow-ipc-partitioned"))
        );
        AnalyticsSearchBackendPlugin producerBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(producerBackend.getCapabilityProvider()).thenReturn(producerCaps);

        BackendCapabilityProvider noTransferCaps = mock(BackendCapabilityProvider.class);
        when(noTransferCaps.dataTransferCapabilities()).thenReturn(Set.of());
        AnalyticsSearchBackendPlugin noTransferBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(noTransferBackend.getCapabilityProvider()).thenReturn(noTransferCaps);

        CapabilityRegistry registry = mock(CapabilityRegistry.class);
        when(registry.getBackend(org.mockito.ArgumentMatchers.anyString())).thenReturn(noTransferBackend);
        when(registry.getBackend(producerBackendName)).thenReturn(producerBackend);
        return registry;
    }
}
