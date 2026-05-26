/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link HashShuffleDispatch}'s plan-mutation steps. The end-to-end dispatch
 * (cluster-wide producer/consumer concurrency, transport, native handlers) is verified in
 * HashShuffleJoinIT once the runtime side is wired.
 */
public class HashShuffleDispatchTests extends OpenSearchTestCase {

    public void testShuffleNamedInputIdEncodesAllThreeAxes() {
        // The producer-side handler computes this name when it ships, the consumer-side handler
        // computes it when it registers a NamedScan; both sides must compute it from the same
        // (consumerStageId, side, partition) triple or the join fails to bind its inputs.
        assertEquals("shuffle-7-left-0", HashShuffleDispatch.shuffleNamedInputId(7, "left", 0));
        assertEquals("shuffle-7-right-3", HashShuffleDispatch.shuffleNamedInputId(7, "right", 3));
        assertNotEquals(
            "different stages must yield different names",
            HashShuffleDispatch.shuffleNamedInputId(1, "left", 0),
            HashShuffleDispatch.shuffleNamedInputId(2, "left", 0)
        );
    }

    public void testEnrichProducerAlternativesAppendsOneInstructionPerAlt() {
        Stage producer = newProducerStage(/* stageId */ 1, /* partitionCount */ 3, /* hashKeys */ List.of(0));
        StagePlan altA = new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()));
        StagePlan altB = new StagePlan(null, "df").withInstructions(List.of(new ShardScanInstructionNode()));
        producer.setPlanAlternatives(List.of(altA, altB));

        HashShuffleDispatch.enrichProducerAlternatives(
            producer,
            "qid",
            /* consumerStageId */ 9,
            /* partitionCount */ 3,
            List.of("node-0", "node-1", "node-2"),
            "left"
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

    public void testEnrichConsumerAlternativesAppendsTwoScansPerPartition() {
        // 3 partitions × 2 sides = 6 ShuffleScanInstructionNodes appended per plan alternative.
        Stage consumer = new Stage(/* stageId */ 9, null, List.of(), null, null, null);
        StagePlan alt = new StagePlan(null, "df");
        consumer.setPlanAlternatives(List.of(alt));

        HashShuffleDispatch.enrichConsumerAlternatives(
            consumer,
            /* partitionCount */ 3,
            /* leftExpectedSenders */ 5,
            /* rightExpectedSenders */ 4,
            /* queryId */ "qid-test"
        );

        List<InstructionNode> instr = consumer.getPlanAlternatives().get(0).instructions();
        assertEquals("3 partitions × 2 sides = 6 scan instructions", 6, instr.size());
        for (InstructionNode node : instr) {
            assertTrue("every appended instruction is a shuffle-scan", node instanceof ShuffleScanInstructionNode);
        }
        // Spot-check naming and sender counts. The interleaving must alternate left/right per
        // partition so the join's two named inputs resolve in lockstep.
        ShuffleScanInstructionNode left0 = (ShuffleScanInstructionNode) instr.get(0);
        ShuffleScanInstructionNode right0 = (ShuffleScanInstructionNode) instr.get(1);
        assertEquals("shuffle-9-left-0", left0.getNamedInputId());
        assertEquals("shuffle-9-right-0", right0.getNamedInputId());
        assertEquals(0, left0.getShufflePartitionIndex());
        assertEquals(0, right0.getShufflePartitionIndex());
        assertEquals(5, left0.getExpectedSenders());
        assertEquals(4, right0.getExpectedSenders());
        assertEquals("qid-test", left0.getQueryId());
        assertEquals(9, left0.getTargetStageId());
        assertEquals("qid-test", right0.getQueryId());
        assertEquals(9, right0.getTargetStageId());
        // Last partition.
        ShuffleScanInstructionNode left2 = (ShuffleScanInstructionNode) instr.get(4);
        ShuffleScanInstructionNode right2 = (ShuffleScanInstructionNode) instr.get(5);
        assertEquals("shuffle-9-left-2", left2.getNamedInputId());
        assertEquals("shuffle-9-right-2", right2.getNamedInputId());
    }

    private static Stage newProducerStage(int stageId, int partitionCount, List<Integer> hashKeys) {
        return new Stage(stageId, null, List.of(), ExchangeInfo.hashDistributed(hashKeys, partitionCount), null, null);
    }
}
