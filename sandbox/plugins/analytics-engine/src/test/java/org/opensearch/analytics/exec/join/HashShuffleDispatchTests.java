/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HashShuffleDispatch}'s plan-mutation steps. The end-to-end dispatch
 * (cluster-wide producer/consumer concurrency, transport, native handlers) is verified in
 * HashShuffleJoinIT once the runtime side is wired.
 */
public class HashShuffleDispatchTests extends OpenSearchTestCase {

    public void testCanonicalInputIdMatchesConvertorRewrite() {
        // The fragment convertor strips OpenSearchShuffleExchange and rewrites the StageInputScan
        // leaf to a NamedScan whose name follows the same "input-<producerStageId>" convention as
        // reduce-stage child inputs; the consumer's handler must register its streaming table
        // under exactly this name or the join fails to bind its inputs.
        assertEquals("input-7", HashShuffleDispatch.canonicalInputId(7));
        assertNotEquals(
            "different producer stages must yield different names",
            HashShuffleDispatch.canonicalInputId(1),
            HashShuffleDispatch.canonicalInputId(2)
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

    public void testEnrichConsumerAlternativesAppendsTwoScansPerPartition() {
        // 3 partitions × 2 sides = 6 ShuffleScanInstructionNodes appended per plan alternative.
        Stage consumer = new Stage(/* stageId */ 9, null, List.of(), null, null, null);
        StagePlan alt = new StagePlan(null, "df");
        consumer.setPlanAlternatives(List.of(alt));

        HashShuffleDispatch.enrichWorkerAlternatives(
            consumer,
            /* partitionCount */ 3,
            /* leftExpectedSenders */ 5,
            /* rightExpectedSenders */ 4,
            /* queryId */ "qid-test",
            /* leftProducerStageId */ 11,
            /* rightProducerStageId */ 12
        );

        List<InstructionNode> instr = consumer.getPlanAlternatives().get(0).instructions();
        assertEquals("1 worker setup + 3 partitions × 2 sides = 7 instructions", 7, instr.size());
        // First is the worker setup; remaining 6 are shuffle scans (partitionCount × 2 sides).
        assertTrue(
            "first appended instruction is the worker setup",
            instr.get(0) instanceof org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode
        );
        for (int i = 1; i < instr.size(); i++) {
            assertTrue("appended instruction is a shuffle-scan", instr.get(i) instanceof ShuffleScanInstructionNode);
        }
        // Spot-check naming and sender counts. The interleaving must alternate left/right per
        // partition so the join's two named inputs resolve in lockstep. Names match the
        // convertor's "input-<producerStageId>" convention; the worker stage execution filters
        // these down to the two instructions matching each task's partition before sending.
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

    private static Stage newProducerStage(int stageId, int partitionCount, List<Integer> hashKeys) {
        return new Stage(stageId, null, List.of(), ExchangeInfo.hashDistributed(hashKeys, partitionCount), null, null);
    }

    /** A registry whose named backend declares DataTransferCapability(PRODUCER), so its plan
     *  alternatives survive enrichProducerAlternatives' shuffle-capability filter. */
    private static CapabilityRegistry shuffleCapableRegistry(String backendName) {
        BackendCapabilityProvider caps = mock(BackendCapabilityProvider.class);
        when(caps.dataTransferCapabilities()).thenReturn(
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.PRODUCER, "arrow-ipc-partitioned"))
        );
        AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
        when(backend.getCapabilityProvider()).thenReturn(caps);
        CapabilityRegistry registry = mock(CapabilityRegistry.class);
        when(registry.getBackend(backendName)).thenReturn(backend);
        return registry;
    }
}
