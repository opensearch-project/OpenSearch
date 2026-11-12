/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.worker;

import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.join.ShuffleEnrichment;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The placeholder → per-task rebuild of {@link ShuffleWorkerSetupInstructionNode}.
 *
 * <p>The coordinator makes two per-worker-stage decisions (join algorithm and shuffle shape) and puts
 * them on a partition-agnostic placeholder; dispatch replaces that placeholder with a partition-specific
 * copy. Anything not copied across is silently reverted to its default for EVERY task, with no error and
 * no log line — which is why each decision gets an explicit assertion here.
 */
public class WorkerFragmentPerTaskSetupTests extends OpenSearchTestCase {

    private static Stage workerStageWith(boolean preferHashJoin, boolean pipelined) {
        Stage consumer = new Stage(/* stageId */ 7, null, List.of(), null, null, null);
        consumer.setPlanAlternatives(List.of(new StagePlan(null, "df")));
        ShuffleEnrichment.enrichWorkerAlternatives(
            consumer,
            /* partitionCount */ 3,
            /* leftExpectedSenders */ 5,
            /* rightExpectedSenders */ 4,
            /* queryId */ "q-per-task",
            /* leftProducerStageId */ 11,
            /* rightProducerStageId */ 12,
            preferHashJoin,
            pipelined
        );
        return consumer;
    }

    private static ShuffleWorkerSetupInstructionNode setupOf(List<FragmentExecutionRequest.PlanAlternative> alts) {
        for (InstructionNode node : alts.get(0).getInstructions()) {
            if (node instanceof ShuffleWorkerSetupInstructionNode setup) {
                return setup;
            }
        }
        throw new AssertionError("no setup instruction survived the per-partition filter");
    }

    /** Materialized: the shape must reach the task, else the data node would run the whole shuffle in the
     *  node default and the residency bound the shape exists for would never apply. */
    public void testMaterializedShapeSurvivesThePerTaskRebuild() {
        Stage stage = workerStageWith(/* preferHashJoin */ false, /* pipelined */ false);

        for (int partition = 0; partition < 3; partition++) {
            ShuffleWorkerSetupInstructionNode setup = setupOf(
                WorkerFragmentStageExecutionFactory.filterPlanAlternativesForPartition(stage, partition)
            );
            assertEquals("the rebuild must bind this task's partition", partition, setup.getPartitionIndex());
            assertFalse("shuffle shape must survive the rebuild for partition " + partition, setup.isPipelined());
            assertFalse("join algorithm must survive it too", setup.getPreferHashJoin());
        }
    }

    /** ...and the pipelined shape survives just as well — the flag is copied, not defaulted. */
    public void testPipelinedShapeSurvivesThePerTaskRebuild() {
        Stage stage = workerStageWith(/* preferHashJoin */ true, /* pipelined */ true);
        ShuffleWorkerSetupInstructionNode setup = setupOf(
            WorkerFragmentStageExecutionFactory.filterPlanAlternativesForPartition(stage, 1)
        );
        assertTrue(setup.isPipelined());
        assertTrue(setup.getPreferHashJoin());
    }

    /** The filter keeps only this partition's scans, and the setup still declares every slot — the two
     *  properties the rebuild exists for, asserted alongside the flags so a change to one is visible. */
    public void testFilterKeepsOnlyThisPartitionsScans() {
        Stage stage = workerStageWith(true, true);
        List<FragmentExecutionRequest.PlanAlternative> alts = WorkerFragmentStageExecutionFactory
            .filterPlanAlternativesForPartition(stage, 2);

        int scans = 0;
        for (InstructionNode node : alts.get(0).getInstructions()) {
            if (node instanceof ShuffleScanInstructionNode scan) {
                assertEquals("only partition 2's scans may survive", 2, scan.getShufflePartitionIndex());
                scans++;
            }
        }
        assertEquals("one scan per slot", 2, scans);
        ShuffleWorkerSetupInstructionNode setup = setupOf(alts);
        assertEquals(5, setup.getLeftExpectedSenders());
        assertEquals(4, setup.getRightExpectedSenders());
    }
}
