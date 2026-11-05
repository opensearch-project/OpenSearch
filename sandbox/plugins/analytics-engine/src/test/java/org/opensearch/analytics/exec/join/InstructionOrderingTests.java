/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link InstructionOrdering} — the invariant that an aggregate-preparing instruction runs
 * after every table-registering one, because preparation builds a physical plan from the fragment bytes and
 * so needs the tables the fragment names to exist.
 */
public class InstructionOrderingTests extends OpenSearchTestCase {

    private static InstructionNode broadcast() {
        return new BroadcastInjectionInstructionNode("broadcast-0", 0, new byte[] { 1 });
    }

    public void testPreparationMovesBehindALaterRegistration() {
        List<InstructionNode> ordered = InstructionOrdering.aggregatePreparationLast(
            List.of(new ShardScanInstructionNode(), new PartialAggregateInstructionNode(), broadcast())
        );

        assertEquals(3, ordered.size());
        assertTrue(ordered.get(0) instanceof ShardScanInstructionNode);
        assertTrue(ordered.get(1) instanceof BroadcastInjectionInstructionNode);
        assertTrue(ordered.get(2) instanceof PartialAggregateInstructionNode);
    }

    /** FINAL preparation plans the fragment the same way PARTIAL does, so it moves too. */
    public void testFinalPreparationAlsoMoves() {
        List<InstructionNode> ordered = InstructionOrdering.aggregatePreparationLast(
            List.of(new FinalAggregateInstructionNode(), broadcast())
        );

        assertTrue(ordered.get(0) instanceof BroadcastInjectionInstructionNode);
        assertTrue(ordered.get(1) instanceof FinalAggregateInstructionNode);
    }

    /**
     * An already-correct chain must come back untouched — callers apply this unconditionally, and a
     * reorder that shuffled well-formed chains would move a producer instruction off the end, where the
     * shuffle transport requires it.
     */
    public void testAlreadyOrderedChainIsReturnedUnchanged() {
        List<InstructionNode> input = List.of(new ShardScanInstructionNode(), broadcast(), new PartialAggregateInstructionNode());

        assertSame(input, InstructionOrdering.aggregatePreparationLast(input));
    }

    public void testChainWithoutPreparationIsReturnedUnchanged() {
        List<InstructionNode> input = List.of(
            new ShardScanInstructionNode(),
            broadcast(),
            new ShuffleProducerInstructionNode(List.of(0), 2, List.of("node-0", "node-1"), "qid", 9, "left")
        );

        assertSame(input, InstructionOrdering.aggregatePreparationLast(input));
    }

    /** Relative order is preserved inside both groups, so a multi-preparation chain stays deterministic. */
    public void testRelativeOrderIsPreservedWithinEachGroup() {
        InstructionNode scan = new ShardScanInstructionNode();
        InstructionNode bcast = broadcast();
        InstructionNode partial = new PartialAggregateInstructionNode();
        InstructionNode last = new FinalAggregateInstructionNode();

        List<InstructionNode> ordered = InstructionOrdering.aggregatePreparationLast(List.of(partial, scan, last, bcast));

        assertEquals(List.of(scan, bcast, partial, last), ordered);
    }
}
