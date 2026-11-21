/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link ShuffleSlots} + the N-ary {@link ShuffleWorkerSetupInstructionNode} wire shape.
 *
 * <p>The load-bearing property is BINARY WIRE COMPATIBILITY: a two-input hash join must keep the
 * historical {@code left}/{@code right} labels so its spill-file names, buffer keys and producer payload
 * tags are unchanged. Only a 3+-input consumer uses the positional form.
 */
public class ShuffleSlotsTests extends OpenSearchTestCase {

    public void testBinaryArityKeepsHistoricalLabels() {
        // Arity 1 (the FINAL-aggregate shuffle worker) and arity 2 (hash join) must map to left/right —
        // any other choice would rename spill files and break the producer/consumer rendezvous.
        assertEquals("left", ShuffleSlots.forInput(0, 1));
        assertEquals("left", ShuffleSlots.forInput(0, 2));
        assertEquals("right", ShuffleSlots.forInput(1, 2));
    }

    public void testHigherArityUsesPositionalLabels() {
        assertEquals("in0", ShuffleSlots.forInput(0, 3));
        assertEquals("in1", ShuffleSlots.forInput(1, 3));
        assertEquals("in2", ShuffleSlots.forInput(2, 3));
        // Distinctness across the whole range is what keeps one slot's rows out of another's buffer.
        assertEquals(4, java.util.stream.IntStream.range(0, 4).mapToObj(i -> ShuffleSlots.forInput(i, 4)).distinct().count());
    }

    public void testForInputRejectsOutOfRange() {
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.forInput(2, 2));
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.forInput(-1, 2));
    }

    public void testValidateRejectsPathTraversal() {
        // Slot labels name on-disk spill files, so a separator or ".." must never reach the filesystem.
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.validate("../escape"));
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.validate("a/b"));
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.validate(".."));
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.validate(""));
        expectThrows(IllegalArgumentException.class, () -> ShuffleSlots.validate(null));
        assertEquals("left", ShuffleSlots.validate("left"));
        assertEquals("in7", ShuffleSlots.validate("in7"));
    }

    public void testWorkerSetupNaryWireRoundtrip() throws Exception {
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        bySlot.put("in0", 5);
        bySlot.put("in1", 3);
        bySlot.put("in2", 7);
        ShuffleWorkerSetupInstructionNode original = new ShuffleWorkerSetupInstructionNode("q-1", 9, 2, bySlot, false);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShuffleWorkerSetupInstructionNode decoded = new ShuffleWorkerSetupInstructionNode(in);
                assertEquals(bySlot, decoded.getExpectedSendersBySlot());
                assertEquals(5, decoded.getExpectedSenders("in0"));
                assertEquals(7, decoded.getExpectedSenders("in2"));
                assertEquals("an undeclared slot reads as -1 (leave unchanged)", -1, decoded.getExpectedSenders("in9"));
                assertEquals("q-1", decoded.getQueryId());
                assertEquals(9, decoded.getTargetStageId());
                assertEquals(2, decoded.getPartitionIndex());
                assertFalse(decoded.getPreferHashJoin());
            }
        }
    }

    public void testWorkerSetupBinaryCtorRoundtripsThroughSlots() throws Exception {
        ShuffleWorkerSetupInstructionNode original = new ShuffleWorkerSetupInstructionNode("q-2", 4, -1, 7, 3, true);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShuffleWorkerSetupInstructionNode decoded = new ShuffleWorkerSetupInstructionNode(in);
                assertEquals(7, decoded.getLeftExpectedSenders());
                assertEquals(3, decoded.getRightExpectedSenders());
                assertEquals(
                    "binary ctor declares exactly the two historical slots",
                    Map.of("left", 7, "right", 3),
                    decoded.getExpectedSendersBySlot()
                );
            }
        }
    }

    public void testWorkerSetupBinaryCtorOmitsNegativeSide() {
        // The single-slot agg-shuffle path passes rightExpectedSenders=-1 meaning "no right slot at all";
        // declaring it as -1 would leave a slot the buffer's awaitReady could never satisfy.
        ShuffleWorkerSetupInstructionNode node = new ShuffleWorkerSetupInstructionNode("q-3", 4, 0, 2, -1, true);
        assertEquals(Map.of("left", 2), node.getExpectedSendersBySlot());
        assertEquals(-1, node.getRightExpectedSenders());
    }
}
