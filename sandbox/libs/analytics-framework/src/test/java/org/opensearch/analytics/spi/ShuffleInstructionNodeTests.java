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

import java.util.List;

public class ShuffleInstructionNodeTests extends OpenSearchTestCase {

    public void testShuffleScanWireRoundtrip() throws Exception {
        ShuffleScanInstructionNode original = new ShuffleScanInstructionNode("input-0", 2, 5);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShuffleScanInstructionNode decoded = new ShuffleScanInstructionNode(in);
                assertEquals("input-0", decoded.getNamedInputId());
                assertEquals(2, decoded.getShufflePartitionIndex());
                assertEquals(5, decoded.getExpectedSenders());
                assertEquals(InstructionType.SHUFFLE_SCAN, decoded.type());
            }
        }
    }

    public void testShuffleProducerWireRoundtrip() throws Exception {
        ShuffleProducerInstructionNode original = new ShuffleProducerInstructionNode(
            List.of(3, 7),
            4,
            List.of("node-a", "node-b", "node-c", "node-d"),
            "q-xyz",
            2,
            "right"
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShuffleProducerInstructionNode decoded = new ShuffleProducerInstructionNode(in);
                assertEquals(List.of(3, 7), decoded.getHashKeyChannels());
                assertEquals(4, decoded.getPartitionCount());
                assertEquals(List.of("node-a", "node-b", "node-c", "node-d"), decoded.getTargetWorkerNodeIds());
                assertEquals("q-xyz", decoded.getQueryId());
                assertEquals(2, decoded.getTargetStageId());
                assertEquals("right", decoded.getSide());
                assertEquals(InstructionType.SHUFFLE_PRODUCER, decoded.type());
            }
        }
    }

    public void testInstructionTypeReadNodeDispatch() throws Exception {
        ShuffleScanInstructionNode scan = new ShuffleScanInstructionNode("x", 0, 1);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            scan.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                InstructionNode decoded = InstructionType.SHUFFLE_SCAN.readNode(in);
                assertTrue(decoded instanceof ShuffleScanInstructionNode);
            }
        }
    }
}
