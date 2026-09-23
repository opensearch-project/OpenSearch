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

public class BroadcastInjectionInstructionNodeTests extends OpenSearchTestCase {

    public void testWireRoundtripPreservesFields() throws Exception {
        byte[] payload = new byte[] { 1, 2, 3, 4, 5 };
        BroadcastInjectionInstructionNode original = new BroadcastInjectionInstructionNode("input-7", 1, payload);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                BroadcastInjectionInstructionNode decoded = new BroadcastInjectionInstructionNode(in);
                assertEquals("input-7", decoded.getNamedInputId());
                assertEquals(1, decoded.getBuildSideIndex());
                assertArrayEquals(payload, decoded.getBroadcastData());
                assertEquals(InstructionType.INJECT_BROADCAST, decoded.type());
            }
        }
    }

    public void testInstructionTypeRoundtripViaEnum() throws Exception {
        BroadcastInjectionInstructionNode original = new BroadcastInjectionInstructionNode("input-0", 0, new byte[] { 0x42 });
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                InstructionNode decoded = InstructionType.INJECT_BROADCAST.readNode(in);
                assertTrue(decoded instanceof BroadcastInjectionInstructionNode);
                BroadcastInjectionInstructionNode cast = (BroadcastInjectionInstructionNode) decoded;
                assertEquals("input-0", cast.getNamedInputId());
                assertEquals(0, cast.getBuildSideIndex());
                assertArrayEquals(new byte[] { 0x42 }, cast.getBroadcastData());
            }
        }
    }
}
