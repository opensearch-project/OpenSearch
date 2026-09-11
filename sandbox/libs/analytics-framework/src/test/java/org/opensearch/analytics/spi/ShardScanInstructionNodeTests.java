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

import java.io.IOException;

public class ShardScanInstructionNodeTests extends OpenSearchTestCase {

    public void testShardScanInstructionNodeWireRoundtripWithTargetPartitions() throws IOException {
        ShardScanInstructionNode original = new ShardScanInstructionNode(true, "test_table", 8);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShardScanInstructionNode decoded = new ShardScanInstructionNode(in);
                assertEquals("test_table", decoded.getLogicalTableName());
                assertTrue(decoded.requestsRowIds());
                assertEquals(Integer.valueOf(8), decoded.getTargetPartitions());
                assertEquals(InstructionType.SETUP_SHARD_SCAN, decoded.type());
            }
        }
    }

    public void testShardScanInstructionNodeWireRoundtripWithNullTargetPartitions() throws IOException {
        ShardScanInstructionNode original = new ShardScanInstructionNode(false, "test_table");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShardScanInstructionNode decoded = new ShardScanInstructionNode(in);
                assertEquals("test_table", decoded.getLogicalTableName());
                assertFalse(decoded.requestsRowIds());
                assertNull(decoded.getTargetPartitions());
                assertEquals(InstructionType.SETUP_SHARD_SCAN, decoded.type());
            }
        }
    }

    public void testShardScanWithDelegationInstructionNodeWireRoundtrip() throws IOException {
        ShardScanWithDelegationInstructionNode original = new ShardScanWithDelegationInstructionNode(
            FilterTreeShape.CONJUNCTIVE,
            3,
            true,
            "delegated_table",
            12
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ShardScanWithDelegationInstructionNode decoded = new ShardScanWithDelegationInstructionNode(in);
                assertEquals("delegated_table", decoded.getLogicalTableName());
                assertTrue(decoded.requestsRowIds());
                assertEquals(Integer.valueOf(12), decoded.getTargetPartitions());
                assertEquals(FilterTreeShape.CONJUNCTIVE, decoded.getTreeShape());
                assertEquals(3, decoded.getDelegatedPredicateCount());
                assertEquals(InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION, decoded.type());
            }
        }
    }

    public void testShardScanInstructionNodeWireRoundtripLegacyVersion() throws IOException {
        ShardScanInstructionNode original = new ShardScanInstructionNode(true, "test_table", 8);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(org.opensearch.Version.V_3_8_0);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(org.opensearch.Version.V_3_8_0);
                ShardScanInstructionNode decoded = new ShardScanInstructionNode(in);
                assertEquals("test_table", decoded.getLogicalTableName());
                assertTrue(decoded.requestsRowIds());
                assertNull(decoded.getTargetPartitions());
                assertEquals(InstructionType.SETUP_SHARD_SCAN, decoded.type());
            }
        }
    }

    public void testShardScanWithDelegationInstructionNodeWireRoundtripLegacyVersion() throws IOException {
        ShardScanWithDelegationInstructionNode original = new ShardScanWithDelegationInstructionNode(
            FilterTreeShape.CONJUNCTIVE,
            3,
            true,
            "delegated_table",
            12
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(org.opensearch.Version.V_3_8_0);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(org.opensearch.Version.V_3_8_0);
                ShardScanWithDelegationInstructionNode decoded = new ShardScanWithDelegationInstructionNode(in);
                assertEquals("delegated_table", decoded.getLogicalTableName());
                assertTrue(decoded.requestsRowIds());
                assertNull(decoded.getTargetPartitions());
                assertEquals(FilterTreeShape.CONJUNCTIVE, decoded.getTreeShape());
                assertEquals(3, decoded.getDelegatedPredicateCount());
                assertEquals(InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION, decoded.type());
            }
        }
    }
}
