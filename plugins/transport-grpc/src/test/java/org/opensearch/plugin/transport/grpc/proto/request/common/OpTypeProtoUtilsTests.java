/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.common;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.protobufs.OpType;
import org.opensearch.test.OpenSearchTestCase;

public class OpTypeProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithOpTypeCreate() {
        // Test conversion from OpType.OP_TYPE_CREATE to DocWriteRequest.OpType.CREATE
        DocWriteRequest.OpType result = OpTypeProtoUtils.fromProto(OpType.OP_TYPE_CREATE);

        // Verify the result
        assertEquals("OP_TYPE_CREATE should convert to DocWriteRequest.OpType.CREATE", DocWriteRequest.OpType.CREATE, result);
    }

    public void testFromProtoWithOpTypeIndex() {
        // Test conversion from OpType.OP_TYPE_INDEX to DocWriteRequest.OpType.INDEX
        DocWriteRequest.OpType result = OpTypeProtoUtils.fromProto(OpType.OP_TYPE_INDEX);

        // Verify the result
        assertEquals("OP_TYPE_INDEX should convert to DocWriteRequest.OpType.INDEX", DocWriteRequest.OpType.INDEX, result);
    }

    public void testFromProtoWithOpTypeUnspecified() {
        // Test conversion from OpType.OP_TYPE_UNSPECIFIED, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> OpTypeProtoUtils.fromProto(OpType.OP_TYPE_UNSPECIFIED)
        );

        // Verify the exception message
        assertTrue("Exception message should mention 'Invalid optype'", exception.getMessage().contains("Invalid optype"));
    }

    public void testFromProtoWithUnrecognizedOpType() {
        // Test conversion with an unrecognized OpType, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> OpTypeProtoUtils.fromProto(OpType.UNRECOGNIZED)
        );

        // Verify the exception message
        assertTrue("Exception message should mention 'Invalid optype'", exception.getMessage().contains("Invalid optype"));
    }
}
