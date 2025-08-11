/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.protobufs.OpType;
import org.opensearch.test.OpenSearchTestCase;

public class OpTypeProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithOpTypeCreate() {
        OpType opType = OpType.newBuilder().setOpTypeCreate(true).build();
        DocWriteRequest.OpType result = OpTypeProtoUtils.fromProto(opType);

        assertEquals("OP_TYPE_CREATE should convert to DocWriteRequest.OpType.CREATE", DocWriteRequest.OpType.CREATE, result);
    }

    public void testFromProtoWithOpTypeIndex() {
        OpType opType = OpType.newBuilder().setOpTypeIndex(true).build();
        DocWriteRequest.OpType result = OpTypeProtoUtils.fromProto(opType);

        assertEquals("OP_TYPE_INDEX should convert to DocWriteRequest.OpType.INDEX", DocWriteRequest.OpType.INDEX, result);
    }

    public void testFromProtoWithOpTypeUnspecified() {
        OpType opType = OpType.newBuilder().build();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> OpTypeProtoUtils.fromProto(opType)
        );

        assertTrue("Exception message should mention 'Invalid optype'", exception.getMessage().contains("Invalid optype"));
    }

    public void testFromProtoWithUnrecognizedOpType() {
        OpType opType = OpType.newBuilder().build();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> OpTypeProtoUtils.fromProto(opType)
        );

        assertTrue("Exception message should mention 'Invalid optype'", exception.getMessage().contains("Invalid optype"));
    }
}
