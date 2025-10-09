/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.protobufs.DeleteDocumentRequest;
import org.opensearch.protobufs.Refresh;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive unit tests for DeleteDocumentRequestProtoUtils.
 */
public class DeleteDocumentRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoBasicFields() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        DeleteRequest deleteRequest = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", deleteRequest.index());
        assertEquals("test-id", deleteRequest.id());
    }

    public void testFromProtoWithAllFields() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("test-routing")
            .setRefresh(Refresh.REFRESH_TRUE)
            .setIfSeqNo(5L)
            .setIfPrimaryTerm(2L)
            .setTimeout("30s")
            .build();

        DeleteRequest deleteRequest = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", deleteRequest.index());
        assertEquals("test-id", deleteRequest.id());
        assertEquals("test-routing", deleteRequest.routing());
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, deleteRequest.getRefreshPolicy());
        assertEquals(5L, deleteRequest.ifSeqNo());
        assertEquals(2L, deleteRequest.ifPrimaryTerm());
        assertEquals("30s", deleteRequest.timeout().toString());
    }

    public void testFromProtoMissingIndex() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder().setId("test-id").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DeleteDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testFromProtoMissingId() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder().setIndex("test-index").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DeleteDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Document ID is required for delete operations", exception.getMessage());
    }

    public void testFromProtoEmptyIndex() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder().setIndex("").setId("test-id").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DeleteDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testFromProtoEmptyId() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DeleteDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Document ID is required for delete operations", exception.getMessage());
    }

    public void testRefreshPolicyConversion() {
        // Test REFRESH_TRUE
        DeleteDocumentRequest protoRequest1 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRefresh(Refresh.REFRESH_TRUE)
            .build();
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, DeleteDocumentRequestProtoUtils.fromProto(protoRequest1).getRefreshPolicy());

        // Test REFRESH_WAIT_FOR
        DeleteDocumentRequest protoRequest2 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRefresh(Refresh.REFRESH_WAIT_FOR)
            .build();
        assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, DeleteDocumentRequestProtoUtils.fromProto(protoRequest2).getRefreshPolicy());

        // Test REFRESH_FALSE (default)
        DeleteDocumentRequest protoRequest3 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRefresh(Refresh.REFRESH_FALSE)
            .build();
        assertEquals(WriteRequest.RefreshPolicy.NONE, DeleteDocumentRequestProtoUtils.fromProto(protoRequest3).getRefreshPolicy());

        // Test unspecified refresh (should default to NONE)
        DeleteDocumentRequest protoRequest4 = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();
        assertEquals(WriteRequest.RefreshPolicy.NONE, DeleteDocumentRequestProtoUtils.fromProto(protoRequest4).getRefreshPolicy());
    }

    public void testVersionConstraints() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setIfSeqNo(10L)
            .setIfPrimaryTerm(5L)
            .build();

        DeleteRequest deleteRequest = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals(10L, deleteRequest.ifSeqNo());
        assertEquals(5L, deleteRequest.ifPrimaryTerm());
    }

    public void testVersionConstraintsBoundaries() {
        // Test minimum values
        DeleteDocumentRequest protoRequest1 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setIfSeqNo(0L)
            .setIfPrimaryTerm(1L)
            .build();
        DeleteRequest deleteRequest1 = DeleteDocumentRequestProtoUtils.fromProto(protoRequest1);
        assertEquals(0L, deleteRequest1.ifSeqNo());
        assertEquals(1L, deleteRequest1.ifPrimaryTerm());

        // Test large values
        DeleteDocumentRequest protoRequest2 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setIfSeqNo(Long.MAX_VALUE)
            .setIfPrimaryTerm(Long.MAX_VALUE)
            .build();
        DeleteRequest deleteRequest2 = DeleteDocumentRequestProtoUtils.fromProto(protoRequest2);
        assertEquals(Long.MAX_VALUE, deleteRequest2.ifSeqNo());
        assertEquals(Long.MAX_VALUE, deleteRequest2.ifPrimaryTerm());
    }

    public void testDefaultValues() {
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        DeleteRequest deleteRequest = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);

        // Test default values
        assertEquals(WriteRequest.RefreshPolicy.NONE, deleteRequest.getRefreshPolicy());
        assertEquals(-2L, deleteRequest.ifSeqNo()); // UNASSIGNED_SEQ_NO
        assertEquals(0L, deleteRequest.ifPrimaryTerm()); // UNASSIGNED_PRIMARY_TERM
        assertNull(deleteRequest.routing());
    }

    public void testRoutingValues() {
        // Test with routing
        DeleteDocumentRequest protoRequest1 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("user123")
            .build();
        assertEquals("user123", DeleteDocumentRequestProtoUtils.fromProto(protoRequest1).routing());

        // Test with empty routing (should be treated as null)
        DeleteDocumentRequest protoRequest2 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("")
            .build();
        assertNull(DeleteDocumentRequestProtoUtils.fromProto(protoRequest2).routing());

        // Test without routing
        DeleteDocumentRequest protoRequest3 = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();
        assertNull(DeleteDocumentRequestProtoUtils.fromProto(protoRequest3).routing());
    }

    public void testTimeoutValues() {
        // Test with timeout
        DeleteDocumentRequest protoRequest1 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setTimeout("5m")
            .build();
        assertEquals("5m", DeleteDocumentRequestProtoUtils.fromProto(protoRequest1).timeout().toString());

        // Test with different timeout formats
        DeleteDocumentRequest protoRequest2 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setTimeout("30s")
            .build();
        assertEquals("30s", DeleteDocumentRequestProtoUtils.fromProto(protoRequest2).timeout().toString());

        // Test with milliseconds
        DeleteDocumentRequest protoRequest3 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setTimeout("1000ms")
            .build();
        assertEquals("1s", DeleteDocumentRequestProtoUtils.fromProto(protoRequest3).timeout().toString());

        // Test with empty timeout (should use default)
        DeleteDocumentRequest protoRequest4 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setTimeout("")
            .build();
        assertNotNull(DeleteDocumentRequestProtoUtils.fromProto(protoRequest4).timeout());

        // Test without timeout
        DeleteDocumentRequest protoRequest5 = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();
        assertNotNull(DeleteDocumentRequestProtoUtils.fromProto(protoRequest5).timeout());
    }

    public void testSpecialCharactersInFields() {
        // Test with special characters in index name
        DeleteDocumentRequest protoRequest1 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index-with-dashes_and_underscores.and.dots")
            .setId("test-id")
            .build();
        assertEquals("test-index-with-dashes_and_underscores.and.dots", DeleteDocumentRequestProtoUtils.fromProto(protoRequest1).index());

        // Test with special characters in ID
        DeleteDocumentRequest protoRequest2 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test:id/with\\special@characters#and$symbols%")
            .build();
        assertEquals("test:id/with\\special@characters#and$symbols%", DeleteDocumentRequestProtoUtils.fromProto(protoRequest2).id());

        // Test with Unicode characters
        DeleteDocumentRequest protoRequest3 = DeleteDocumentRequest.newBuilder()
            .setIndex("测试索引")
            .setId("测试文档ID")
            .setRouting("用户路由")
            .build();
        DeleteRequest deleteRequest3 = DeleteDocumentRequestProtoUtils.fromProto(protoRequest3);
        assertEquals("测试索引", deleteRequest3.index());
        assertEquals("测试文档ID", deleteRequest3.id());
        assertEquals("用户路由", deleteRequest3.routing());
    }

    public void testNegativeVersionConstraints() {
        // Test with negative sequence numbers (should be allowed for special values)
        DeleteDocumentRequest protoRequest1 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setIfSeqNo(-1L) // NO_OPS_PERFORMED
            .setIfPrimaryTerm(1L)
            .build();
        assertEquals(-1L, DeleteDocumentRequestProtoUtils.fromProto(protoRequest1).ifSeqNo());

        DeleteDocumentRequest protoRequest2 = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setIfSeqNo(-2L) // UNASSIGNED_SEQ_NO
            .setIfPrimaryTerm(0L)
            .build();
        assertEquals(-2L, DeleteDocumentRequestProtoUtils.fromProto(protoRequest2).ifSeqNo());
    }

    public void testComplexScenarios() {
        // Test with all optional fields set to non-default values
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder()
            .setIndex("complex-test-index")
            .setId("complex-test-id")
            .setRouting("shard-routing-key")
            .setRefresh(Refresh.REFRESH_WAIT_FOR)
            .setIfSeqNo(42L)
            .setIfPrimaryTerm(7L)
            .setTimeout("2m30s")
            .build();

        DeleteRequest deleteRequest = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("complex-test-index", deleteRequest.index());
        assertEquals("complex-test-id", deleteRequest.id());
        assertEquals("shard-routing-key", deleteRequest.routing());
        assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, deleteRequest.getRefreshPolicy());
        assertEquals(42L, deleteRequest.ifSeqNo());
        assertEquals(7L, deleteRequest.ifPrimaryTerm());
        assertEquals("2m30s", deleteRequest.timeout().toString());
    }

    public void testRequestConsistency() {
        // Test that multiple conversions of the same proto request yield identical results
        DeleteDocumentRequest protoRequest = DeleteDocumentRequest.newBuilder()
            .setIndex("consistency-test")
            .setId("doc-123")
            .setRouting("routing-key")
            .setRefresh(Refresh.REFRESH_TRUE)
            .setIfSeqNo(100L)
            .setIfPrimaryTerm(3L)
            .setTimeout("45s")
            .build();

        DeleteRequest deleteRequest1 = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);
        DeleteRequest deleteRequest2 = DeleteDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals(deleteRequest1.index(), deleteRequest2.index());
        assertEquals(deleteRequest1.id(), deleteRequest2.id());
        assertEquals(deleteRequest1.routing(), deleteRequest2.routing());
        assertEquals(deleteRequest1.getRefreshPolicy(), deleteRequest2.getRefreshPolicy());
        assertEquals(deleteRequest1.ifSeqNo(), deleteRequest2.ifSeqNo());
        assertEquals(deleteRequest1.ifPrimaryTerm(), deleteRequest2.ifPrimaryTerm());
        assertEquals(deleteRequest1.timeout().toString(), deleteRequest2.timeout().toString());
    }
}
