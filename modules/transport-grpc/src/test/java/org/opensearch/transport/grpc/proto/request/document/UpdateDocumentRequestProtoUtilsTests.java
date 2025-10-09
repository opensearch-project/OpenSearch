/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import com.google.protobuf.ByteString;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.protobufs.Refresh;
import org.opensearch.protobufs.UpdateDocumentRequest;
import org.opensearch.protobufs.UpdateDocumentRequestBody;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive unit tests for UpdateDocumentRequestProtoUtils.
 */
public class UpdateDocumentRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoBasicFields() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();

        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", updateRequest.index());
        assertEquals("test-id", updateRequest.id());
        assertNotNull(updateRequest.doc());
        // Note: docContentType() method doesn't exist, this is handled internally
    }

    public void testFromProtoWithAllFields() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("test-routing")
            .setRefresh(Refresh.REFRESH_TRUE)
            .setIfSeqNo(5L)
            .setIfPrimaryTerm(2L)
            .setTimeout("30s")
            .setRetryOnConflict(3)
            .setRequestBody(
                UpdateDocumentRequestBody.newBuilder()
                    .setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"updated_value\"}"))
                    .setBytesUpsert(ByteString.copyFromUtf8("{\"field\":\"upsert_value\", \"created\":true}"))
                    .setDocAsUpsert(true)
                    .setDetectNoop(false)
                    .build()
            )
            .build();

        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", updateRequest.index());
        assertEquals("test-id", updateRequest.id());
        assertEquals("test-routing", updateRequest.routing());
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, updateRequest.getRefreshPolicy());
        assertEquals(5L, updateRequest.ifSeqNo());
        assertEquals(2L, updateRequest.ifPrimaryTerm());
        assertEquals("30s", updateRequest.timeout().toString());
        assertEquals(3, updateRequest.retryOnConflict());
        assertNotNull(updateRequest.doc());
        assertNotNull(updateRequest.upsertRequest());
        assertTrue(updateRequest.docAsUpsert());
        assertFalse(updateRequest.detectNoop());
    }

    public void testFromProtoWithScriptedUpsert() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(
                UpdateDocumentRequestBody.newBuilder()
                    .setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
                    .setScriptedUpsert(true)
                    .build()
            )
            .build();

        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", updateRequest.index());
        assertEquals("test-id", updateRequest.id());
        assertTrue(updateRequest.scriptedUpsert());
    }

    public void testFromProtoMissingIndex() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setId("test-id")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testFromProtoMissingId() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Document ID is required for update operations", exception.getMessage());
    }

    public void testFromProtoMissingRequestBody() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Update request body is required", exception.getMessage());
    }

    public void testFromProtoEmptyRequestBody() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().build())
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Update document content is required (use bytes_doc field)", exception.getMessage());
    }

    public void testRefreshPolicyConversion() {
        // Test REFRESH_TRUE
        UpdateDocumentRequest protoRequest1 = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRefresh(Refresh.REFRESH_TRUE)
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, UpdateDocumentRequestProtoUtils.fromProto(protoRequest1).getRefreshPolicy());

        // Test REFRESH_WAIT_FOR
        UpdateDocumentRequest protoRequest2 = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRefresh(Refresh.REFRESH_WAIT_FOR)
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();
        assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, UpdateDocumentRequestProtoUtils.fromProto(protoRequest2).getRefreshPolicy());

        // Test REFRESH_FALSE (default)
        UpdateDocumentRequest protoRequest3 = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRefresh(Refresh.REFRESH_FALSE)
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();
        assertEquals(WriteRequest.RefreshPolicy.NONE, UpdateDocumentRequestProtoUtils.fromProto(protoRequest3).getRefreshPolicy());
    }

    public void testVersionConstraints() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setIfSeqNo(10L)
            .setIfPrimaryTerm(5L)
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();

        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals(10L, updateRequest.ifSeqNo());
        assertEquals(5L, updateRequest.ifPrimaryTerm());
    }

    public void testRetryOnConflictBoundaries() {
        // Test minimum value
        UpdateDocumentRequest protoRequest1 = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRetryOnConflict(0)
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();
        assertEquals(0, UpdateDocumentRequestProtoUtils.fromProto(protoRequest1).retryOnConflict());

        // Test maximum reasonable value
        UpdateDocumentRequest protoRequest2 = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRetryOnConflict(10)
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();
        assertEquals(10, UpdateDocumentRequestProtoUtils.fromProto(protoRequest2).retryOnConflict());
    }

    public void testUpsertOnlyRequest() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(
                UpdateDocumentRequestBody.newBuilder()
                    .setBytesUpsert(ByteString.copyFromUtf8("{\"field\":\"upsert_only\", \"created\":true}"))
                    .build()
            )
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Update document content is required (use bytes_doc field)", exception.getMessage());
    }

    public void testInvalidJsonInDoc() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("invalid json")).build())
            .build();

        // Should not throw exception during conversion - OpenSearch will handle invalid JSON
        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);
        assertNotNull(updateRequest);
        assertEquals("test-index", updateRequest.index());
        assertEquals("test-id", updateRequest.id());
    }

    public void testEmptyStringsHandling() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("")
            .setId("")
            .setRouting("")
            .setTimeout("")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> UpdateDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testNullAndDefaultValues() {
        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}")).build())
            .build();

        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);

        // Test default values
        assertEquals(WriteRequest.RefreshPolicy.NONE, updateRequest.getRefreshPolicy());
        assertEquals(-2L, updateRequest.ifSeqNo()); // UNASSIGNED_SEQ_NO
        assertEquals(0L, updateRequest.ifPrimaryTerm()); // UNASSIGNED_PRIMARY_TERM
        assertEquals(0, updateRequest.retryOnConflict());
        assertNull(updateRequest.routing());
        assertNull(updateRequest.upsertRequest());
        assertTrue(updateRequest.detectNoop()); // Default is true
        assertFalse(updateRequest.docAsUpsert()); // Default is false
        assertFalse(updateRequest.scriptedUpsert()); // Default is false
    }

    public void testLargeDocument() {
        // Test with a large document (1MB)
        StringBuilder largeDoc = new StringBuilder();
        largeDoc.append("{\"data\":\"");
        for (int i = 0; i < 100000; i++) {
            largeDoc.append("0123456789");
        }
        largeDoc.append("\"}");

        UpdateDocumentRequest protoRequest = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8(largeDoc.toString())).build())
            .build();

        UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(protoRequest);
        assertNotNull(updateRequest);
        assertEquals("test-index", updateRequest.index());
        assertEquals("test-id", updateRequest.id());
        assertTrue(updateRequest.doc().source().length() > 1000000);
    }
}
