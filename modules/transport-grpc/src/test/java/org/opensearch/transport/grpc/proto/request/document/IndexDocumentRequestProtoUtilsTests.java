/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import com.google.protobuf.ByteString;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.protobufs.IndexDocumentRequest;
import org.opensearch.protobufs.OpType;
import org.opensearch.protobufs.Refresh;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for IndexDocumentRequestProtoUtils.
 */
public class IndexDocumentRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoBasicFields() {
        IndexDocumentRequest protoRequest = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        IndexRequest indexRequest = IndexDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", indexRequest.index());
        assertEquals("test-id", indexRequest.id());
        assertNotNull(indexRequest.source());
        assertEquals(XContentType.JSON, indexRequest.getContentType());
    }

    public void testFromProtoWithAllFields() {
        IndexDocumentRequest protoRequest = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .setOpType(OpType.OP_TYPE_CREATE)
            .setRouting("test-routing")
            .setRefresh(Refresh.REFRESH_TRUE)
            .setIfSeqNo(5L)
            .setIfPrimaryTerm(2L)
            .setPipeline("test-pipeline")
            .setTimeout("30s")
            .build();

        IndexRequest indexRequest = IndexDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", indexRequest.index());
        assertEquals("test-id", indexRequest.id());
        assertEquals("test-routing", indexRequest.routing());
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, indexRequest.getRefreshPolicy());
        assertEquals(5L, indexRequest.ifSeqNo());
        assertEquals(2L, indexRequest.ifPrimaryTerm());
        assertEquals("test-pipeline", indexRequest.getPipeline());
        assertEquals("30s", indexRequest.timeout().toString());
    }

    public void testFromProtoMissingIndex() {
        IndexDocumentRequest protoRequest = IndexDocumentRequest.newBuilder()
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testFromProtoMissingDocument() {
        IndexDocumentRequest protoRequest = IndexDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Document content is required (use bytes_request_body field)", exception.getMessage());
    }

    public void testFromProtoObjectMapNotSupported() {
        IndexDocumentRequest protoRequest = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(org.opensearch.protobufs.ObjectMap.newBuilder().build())
            .build();

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> IndexDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("ObjectMap request body not yet supported, use bytes_request_body", exception.getMessage());
    }

    public void testRefreshPolicyConversion() {
        // Test REFRESH_TRUE
        IndexDocumentRequest protoRequest1 = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .setRefresh(Refresh.REFRESH_TRUE)
            .build();
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, IndexDocumentRequestProtoUtils.fromProto(protoRequest1).getRefreshPolicy());

        // Test REFRESH_WAIT_FOR
        IndexDocumentRequest protoRequest2 = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .setRefresh(Refresh.REFRESH_WAIT_FOR)
            .build();
        assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, IndexDocumentRequestProtoUtils.fromProto(protoRequest2).getRefreshPolicy());

        // Test REFRESH_FALSE (default)
        IndexDocumentRequest protoRequest3 = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .setRefresh(Refresh.REFRESH_FALSE)
            .build();
        assertEquals(WriteRequest.RefreshPolicy.NONE, IndexDocumentRequestProtoUtils.fromProto(protoRequest3).getRefreshPolicy());
    }
}
