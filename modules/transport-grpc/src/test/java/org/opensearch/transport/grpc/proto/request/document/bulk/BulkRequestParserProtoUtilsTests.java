/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import com.google.protobuf.ByteString;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.DeleteOperation;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.OpType;
import org.opensearch.protobufs.OperationContainer;
import org.opensearch.protobufs.UpdateOperation;
import org.opensearch.protobufs.WriteOperation;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

public class BulkRequestParserProtoUtilsTests extends OpenSearchTestCase {

    public void testBuildCreateRequest() {
        WriteOperation writeOperation = WriteOperation.newBuilder()
            .setUnderscoreIndex("test-index")
            .setUnderscoreId("test-id")
            .setRouting("test-routing")
            .setRequireAlias(true)
            .build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            document,
            "default-index",
            "default-id",
            "default-routing",
            1L,
            VersionType.INTERNAL,
            "default-pipeline",
            1L,
            2L,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertEquals("Routing should match", "test-routing", indexRequest.routing());
        assertEquals("Version should match", 1L, indexRequest.version());
        assertEquals("VersionType should match", VersionType.INTERNAL, indexRequest.versionType());
        assertEquals("Pipeline should match", "default-pipeline", indexRequest.getPipeline());
        assertEquals("IfSeqNo should match", 1L, indexRequest.ifSeqNo());
        assertEquals("IfPrimaryTerm should match", 2L, indexRequest.ifPrimaryTerm());
        assertTrue("RequireAlias should match", indexRequest.isRequireAlias());
        assertEquals("Create flag should be true", DocWriteRequest.OpType.CREATE, indexRequest.opType());
    }

    public void testBuildIndexRequest() {
        IndexOperation indexOperation = IndexOperation.newBuilder()
            .setUnderscoreIndex("test-index")
            .setUnderscoreId("test-id")
            .setRouting("test-routing")
            .setVersion(2)
            .setVersionType(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL_GTE)
            .setPipeline("test-pipeline")
            .setIfSeqNo(3)
            .setIfPrimaryTerm(4)
            .setRequireAlias(true)
            .build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            document,
            null,
            "default-index",
            "default-id",
            "default-routing",
            1L,
            VersionType.INTERNAL,
            "default-pipeline",
            1L,
            2L,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertEquals("Routing should match", "test-routing", indexRequest.routing());
        assertEquals("Version should match", 2L, indexRequest.version());
        assertEquals("VersionType should match", VersionType.EXTERNAL_GTE, indexRequest.versionType());
        assertEquals("Pipeline should match", "test-pipeline", indexRequest.getPipeline());
        assertEquals("IfSeqNo should match", 3L, indexRequest.ifSeqNo());
        assertEquals("IfPrimaryTerm should match", 4L, indexRequest.ifPrimaryTerm());
        assertTrue("RequireAlias should match", indexRequest.isRequireAlias());
        assertNotEquals("Create flag should be false", DocWriteRequest.OpType.CREATE, indexRequest.opType());
    }

    public void testBuildIndexRequestWithOpType() {
        IndexOperation indexOperation = IndexOperation.newBuilder().setUnderscoreIndex("test-index").setUnderscoreId("test-id").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        OpType opType = org.opensearch.protobufs.OpType.OP_TYPE_CREATE;

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            document,
            opType,
            "default-index",
            "default-id",
            "default-routing",
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            "default-pipeline",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertEquals("Create flag should be true", DocWriteRequest.OpType.CREATE, indexRequest.opType());
    }

    public void testBuildDeleteRequest() {
        DeleteOperation deleteOperation = DeleteOperation.newBuilder()
            .setUnderscoreIndex("test-index")
            .setUnderscoreId("test-id")
            .setRouting("test-routing")
            .setVersion(2)
            .setVersionType(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL)
            .setIfSeqNo(3)
            .setIfPrimaryTerm(4)
            .build();

        DeleteRequest deleteRequest = BulkRequestParserProtoUtils.buildDeleteRequest(
            deleteOperation,
            "default-index",
            "default-id",
            "default-routing",
            1L,
            VersionType.INTERNAL,
            1L,
            2L
        );

        assertNotNull("DeleteRequest should not be null", deleteRequest);
        assertEquals("Index should match", "test-index", deleteRequest.index());
        assertEquals("Id should match", "test-id", deleteRequest.id());
        assertEquals("Routing should match", "test-routing", deleteRequest.routing());
        assertEquals("Version should match", 2L, deleteRequest.version());
        assertEquals("VersionType should match", VersionType.EXTERNAL, deleteRequest.versionType());
        assertEquals("IfSeqNo should match", 3L, deleteRequest.ifSeqNo());
        assertEquals("IfPrimaryTerm should match", 4L, deleteRequest.ifPrimaryTerm());
    }

    public void testBuildUpdateRequest() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder()
            .setUnderscoreIndex("test-index")
            .setUnderscoreId("test-id")
            .setRouting("test-routing")
            .setRetryOnConflict(3)
            .setIfSeqNo(4)
            .setIfPrimaryTerm(5)
            .setRequireAlias(true)
            .build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(ByteString.copyFrom(document))
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setDocAsUpsert(true).setDetectNoop(true).build())
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            document,
            bulkRequestBody,
            "default-index",
            "default-id",
            "default-routing",
            null,
            1,
            "default-pipeline",
            1L,
            2L,
            false
        );

        assertNotNull("UpdateRequest should not be null", updateRequest);
        assertEquals("Index should match", "test-index", updateRequest.index());
        assertEquals("Id should match", "test-id", updateRequest.id());
        assertEquals("Routing should match", "test-routing", updateRequest.routing());
        assertEquals("RetryOnConflict should match", 3, updateRequest.retryOnConflict());
        assertEquals("IfSeqNo should match", 4L, updateRequest.ifSeqNo());
        assertEquals("IfPrimaryTerm should match", 5L, updateRequest.ifPrimaryTerm());
        assertTrue("RequireAlias should match", updateRequest.isRequireAlias());
        assertTrue("DocAsUpsert should match", updateRequest.docAsUpsert());
        assertTrue("DetectNoop should match", updateRequest.detectNoop());
    }

    public void testGetDocWriteRequests() {
        IndexOperation indexOp = IndexOperation.newBuilder().setUnderscoreIndex("test-index").setUnderscoreId("test-id-1").build();
        WriteOperation writeOp = WriteOperation.newBuilder().setUnderscoreIndex("test-index").setUnderscoreId("test-id-2").build();
        UpdateOperation updateOp = UpdateOperation.newBuilder().setUnderscoreIndex("test-index").setUnderscoreId("test-id-3").build();
        DeleteOperation deleteOp = DeleteOperation.newBuilder().setUnderscoreIndex("test-index").setUnderscoreId("test-id-4").build();

        BulkRequestBody indexBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setIndex(indexOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value1\"}"))
            .build();

        BulkRequestBody createBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setCreate(writeOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value2\"}"))
            .build();

        BulkRequestBody updateBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value3\"}"))
            .build();

        BulkRequestBody deleteBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setDelete(deleteOp).build())
            .build();

        BulkRequest request = BulkRequest.newBuilder()
            .addRequestBody(indexBody)
            .addRequestBody(createBody)
            .addRequestBody(updateBody)
            .addRequestBody(deleteBody)
            .build();

        DocWriteRequest<?>[] requests = BulkRequestParserProtoUtils.getDocWriteRequests(
            request,
            "default-index",
            "default-routing",
            null,
            "default-pipeline",
            false
        );

        assertNotNull("Requests should not be null", requests);
        assertEquals("Should have 4 requests", 4, requests.length);
        assertTrue("First request should be an IndexRequest", requests[0] instanceof IndexRequest);
        assertTrue(
            "Second request should be an IndexRequest with create=true",
            requests[1] instanceof IndexRequest && ((IndexRequest) requests[1]).opType().equals(DocWriteRequest.OpType.CREATE)
        );
        assertTrue("Third request should be an UpdateRequest", requests[2] instanceof UpdateRequest);
        assertTrue("Fourth request should be a DeleteRequest", requests[3] instanceof DeleteRequest);

        IndexRequest indexRequest = (IndexRequest) requests[0];
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id-1", indexRequest.id());

        IndexRequest createRequest = (IndexRequest) requests[1];
        assertEquals("Index should match", "test-index", createRequest.index());
        assertEquals("Id should match", "test-id-2", createRequest.id());
        assertEquals("Create flag should be true", DocWriteRequest.OpType.CREATE, createRequest.opType());

        UpdateRequest updateRequest = (UpdateRequest) requests[2];
        assertEquals("Index should match", "test-index", updateRequest.index());
        assertEquals("Id should match", "test-id-3", updateRequest.id());

        DeleteRequest deleteRequest = (DeleteRequest) requests[3];
        assertEquals("Index should match", "test-index", deleteRequest.index());
        assertEquals("Id should match", "test-id-4", deleteRequest.id());
    }

    public void testGetDocWriteRequestsWithInvalidOperation() {
        BulkRequestBody invalidBody = BulkRequestBody.newBuilder().build();

        BulkRequest request = BulkRequest.newBuilder().addRequestBody(invalidBody).build();

        expectThrows(
            IllegalArgumentException.class,
            () -> BulkRequestParserProtoUtils.getDocWriteRequests(
                request,
                "default-index",
                "default-routing",
                null,
                "default-pipeline",
                false
            )
        );
    }
}
