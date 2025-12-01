/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
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
            .setXIndex("test-index")
            .setXId("test-id")
            .setRouting("test-routing")
            .setRequireAlias(true)
            .build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
            .setXIndex("test-index")
            .setXId("test-id")
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
            UnsafeByteOperations.unsafeWrap(document),
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
        IndexOperation indexOperation = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        OpType opType = org.opensearch.protobufs.OpType.OP_TYPE_CREATE;

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
            .setXIndex("test-index")
            .setXId("test-id")
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
            .setXIndex("test-index")
            .setXId("test-id")
            .setRouting("test-routing")
            .setRetryOnConflict(3)
            .setIfSeqNo(4)
            .setIfPrimaryTerm(5)
            .setRequireAlias(true)
            .build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setDocAsUpsert(true).setDetectNoop(true).build())
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id-1").build();
        WriteOperation writeOp = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id-2").build();
        UpdateOperation updateOp = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id-3").build();
        DeleteOperation deleteOp = DeleteOperation.newBuilder().setXIndex("test-index").setXId("test-id-4").build();

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
            .addBulkRequestBody(indexBody)
            .addBulkRequestBody(createBody)
            .addBulkRequestBody(updateBody)
            .addBulkRequestBody(deleteBody)
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

        BulkRequest request = BulkRequest.newBuilder().addBulkRequestBody(invalidBody).build();

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

    public void testBuildCreateRequestWithDefaults() {
        WriteOperation writeOperation = WriteOperation.newBuilder().build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertEquals("Index should use default", "default-index", indexRequest.index());
        assertEquals("Id should use default", "default-id", indexRequest.id());
        assertEquals("Routing should use default", "default-routing", indexRequest.routing());
        assertEquals("Pipeline should use default", "default-pipeline", indexRequest.getPipeline());
        assertFalse("RequireAlias should use default", indexRequest.isRequireAlias());
    }

    public void testBuildCreateRequestWithPipeline() {
        WriteOperation writeOperation = WriteOperation.newBuilder().setPipeline("custom-pipeline").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(document),
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

        assertEquals("Pipeline should use custom value", "custom-pipeline", indexRequest.getPipeline());
    }

    public void testBuildIndexRequestWithAllFields() {
        IndexOperation indexOperation = IndexOperation.newBuilder()
            .setXIndex("test-index")
            .setXId("test-id")
            .setRouting("test-routing")
            .setVersion(2)
            .setVersionType(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL)
            .setPipeline("test-pipeline")
            .setIfSeqNo(3)
            .setIfPrimaryTerm(4)
            .setRequireAlias(true)
            .build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(document),
            OpType.OP_TYPE_INDEX,
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
        assertEquals("VersionType should match", VersionType.EXTERNAL, indexRequest.versionType());
        assertEquals("Pipeline should match", "test-pipeline", indexRequest.getPipeline());
        assertEquals("IfSeqNo should match", 3L, indexRequest.ifSeqNo());
        assertEquals("IfPrimaryTerm should match", 4L, indexRequest.ifPrimaryTerm());
        assertTrue("RequireAlias should match", indexRequest.isRequireAlias());
        assertFalse("Create flag should be false for INDEX opType", indexRequest.opType().equals(DocWriteRequest.OpType.CREATE));
    }

    public void testBuildIndexRequestWithNullOpType() {
        IndexOperation indexOperation = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertFalse("Create flag should be false when opType is null", indexRequest.opType().equals(DocWriteRequest.OpType.CREATE));
    }

    public void testBuildUpdateRequestWithScript() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .setUpdateAction(
                org.opensearch.protobufs.UpdateAction.newBuilder()
                    .setScript(
                        org.opensearch.protobufs.Script.newBuilder()
                            .setInline(
                                org.opensearch.protobufs.InlineScript.newBuilder()
                                    .setSource("ctx._source.field = 'updated'")
                                    .setLang(
                                        org.opensearch.protobufs.ScriptLanguage.newBuilder()
                                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertNotNull("Script should be set", updateRequest.script());
        assertEquals("Script source should match", "ctx._source.field = 'updated'", updateRequest.script().getIdOrCode());
    }

    public void testBuildUpdateRequestWithUpsert() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);
        byte[] upsertDoc = "{\"upsert_field\":\"upsert_value\"}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setUpsert(ByteString.copyFrom(upsertDoc)).build())
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertNotNull("Upsert should be set", updateRequest.upsertRequest());
    }

    public void testBuildUpdateRequestWithScriptedUpsert() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setScriptedUpsert(true).build())
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertTrue("ScriptedUpsert should be true", updateRequest.scriptedUpsert());
    }

    public void testBuildUpdateRequestWithFetchSource() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .setUpdateAction(
                org.opensearch.protobufs.UpdateAction.newBuilder()
                    .setXSource(org.opensearch.protobufs.SourceConfig.newBuilder().setFetch(true).build())
                    .build()
            )
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertNotNull("FetchSource should be set", updateRequest.fetchSource());
    }

    public void testBuildUpdateRequestWithoutUpdateAction() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
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
    }

    public void testBuildDeleteRequestWithDefaults() {
        DeleteOperation deleteOperation = DeleteOperation.newBuilder().build();

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
        assertEquals("Index should use default", "default-index", deleteRequest.index());
        assertEquals("Id should use default", "default-id", deleteRequest.id());
        assertEquals("Routing should use default", "default-routing", deleteRequest.routing());
        assertEquals("Version should use default", 1L, deleteRequest.version());
        assertEquals("VersionType should use default", VersionType.INTERNAL, deleteRequest.versionType());
        assertEquals("IfSeqNo should use default", 1L, deleteRequest.ifSeqNo());
        assertEquals("IfPrimaryTerm should use default", 2L, deleteRequest.ifPrimaryTerm());
    }

    public void testGetDocWriteRequestsWithGlobalValues() {
        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        BulkRequestBody indexBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setIndex(indexOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value1\"}"))
            .build();

        BulkRequest request = BulkRequest.newBuilder()
            .addBulkRequestBody(indexBody)
            .setRouting("global-routing")
            .setPipeline("global-pipeline")
            .setRequireAlias(true)
            .build();

        DocWriteRequest<?>[] requests = BulkRequestParserProtoUtils.getDocWriteRequests(
            request,
            "default-index",
            null, // Pass null to test global routing
            null,
            null, // Pass null to test global pipeline
            null  // Pass null to test global requireAlias
        );

        assertNotNull("Requests should not be null", requests);
        assertEquals("Should have 1 request", 1, requests.length);
        assertTrue("First request should be an IndexRequest", requests[0] instanceof IndexRequest);

        IndexRequest indexRequest = (IndexRequest) requests[0];
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertEquals("Routing should use global value", "global-routing", indexRequest.routing());
        assertEquals("Pipeline should use global value", "global-pipeline", indexRequest.getPipeline());
        assertTrue("RequireAlias should use global value", indexRequest.isRequireAlias());
    }

    public void testGetDocWriteRequestsWithEmptyList() {
        BulkRequest request = BulkRequest.newBuilder().build();

        DocWriteRequest<?>[] requests = BulkRequestParserProtoUtils.getDocWriteRequests(
            request,
            "default-index",
            "default-routing",
            null,
            "default-pipeline",
            false
        );

        assertNotNull("Requests should not be null", requests);
        assertEquals("Should have 0 requests", 0, requests.length);
    }

    public void testFromProtoWithAllUpdateActionFields() {
        UpdateRequest updateRequest = new UpdateRequest("test-index", "test-id");
        byte[] document = "{\"doc\":{\"field\":\"value\"}}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setUpdateAction(
                org.opensearch.protobufs.UpdateAction.newBuilder()
                    .setScript(
                        org.opensearch.protobufs.Script.newBuilder()
                            .setInline(
                                org.opensearch.protobufs.InlineScript.newBuilder()
                                    .setSource("ctx._source.field = 'updated'")
                                    .setLang(
                                        org.opensearch.protobufs.ScriptLanguage.newBuilder()
                                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .setScriptedUpsert(true)
                    .setUpsert(ByteString.copyFromUtf8("{\"upsert_field\":\"upsert_value\"}"))
                    .setDocAsUpsert(true)
                    .setDetectNoop(false)
                    .setXSource(org.opensearch.protobufs.SourceConfig.newBuilder().setFetch(false).build())
                    .build()
            )
            .build();

        UpdateOperation updateOperation = UpdateOperation.newBuilder().setIfSeqNo(123L).setIfPrimaryTerm(456L).build();

        UpdateRequest result = BulkRequestParserProtoUtils.fromProto(
            updateRequest,
            UnsafeByteOperations.unsafeWrap(document),
            bulkRequestBody,
            123L,
            456L
        );

        assertNotNull("Result should not be null", result);
        assertNotNull("Script should be set", result.script());
        assertTrue("ScriptedUpsert should be true", result.scriptedUpsert());
        assertNotNull("Upsert should be set", result.upsertRequest());
        assertTrue("DocAsUpsert should be true", result.docAsUpsert());
        assertFalse("DetectNoop should be false", result.detectNoop());
        assertNotNull("FetchSource should be set", result.fetchSource());
        assertEquals("IfSeqNo should be set", 123L, result.ifSeqNo());
        assertEquals("IfPrimaryTerm should be set", 456L, result.ifPrimaryTerm());
    }

    public void testBuildCreateRequestWithSmileContent() throws Exception {
        WriteOperation writeOperation = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create SMILE-encoded document
        byte[] smileDocument = createSmileDocument();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(smileDocument),
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as SMILE
        assertEquals("Content type should be SMILE", "application/smile", indexRequest.getContentType().mediaType());
    }

    public void testBuildCreateRequestWithCborContent() throws Exception {
        WriteOperation writeOperation = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create CBOR-encoded document
        byte[] cborDocument = createCborDocument();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(cborDocument),
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as CBOR
        assertEquals("Content type should be CBOR", "application/cbor", indexRequest.getContentType().mediaType());
    }

    public void testBuildIndexRequestWithSmileContent() throws Exception {
        IndexOperation indexOperation = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create SMILE-encoded document
        byte[] smileDocument = createSmileDocument();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(smileDocument),
            null,
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as SMILE
        assertEquals("Content type should be SMILE", "application/smile", indexRequest.getContentType().mediaType());
    }

    public void testBuildIndexRequestWithCborContent() throws Exception {
        IndexOperation indexOperation = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create CBOR-encoded document
        byte[] cborDocument = createCborDocument();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(cborDocument),
            null,
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as CBOR
        assertEquals("Content type should be CBOR", "application/cbor", indexRequest.getContentType().mediaType());
    }

    public void testUpdateRequestWithCborUpsert() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("test-index", "test-id");
        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        // Create CBOR-encoded upsert document
        byte[] cborUpsert = createCborDocument();

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setUpsert(ByteString.copyFrom(cborUpsert)).build())
            .build();

        UpdateOperation updateOperation = UpdateOperation.newBuilder().build();

        UpdateRequest result = BulkRequestParserProtoUtils.fromProto(
            updateRequest,
            UnsafeByteOperations.unsafeWrap(document),
            bulkRequestBody,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        assertNotNull("Result should not be null", result);
        assertNotNull("Upsert should be set", result.upsertRequest());
    }

    public void testBuildCreateRequestWithEmptyDocument() {
        WriteOperation writeOperation = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] emptyDocument = new byte[0];

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(emptyDocument),
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertNotNull("Source should be set", indexRequest.source());
        // Empty document should default to JSON
        assertTrue("Content type should default to JSON", indexRequest.getContentType().mediaType().startsWith("application/json"));
    }

    public void testBuildCreateRequestWithJsonContent() throws Exception {
        WriteOperation writeOperation = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create JSON document
        byte[] jsonDocument = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(jsonDocument),
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as JSON (may include charset)
        assertTrue("Content type should be JSON", indexRequest.getContentType().mediaType().startsWith("application/json"));
    }

    public void testBuildCreateRequestWithYamlContent() throws Exception {
        WriteOperation writeOperation = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create YAML-encoded document
        byte[] yamlDocument = createYamlDocument();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            writeOperation,
            UnsafeByteOperations.unsafeWrap(yamlDocument),
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertEquals("Id should match", "test-id", indexRequest.id());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as YAML
        assertEquals("Content type should be YAML", "application/yaml", indexRequest.getContentType().mediaType());
    }

    public void testBuildIndexRequestWithJsonContent() throws Exception {
        IndexOperation indexOperation = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create JSON document
        byte[] jsonDocument = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(jsonDocument),
            null,
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as JSON (may include charset)
        assertTrue("Content type should be JSON", indexRequest.getContentType().mediaType().startsWith("application/json"));
    }

    public void testBuildIndexRequestWithYamlContent() throws Exception {
        IndexOperation indexOperation = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        // Create YAML-encoded document
        byte[] yamlDocument = createYamlDocument();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOperation,
            UnsafeByteOperations.unsafeWrap(yamlDocument),
            null,
            "default-index",
            "default-id",
            null,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("IndexRequest should not be null", indexRequest);
        assertEquals("Index should match", "test-index", indexRequest.index());
        assertNotNull("Source should be set", indexRequest.source());
        // Verify the content type was detected as YAML
        assertEquals("Content type should be YAML", "application/yaml", indexRequest.getContentType().mediaType());
    }

    /**
     * Helper method to create a SMILE-encoded document.
     */
    private byte[] createSmileDocument() throws Exception {
        org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.smileBuilder();
        builder.startObject();
        builder.field("field", "value");
        builder.endObject();
        return org.opensearch.core.common.bytes.BytesReference.toBytes(org.opensearch.core.common.bytes.BytesReference.bytes(builder));
    }

    /**
     * Helper method to create a CBOR-encoded document.
     */
    private byte[] createCborDocument() throws Exception {
        org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.cborBuilder();
        builder.startObject();
        builder.field("field", "value");
        builder.endObject();
        return org.opensearch.core.common.bytes.BytesReference.toBytes(org.opensearch.core.common.bytes.BytesReference.bytes(builder));
    }

    /**
     * Helper method to create a YAML-encoded document.
     */
    private byte[] createYamlDocument() throws Exception {
        org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.yamlBuilder();
        builder.startObject();
        builder.field("field", "value");
        builder.endObject();
        return org.opensearch.core.common.bytes.BytesReference.toBytes(org.opensearch.core.common.bytes.BytesReference.bytes(builder));
    }

    /**
     * Test detectMediaType with null or empty document
     */
    public void testDetectMediaTypeNullOrEmpty() {
        MediaType result = BulkRequestParserProtoUtils.detectMediaType((BytesReference) null);
        assertEquals("application/json", result.mediaTypeWithoutParameters());

        result = BulkRequestParserProtoUtils.detectMediaType(new BytesArray(new byte[0]));
        assertEquals("application/json", result.mediaTypeWithoutParameters());
    }

    /**
     * Test detectMediaType with unrecognizable format
     */
    public void testDetectMediaTypeUnrecognizable() {
        byte[] invalidBytes = new byte[] { (byte) 0xFF, (byte) 0xFE, (byte) 0xFD, (byte) 0xFC };
        MediaType result = BulkRequestParserProtoUtils.detectMediaType(new BytesArray(invalidBytes));
        assertEquals("application/json", result.mediaTypeWithoutParameters());
    }

    /**
     * Test buildUpdateRequest with upsert request and pipeline
     */
    public void testBuildUpdateRequestWithUpsertAndPipeline() {
        UpdateOperation updateOperation = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        byte[] upsertDoc = "{\"upsert_field\":\"upsert_value\"}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOperation).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setUpsert(ByteString.copyFrom(upsertDoc)).build())
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOperation,
            UnsafeByteOperations.unsafeWrap(document),
            bulkRequestBody,
            "default-index",
            "default-id",
            null,
            null,
            0,
            "test-pipeline", // pipeline
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("UpdateRequest should not be null", updateRequest);
        assertNotNull("Upsert request should be set", updateRequest.upsertRequest());
        assertEquals("Pipeline should be set on upsert request", "test-pipeline", updateRequest.upsertRequest().getPipeline());
    }

    /**
     * Test fromProto with empty document bytes (ByteString.EMPTY)
     */
    public void testFromProtoWithEmptyDocumentBytes() {
        UpdateRequest updateRequest = new UpdateRequest("test-index", "test-id");

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setUpdateAction(
                org.opensearch.protobufs.UpdateAction.newBuilder()
                    .setScript(
                        org.opensearch.protobufs.Script.newBuilder()
                            .setInline(
                                org.opensearch.protobufs.InlineScript.newBuilder()
                                    .setSource("ctx._source.counter += 1")
                                    .setLang(
                                        org.opensearch.protobufs.ScriptLanguage.newBuilder()
                                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        // Test with ByteString.EMPTY (no doc field)
        UpdateRequest result = BulkRequestParserProtoUtils.fromProto(
            updateRequest,
            ByteString.EMPTY,
            bulkRequestBody,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM
        );

        assertNotNull("Result should not be null", result);
        assertNotNull("Script should be set", result.script());
        assertNull("Doc should not be set when ByteString is empty", result.doc());
    }

    /**
     * Test fromProto with null document bytes
     */
    public void testFromProtoWithNullDocumentBytes() {
        UpdateRequest updateRequest = new UpdateRequest("test-index", "test-id");

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setUpdateAction(
                org.opensearch.protobufs.UpdateAction.newBuilder()
                    .setScript(
                        org.opensearch.protobufs.Script.newBuilder()
                            .setInline(
                                org.opensearch.protobufs.InlineScript.newBuilder()
                                    .setSource("ctx._source.counter += 1")
                                    .setLang(
                                        org.opensearch.protobufs.ScriptLanguage.newBuilder()
                                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        // Test with null documentBytes
        UpdateRequest result = BulkRequestParserProtoUtils.fromProto(
            updateRequest,
            null,
            bulkRequestBody,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM
        );

        assertNotNull("Result should not be null", result);
        assertNotNull("Script should be set", result.script());
        assertNull("Doc should not be set when documentBytes is null", result.doc());
    }

    /**
     * Test getDocWriteRequests with update operation using UpdateAction.doc field
     */
    public void testGetDocWriteRequestsWithUpdateActionDoc() {
        UpdateOperation updateOp = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody updateBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOp).build())
            .setUpdateAction(org.opensearch.protobufs.UpdateAction.newBuilder().setDoc(UnsafeByteOperations.unsafeWrap(document)).build())
            .build();

        BulkRequest request = BulkRequest.newBuilder().addBulkRequestBody(updateBody).build();

        DocWriteRequest<?>[] requests = BulkRequestParserProtoUtils.getDocWriteRequests(request, "default-index", null, null, null, false);

        assertNotNull("Requests should not be null", requests);
        assertEquals("Should have 1 request", 1, requests.length);
        assertTrue("Request should be an UpdateRequest", requests[0] instanceof UpdateRequest);

        UpdateRequest updateRequest = (UpdateRequest) requests[0];
        assertNotNull("Doc should be set from UpdateAction.doc", updateRequest.doc());
    }

    /**
     * Test valueOrDefault for String with null value and non-null globalDefault
     */
    public void testValueOrDefaultStringWithNullValue() {
        UpdateOperation updateOp = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

        BulkRequestBody updateBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOp).build())
            .setObject(UnsafeByteOperations.unsafeWrap(document))
            .build();

        BulkRequest request = BulkRequest.newBuilder()
            .addBulkRequestBody(updateBody)
            .setRouting("global-routing")
            .setPipeline("global-pipeline")
            .build();

        DocWriteRequest<?>[] requests = BulkRequestParserProtoUtils.getDocWriteRequests(
            request,
            "default-index",
            null, // defaultRouting is null, should use global routing
            null,
            null, // defaultPipeline is null, should use global pipeline
            false
        );

        assertNotNull("Requests should not be null", requests);
        assertEquals("Should have 1 request", 1, requests.length);

        UpdateRequest updateRequest = (UpdateRequest) requests[0];
        assertEquals("Routing should use global value", "global-routing", updateRequest.routing());
    }

    /**
     * Test valueOrDefault for Boolean with null value and non-null globalDefault
     */
    public void testValueOrDefaultBooleanWithNullValue() {
        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        BulkRequestBody indexBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setIndex(indexOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        BulkRequest request = BulkRequest.newBuilder().addBulkRequestBody(indexBody).setRequireAlias(true).build();

        DocWriteRequest<?>[] requests = BulkRequestParserProtoUtils.getDocWriteRequests(
            request,
            "default-index",
            null,
            null,
            null,
            null // defaultRequireAlias is null, should use global requireAlias
        );

        assertNotNull("Requests should not be null", requests);
        assertEquals("Should have 1 request", 1, requests.length);

        IndexRequest indexRequest = (IndexRequest) requests[0];
        assertTrue("RequireAlias should use global value", indexRequest.isRequireAlias());
    }

    /**
     * Test ByteString to BytesReference conversion with UnsafeByteOperations.unsafeWrap()
     * This tests the zero-copy path where ByteString wraps a byte array
     */
    public void testByteStringToBytesReferenceZeroCopy() {
        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        ByteString byteString = UnsafeByteOperations.unsafeWrap(document);

        WriteOperation createOp = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            createOp,
            byteString,
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
        assertNotNull("Source should not be null", indexRequest.source());
        // Verify the content is correct
        assertEquals("Source content should match", new String(document, StandardCharsets.UTF_8), indexRequest.source().utf8ToString());
    }

    /**
     * Test ByteString to BytesReference conversion with ByteString.copyFrom()
     * This tests the copy path where ByteString creates an internal copy
     */
    public void testByteStringToBytesReferenceCopy() {
        byte[] document = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        ByteString byteString = ByteString.copyFrom(document);

        WriteOperation createOp = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            createOp,
            byteString,
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
        assertNotNull("Source should not be null", indexRequest.source());
        assertEquals("Source content should match", new String(document, StandardCharsets.UTF_8), indexRequest.source().utf8ToString());
    }

    /**
     * Test ByteString to BytesReference conversion with empty ByteString
     */
    public void testByteStringToBytesReferenceEmpty() {
        ByteString byteString = ByteString.EMPTY;

        WriteOperation createOp = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            createOp,
            byteString,
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
        assertNotNull("Source should not be null", indexRequest.source());
        assertEquals("Source should be empty", 0, indexRequest.source().length());
    }

    /**
     * Test update request with doc field using UnsafeByteOperations.unsafeWrap()
     */
    public void testUpdateRequestDocFieldZeroCopy() {
        byte[] document = "{\"field\":\"updated_value\"}".getBytes(StandardCharsets.UTF_8);
        ByteString docBytes = UnsafeByteOperations.unsafeWrap(document);

        UpdateOperation updateOp = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        org.opensearch.protobufs.UpdateAction updateAction = org.opensearch.protobufs.UpdateAction.newBuilder().setDoc(docBytes).build();

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOp).build())
            .setUpdateAction(updateAction)
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOp,
            docBytes,
            bulkRequestBody,
            "default-index",
            "default-id",
            "default-routing",
            null,
            0,
            "default-pipeline",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("UpdateRequest should not be null", updateRequest);
        assertNotNull("Doc should not be null", updateRequest.doc());
        assertEquals("Doc content should match", new String(document, StandardCharsets.UTF_8), updateRequest.doc().source().utf8ToString());
    }

    /**
     * Test update request with upsert field using UnsafeByteOperations.unsafeWrap()
     */
    public void testUpdateRequestUpsertFieldZeroCopy() {
        byte[] docBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        byte[] upsertBytes = "{\"field\":\"default_value\"}".getBytes(StandardCharsets.UTF_8);

        UpdateOperation updateOp = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        org.opensearch.protobufs.UpdateAction updateAction = org.opensearch.protobufs.UpdateAction.newBuilder()
            .setDoc(UnsafeByteOperations.unsafeWrap(docBytes))
            .setUpsert(UnsafeByteOperations.unsafeWrap(upsertBytes))
            .build();

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOp).build())
            .setUpdateAction(updateAction)
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOp,
            UnsafeByteOperations.unsafeWrap(docBytes),
            bulkRequestBody,
            "default-index",
            "default-id",
            "default-routing",
            null,
            0,
            "default-pipeline",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("UpdateRequest should not be null", updateRequest);
        assertNotNull("Upsert should not be null", updateRequest.upsertRequest());
        assertEquals(
            "Upsert content should match",
            new String(upsertBytes, StandardCharsets.UTF_8),
            updateRequest.upsertRequest().source().utf8ToString()
        );
    }

    /**
     * Test update request with both doc and upsert fields using ByteString.copyFrom()
     */
    public void testUpdateRequestDocAndUpsertCopy() {
        byte[] docBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        byte[] upsertBytes = "{\"field\":\"default_value\"}".getBytes(StandardCharsets.UTF_8);

        UpdateOperation updateOp = UpdateOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        org.opensearch.protobufs.UpdateAction updateAction = org.opensearch.protobufs.UpdateAction.newBuilder()
            .setDoc(ByteString.copyFrom(docBytes))
            .setUpsert(ByteString.copyFrom(upsertBytes))
            .build();

        BulkRequestBody bulkRequestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(OperationContainer.newBuilder().setUpdate(updateOp).build())
            .setUpdateAction(updateAction)
            .build();

        UpdateRequest updateRequest = BulkRequestParserProtoUtils.buildUpdateRequest(
            updateOp,
            ByteString.copyFrom(docBytes),
            bulkRequestBody,
            "default-index",
            "default-id",
            "default-routing",
            null,
            0,
            "default-pipeline",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            false
        );

        assertNotNull("UpdateRequest should not be null", updateRequest);
        assertNotNull("Doc should not be null", updateRequest.doc());
        assertNotNull("Upsert should not be null", updateRequest.upsertRequest());
        assertEquals("Doc content should match", new String(docBytes, StandardCharsets.UTF_8), updateRequest.doc().source().utf8ToString());
        assertEquals(
            "Upsert content should match",
            new String(upsertBytes, StandardCharsets.UTF_8),
            updateRequest.upsertRequest().source().utf8ToString()
        );
    }

    /**
     * Test index request with large document using UnsafeByteOperations.unsafeWrap()
     */
    public void testIndexRequestLargeDocumentZeroCopy() {
        // Create a large document (> 1KB)
        StringBuilder sb = new StringBuilder("{\"data\":\"");
        for (int i = 0; i < 200; i++) {
            sb.append("0123456789");
        }
        sb.append("\"}");
        byte[] document = sb.toString().getBytes(StandardCharsets.UTF_8);

        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildIndexRequest(
            indexOp,
            UnsafeByteOperations.unsafeWrap(document),
            null,
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
        assertNotNull("Source should not be null", indexRequest.source());
        assertEquals("Source length should match", document.length, indexRequest.source().length());
        assertEquals("Source content should match", new String(document, StandardCharsets.UTF_8), indexRequest.source().utf8ToString());
    }

    /**
     * Test create request with UTF-8 encoded document using UnsafeByteOperations.unsafeWrap()
     */
    public void testCreateRequestUtf8DocumentZeroCopy() {
        String jsonWithUnicode = "{\"field\":\"Hello 世界 🌍\"}";
        byte[] document = jsonWithUnicode.getBytes(StandardCharsets.UTF_8);

        WriteOperation createOp = WriteOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        IndexRequest indexRequest = BulkRequestParserProtoUtils.buildCreateRequest(
            createOp,
            UnsafeByteOperations.unsafeWrap(document),
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
        assertNotNull("Source should not be null", indexRequest.source());
        assertEquals("Source content should match UTF-8", jsonWithUnicode, indexRequest.source().utf8ToString());
    }
}
