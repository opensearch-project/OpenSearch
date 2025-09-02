/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import com.google.protobuf.ByteString;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.CreateOperation;
import org.opensearch.protobufs.DeleteOperation;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.UpdateOperation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.proto.request.document.bulk.BulkRequestProtoUtils;
import org.junit.Before;

import java.io.IOException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BulkRequestProtoUtilsTests extends OpenSearchTestCase {

    @Mock
    private NodeClient client;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testPrepareRequestWithIndexOperation() throws IOException {
        // Create a Protocol Buffer BulkRequest with an index operation
        BulkRequest request = createBulkRequestWithIndexOperation();

        // Convert to OpenSearch BulkRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the converted request
        assertEquals("Should have 1 request", 1, bulkRequest.numberOfActions());
        // The actual refresh policy is NONE, not IMMEDIATE
        assertEquals("Should have the correct refresh policy", WriteRequest.RefreshPolicy.NONE, bulkRequest.getRefreshPolicy());

        // Verify the index request
        DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(0);
        assertEquals("Should be an INDEX operation", DocWriteRequest.OpType.INDEX, docWriteRequest.opType());
        assertEquals("Should have the correct index", "test-index", docWriteRequest.index());
        assertEquals("Should have the correct id", "test-id", docWriteRequest.id());
        assertEquals("Should have the correct pipeline", "test-pipeline", ((IndexRequest) docWriteRequest).getPipeline());

    }

    public void testPrepareRequestWithCreateOperation() throws IOException {
        // Create a Protocol Buffer BulkRequest with a create operation
        BulkRequest request = createBulkRequestWithCreateOperation();

        // Convert to OpenSearch BulkRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the converted request
        assertEquals("Should have 1 request", 1, bulkRequest.numberOfActions());

        // Verify the create request
        DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(0);
        assertEquals("Should be a CREATE operation", DocWriteRequest.OpType.CREATE, docWriteRequest.opType());
        assertEquals("Should have the correct index", "test-index", docWriteRequest.index());
        assertEquals("Should have the correct id", "test-id", docWriteRequest.id());
    }

    public void testPrepareRequestWithDeleteOperation() throws IOException {
        // Create a Protocol Buffer BulkRequest with a delete operation
        BulkRequest request = createBulkRequestWithDeleteOperation();

        // Convert to OpenSearch BulkRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the converted request
        assertEquals("Should have 1 request", 1, bulkRequest.numberOfActions());

        // Verify the delete request
        DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(0);
        assertEquals("Should have the correct index", "test-index", docWriteRequest.index());
        assertEquals("Should have the correct id", "test-id", docWriteRequest.id());
    }

    public void testPrepareRequestWithUpdateOperation() throws IOException {
        // Create a Protocol Buffer BulkRequest with an update operation
        BulkRequest request = createBulkRequestWithUpdateOperation();

        // Convert to OpenSearch BulkRequest
        org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

        // Verify the converted request
        assertEquals("Should have 1 request", 1, bulkRequest.numberOfActions());

        // Verify the update request
        DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(0);
        assertEquals("Should have the correct index", "test-index", docWriteRequest.index());
        assertEquals("Should have the correct id", "test-id", docWriteRequest.id());
    }

    // Helper methods to create test requests

    private BulkRequest createBulkRequestWithIndexOperation() {
        IndexOperation indexOp = IndexOperation.newBuilder().setIndex("test-index").setId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder()
            .setIndex(indexOp)
            .setDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        return BulkRequest.newBuilder()
            .setPipeline("test-pipeline")
            .setRefreshValue(1) // REFRESH_TRUE = 1
            .addRequestBody(requestBody)
            .build();
    }

    private BulkRequest createBulkRequestWithCreateOperation() {
        CreateOperation createOp = CreateOperation.newBuilder().setIndex("test-index").setId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder()
            .setCreate(createOp)
            .setDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        return BulkRequest.newBuilder().addRequestBody(requestBody).build();
    }

    private BulkRequest createBulkRequestWithDeleteOperation() {
        DeleteOperation deleteOp = DeleteOperation.newBuilder().setIndex("test-index").setId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder().setDelete(deleteOp).build();

        return BulkRequest.newBuilder().addRequestBody(requestBody).build();
    }

    private BulkRequest createBulkRequestWithUpdateOperation() {
        UpdateOperation updateOp = UpdateOperation.newBuilder().setIndex("test-index").setId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder()
            .setUpdate(updateOp)
            .setDoc(ByteString.copyFromUtf8("{\"field\":\"updated-value\"}"))
            .build();

        return BulkRequest.newBuilder().addRequestBody(requestBody).build();
    }
}
