/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.BulkResponse;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.services.DocumentServiceGrpc;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.ManagedChannel;

/**
 * Integration tests for the DocumentService gRPC service.
 */
public class DocumentServiceIT extends GrpcTransportBaseIT {

    /**
     * Tests the bulk operation via gRPC.
     */
    public void testDocumentServiceBulk() throws Exception {
        // Create a test index
        String indexName = "test-bulk-index";
        createTestIndex(indexName);

        // Create a gRPC client
        try (NettyGrpcClient client = createGrpcClient()) {
            // Create a DocumentService stub
            ManagedChannel channel = client.getChannel();
            DocumentServiceGrpc.DocumentServiceBlockingStub documentStub = DocumentServiceGrpc.newBlockingStub(channel);

            // Create a bulk request with an index operation
            IndexOperation indexOp = IndexOperation.newBuilder().setXIndex(indexName).setXId("1").build();

            BulkRequestBody requestBody = BulkRequestBody.newBuilder()
                .setOperationContainer(org.opensearch.protobufs.OperationContainer.newBuilder().setIndex(indexOp).build())
                .setObject(com.google.protobuf.ByteString.copyFromUtf8(DEFAULT_DOCUMENT_SOURCE))
                .build();

            BulkRequest bulkRequest = BulkRequest.newBuilder().addBulkRequestBody(requestBody).build();

            // Execute the bulk request
            BulkResponse bulkResponse = documentStub.bulk(bulkRequest);

            // Verify the response
            assertNotNull("Bulk response should not be null", bulkResponse);
            // Verify the document is searchable
            waitForSearchableDoc(indexName, "1");
        }
    }
}
