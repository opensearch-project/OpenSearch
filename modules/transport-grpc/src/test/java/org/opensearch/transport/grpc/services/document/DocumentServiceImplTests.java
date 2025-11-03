/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services.document;

import com.google.protobuf.ByteString;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.services.DocumentServiceImpl;
import org.junit.Before;

import java.io.IOException;

import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

public class DocumentServiceImplTests extends OpenSearchTestCase {

    private DocumentServiceImpl service;

    @Mock
    private NodeClient client;

    @Mock
    private StreamObserver<org.opensearch.protobufs.BulkResponse> responseObserver;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        service = new DocumentServiceImpl(client);
    }

    public void testBulkSuccess() throws IOException {
        // Create a test request
        BulkRequest request = createTestBulkRequest();

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify that client.bulk was called with any BulkRequest and any ActionListener
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());
    }

    public void testBulkError() throws IOException {
        // Create a test request
        BulkRequest request = createTestBulkRequest();

        // Make the client throw an exception when bulk is called
        doThrow(new RuntimeException("Test exception")).when(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify that the error was sent
        verify(responseObserver).onError(any(RuntimeException.class));
    }

    private BulkRequest createTestBulkRequest() {
        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(org.opensearch.protobufs.OperationContainer.newBuilder().setIndex(indexOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        return BulkRequest.newBuilder().addBulkRequestBody(requestBody).build();
    }
}
