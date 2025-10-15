/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services.document;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.services.DocumentServiceImpl;
import com.google.protobuf.ByteString;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class DocumentServiceImplTests extends OpenSearchTestCase {

    private DocumentServiceImpl service;

    @Mock
    private NodeClient client;

    @Mock
    private StreamObserver<org.opensearch.protobufs.BulkResponse> responseObserver;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        service = new DocumentServiceImpl(client, true);
    }

    public void testBulkSuccess() {
        // Create a test request
        BulkRequest request = createTestBulkRequest();

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify that client.bulk was called with any BulkRequest and any ActionListener
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());
    }

    public void testBulkError() {
        // Create a test request
        BulkRequest request = createTestBulkRequest();

        // Make the client throw an exception when bulk is called
        doThrow(new RuntimeException("Test exception")).when(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify that the error was sent
        verify(responseObserver).onError(any(RuntimeException.class));
    }

    public void testErrorTracingConfigValidationFailsWhenServerSettingIsDisabledAndRequestRequiresTracing() {
        // Setup request and the service, server setting is off and request requires tracing
        BulkRequest request = createTestBulkRequest();
        DocumentServiceImpl serviceWithDisabledErrorsTracing = new DocumentServiceImpl(client, false);

        // Call bulk method
        serviceWithDisabledErrorsTracing.bulk(request, responseObserver);

        // Verify that an error was sent
        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    public void testErrorTracingConfigValidationPassesWhenServerSettingIsDisabledAndRequestSkipsTracing() {
        // Setup request and the service, server setting is off and request does not require tracing
        BulkRequest request = createTestBulkRequest().toBuilder().setGlobalParams(
            org.opensearch.protobufs.GlobalParams.newBuilder().setErrorTrace(false)
        ).build();
        DocumentServiceImpl serviceWithDisabledErrorsTracing = new DocumentServiceImpl(client, false);

        // Call bulk method
        serviceWithDisabledErrorsTracing.bulk(request, responseObserver);

        // Verify that client.bulk was called
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());
    }

    private BulkRequest createTestBulkRequest() {
        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder()
                                                     .setOperationContainer(org.opensearch.protobufs.OperationContainer.newBuilder().setIndex(indexOp).build())
                                                     .setObject(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
                                                     .build();

        return BulkRequest.newBuilder()
                          .addRequestBody(requestBody)
                          .setGlobalParams(org.opensearch.protobufs.GlobalParams.newBuilder().setErrorTrace(true))
                          .build();
    }
}
