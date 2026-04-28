/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services.document;

import com.google.protobuf.ByteString;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.services.DocumentServiceImpl;
import org.junit.Before;

import java.io.IOException;

import io.grpc.stub.StreamObserver;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DocumentServiceImplTests extends OpenSearchTestCase {

    private DocumentServiceImpl service;

    @Mock
    private NodeClient client;

    @Mock
    private CircuitBreakerService circuitBreakerService;

    @Mock
    private CircuitBreaker circuitBreaker;

    @Mock
    private StreamObserver<org.opensearch.protobufs.BulkResponse> responseObserver;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        when(circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS)).thenReturn(circuitBreaker);
        service = new DocumentServiceImpl(client, circuitBreakerService);
    }

    public void testBulkSuccess() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify that client.bulk was called with any BulkRequest and any ActionListener
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());
    }

    public void testBulkError() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Make the client throw an exception when bulk is called
        doThrow(new RuntimeException("Test exception")).when(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify that the error was sent
        verify(responseObserver).onError(any(RuntimeException.class));
    }

    public void testCircuitBreakerCheckedBeforeProcessing() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify circuit breaker was checked with the request size
        verify(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), eq("<grpc_request>"));

        // Verify client.bulk was called
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());
    }

    public void testCircuitBreakerTripsAndRejectsRequest() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Make circuit breaker throw exception
        CircuitBreakingException circuitBreakerException = new CircuitBreakingException(
            "Data too large",
            100L,
            50 * 1024 * 1024L,
            CircuitBreaker.Durability.TRANSIENT
        );
        doThrow(circuitBreakerException).when(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify circuit breaker was checked
        verify(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), eq("<grpc_request>"));

        // Verify client.bulk was NOT called (request was rejected before processing)
        verify(client, never()).bulk(any(org.opensearch.action.bulk.BulkRequest.class), any());

        // Verify error was sent to client
        verify(responseObserver).onError(any());
    }

    public void testCircuitBreakerBytesReleasedOnSuccess() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Capture the ActionListener to simulate success
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<BulkResponse>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify client.bulk was called and capture the listener
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), listenerCaptor.capture());

        // Simulate successful response - create a real BulkResponse
        BulkItemResponse[] responses = new BulkItemResponse[1];
        Index index = new Index("test-index", "_na_");
        ShardId shardId = new ShardId(index, 1);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 1, 1, true);
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo();
        indexResponse.setShardInfo(shardInfo);
        responses[0] = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, indexResponse);
        BulkResponse bulkResponse = new BulkResponse(responses, 100);
        listenerCaptor.getValue().onResponse(bulkResponse);

        // Verify the wrapped observer was called (through CircuitBreakerStreamObserver)
        verify(responseObserver).onNext(any(org.opensearch.protobufs.BulkResponse.class));
        verify(responseObserver).onCompleted();

        // Verify bytes were released after success (via CircuitBreakerStreamObserver wrapper)
        verify(circuitBreaker).addWithoutBreaking(anyLong());
    }

    public void testCircuitBreakerBytesReleasedOnFailure() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Capture the ActionListener to simulate failure
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<BulkResponse>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify client.bulk was called and capture the listener
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), listenerCaptor.capture());

        // Simulate failure
        Exception testException = new RuntimeException("Bulk operation failed");
        listenerCaptor.getValue().onFailure(testException);

        // Verify the wrapped observer was called (through CircuitBreakerStreamObserver)
        verify(responseObserver).onError(any());

        // Verify bytes were released after failure (via CircuitBreakerStreamObserver wrapper)
        verify(circuitBreaker).addWithoutBreaking(anyLong());
    }

    public void testCircuitBreakerBytesReleasedExactlyOnce() throws IOException {
        // Create a test request
        org.opensearch.protobufs.BulkRequest request = createTestBulkRequest();

        // Capture the ActionListener
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<BulkResponse>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        // Call the bulk method
        service.bulk(request, responseObserver);

        // Verify circuit breaker was checked
        verify(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), eq("<grpc_request>"));

        // Verify client.bulk was called and capture the listener
        verify(client).bulk(any(org.opensearch.action.bulk.BulkRequest.class), listenerCaptor.capture());

        // Simulate successful response - create a real BulkResponse
        BulkItemResponse[] responses = new BulkItemResponse[1];
        Index index = new Index("test-index", "_na_");
        ShardId shardId = new ShardId(index, 1);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1, 1, 1, true);
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo();
        indexResponse.setShardInfo(shardInfo);
        responses[0] = new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, indexResponse);
        BulkResponse bulkResponse = new BulkResponse(responses, 100);
        listenerCaptor.getValue().onResponse(bulkResponse);

        // Verify the wrapped observer was called (through CircuitBreakerStreamObserver)
        verify(responseObserver).onNext(any(org.opensearch.protobufs.BulkResponse.class));
        verify(responseObserver).onCompleted();

        // Verify bytes were released exactly once (via CircuitBreakerStreamObserver wrapper, no double-release)
        verify(circuitBreaker, times(1)).addWithoutBreaking(anyLong());
    }

    private org.opensearch.protobufs.BulkRequest createTestBulkRequest() {
        IndexOperation indexOp = IndexOperation.newBuilder().setXIndex("test-index").setXId("test-id").build();

        BulkRequestBody requestBody = BulkRequestBody.newBuilder()
            .setOperationContainer(org.opensearch.protobufs.OperationContainer.newBuilder().setIndex(indexOp).build())
            .setObject(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        return org.opensearch.protobufs.BulkRequest.newBuilder().addBulkRequestBody(requestBody).build();
    }
}
