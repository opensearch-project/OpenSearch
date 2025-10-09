/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.DeleteDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for DeleteDocumentActionListener.
 */
public class DeleteDocumentActionListenerTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessDeleted() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);

        listener.onResponse(deleteResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessNotFound() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 1L, false);

        listener.onResponse(deleteResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnFailure() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        Exception testException = new RuntimeException("Test error");

        listener.onFailure(testException);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnFailureWithSpecificExceptions() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        // Test with IllegalArgumentException
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");
        listener.onFailure(illegalArgException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Reset mock
        responseObserver = mock(StreamObserver.class);
        listener = new DeleteDocumentActionListener(responseObserver);

        // Test with NullPointerException
        NullPointerException nullPointerException = new NullPointerException("Null pointer");
        listener.onFailure(nullPointerException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithConversionError() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        // Create a mock response that will cause conversion issues
        DeleteResponse mockResponse = mock(DeleteResponse.class);
        when(mockResponse.getIndex()).thenThrow(new RuntimeException("Conversion error"));

        listener.onResponse(mockResponse);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithNullResponse() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        listener.onResponse(null);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithVersionBoundaries() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test with maximum version values
        DeleteResponse maxResponse = new DeleteResponse(shardId, "test-id", Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, true);
        listener.onResponse(maxResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Reset mock for minimum values test
        responseObserver = mock(StreamObserver.class);
        listener = new DeleteDocumentActionListener(responseObserver);

        // Test with minimum version values
        DeleteResponse minResponse = new DeleteResponse(shardId, "test-id", 1L, 0L, 1L, false);
        listener.onResponse(minResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithSpecialCharacters() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index-with-special_chars.and.dots", "uuid-123", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test:id/with\\special@characters#and$symbols%", 1L, 2L, 3L, true);

        listener.onResponse(deleteResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithUnicodeCharacters() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("测试索引", "测试UUID", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "测试文档ID", 1L, 2L, 3L, true);

        listener.onResponse(deleteResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    public void testConstructorWithNullObserver() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DeleteDocumentActionListener(null));
        assertEquals("Response observer cannot be null", exception.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithEdgeCaseVersions() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test with UNASSIGNED_SEQ_NO (-2) and NOT_FOUND version (-1)
        DeleteResponse edgeResponse = new DeleteResponse(shardId, "test-id", 1L, -2L, -1L, false);
        listener.onResponse(edgeResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleCallsToOnResponse() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);

        // First call should succeed
        listener.onResponse(deleteResponse);
        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Second call should be ignored (stream already completed)
        listener.onResponse(deleteResponse);
        // Verify that onNext and onCompleted are not called again
        verify(responseObserver).onNext(any(DeleteDocumentResponse.class)); // Still only once
        verify(responseObserver).onCompleted(); // Still only once
    }

    @SuppressWarnings("unchecked")
    public void testOnFailureAfterOnResponse() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);

        // First call onResponse
        listener.onResponse(deleteResponse);
        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Then call onFailure - should be ignored
        Exception testException = new RuntimeException("Test error");
        listener.onFailure(testException);

        // Verify onError is not called after stream is already completed
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseAfterOnFailure() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        // First call onFailure
        Exception testException = new RuntimeException("Test error");
        listener.onFailure(testException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Then call onResponse - should be ignored
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        DeleteResponse deleteResponse = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);
        listener.onResponse(deleteResponse);

        // Verify onNext and onCompleted are not called after error
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testMultipleOnFailureCalls() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        Exception testException1 = new RuntimeException("First error");
        Exception testException2 = new RuntimeException("Second error");

        // First call should succeed
        listener.onFailure(testException1);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Second call should be ignored
        listener.onFailure(testException2);
        // Verify onError is called only once
        verify(responseObserver).onError(any(StatusRuntimeException.class)); // Still only once
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithVersionConflictScenario() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Simulate a version conflict scenario where document was already deleted
        DeleteResponse conflictResponse = new DeleteResponse(shardId, "test-id", 3L, 5L, 2L, false);
        listener.onResponse(conflictResponse);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithMultipleShardScenarios() {
        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);
        DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);

        // Test with different shard IDs from the same index
        ShardId shard0 = new ShardId("multi-shard-index", "uuid-123", 0);
        DeleteResponse response0 = new DeleteResponse(shard0, "doc-0", 1L, 1L, 1L, true);

        listener.onResponse(response0);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());

        // Reset for next test
        responseObserver = mock(StreamObserver.class);
        listener = new DeleteDocumentActionListener(responseObserver);

        ShardId shard1 = new ShardId("multi-shard-index", "uuid-123", 1);
        DeleteResponse response1 = new DeleteResponse(shard1, "doc-1", 1L, 2L, 1L, false);

        listener.onResponse(response1);

        verify(responseObserver).onNext(any(DeleteDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }
}
