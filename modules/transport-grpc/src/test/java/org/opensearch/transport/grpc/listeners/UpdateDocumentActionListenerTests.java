/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.get.GetResult;
import org.opensearch.protobufs.UpdateDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for UpdateDocumentActionListener.
 */
public class UpdateDocumentActionListenerTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessUpdated() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessCreated() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 1L, UpdateResponse.Result.CREATED);

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessDeleted() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 4L, UpdateResponse.Result.DELETED);

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessNoop() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.NOOP);

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithGetResult() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        GetResult getResult = mock(GetResult.class);
        when(getResult.isExists()).thenReturn(true);
        when(getResult.getIndex()).thenReturn("test-index");
        when(getResult.getId()).thenReturn("test-id");
        when(getResult.getVersion()).thenReturn(3L);
        updateResponse.setGetResult(getResult);

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnFailure() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        Exception testException = new RuntimeException("Test error");

        listener.onFailure(testException);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnFailureWithSpecificExceptions() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        // Test with IllegalArgumentException
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");
        listener.onFailure(illegalArgException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Reset mock
        responseObserver = mock(StreamObserver.class);
        listener = new UpdateDocumentActionListener(responseObserver);

        // Test with NullPointerException
        NullPointerException nullPointerException = new NullPointerException("Null pointer");
        listener.onFailure(nullPointerException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithConversionError() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        // Create a mock response that will cause conversion issues
        UpdateResponse mockResponse = mock(UpdateResponse.class);
        when(mockResponse.getIndex()).thenThrow(new RuntimeException("Conversion error"));

        listener.onResponse(mockResponse);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithNullResponse() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        listener.onResponse(null);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithVersionBoundaries() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);

        // Test with maximum version values
        UpdateResponse maxResponse = new UpdateResponse(
            shardId,
            "test-id",
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            UpdateResponse.Result.UPDATED
        );
        listener.onResponse(maxResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Reset mock for minimum values test
        responseObserver = mock(StreamObserver.class);
        listener = new UpdateDocumentActionListener(responseObserver);

        // Test with minimum version values
        UpdateResponse minResponse = new UpdateResponse(shardId, "test-id", 1L, 0L, 1L, UpdateResponse.Result.CREATED);
        listener.onResponse(minResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithSpecialCharacters() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index-with-special_chars.and.dots", "uuid-123", 0);
        UpdateResponse updateResponse = new UpdateResponse(
            shardId,
            "test:id/with\\special@characters#and$symbols%",
            1L,
            2L,
            3L,
            UpdateResponse.Result.UPDATED
        );

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithUnicodeCharacters() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("测试索引", "测试UUID", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "测试文档ID", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        listener.onResponse(updateResponse);

        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    public void testConstructorWithNullObserver() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new UpdateDocumentActionListener(null));
        assertEquals("Response observer cannot be null", exception.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleCallsToOnResponse() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        // First call should succeed
        listener.onResponse(updateResponse);
        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Second call should be ignored (stream already completed)
        listener.onResponse(updateResponse);
        // Verify that onNext and onCompleted are not called again
        verify(responseObserver).onNext(any(UpdateDocumentResponse.class)); // Still only once
        verify(responseObserver).onCompleted(); // Still only once
    }

    @SuppressWarnings("unchecked")
    public void testOnFailureAfterOnResponse() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);

        // First call onResponse
        listener.onResponse(updateResponse);
        verify(responseObserver).onNext(any(UpdateDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Then call onFailure - should be ignored
        Exception testException = new RuntimeException("Test error");
        listener.onFailure(testException);

        // Verify onError is not called after stream is already completed
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseAfterOnFailure() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

        // First call onFailure
        Exception testException = new RuntimeException("Test error");
        listener.onFailure(testException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Then call onResponse - should be ignored
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        UpdateResponse updateResponse = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);
        listener.onResponse(updateResponse);

        // Verify onNext and onCompleted are not called after error
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testMultipleOnFailureCalls() {
        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);

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
}
