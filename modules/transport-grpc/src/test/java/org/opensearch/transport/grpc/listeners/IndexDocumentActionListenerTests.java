/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.IndexDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for IndexDocumentActionListener.
 */
public class IndexDocumentActionListenerTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccess() {
        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);
        IndexDocumentActionListener listener = new IndexDocumentActionListener(responseObserver);

        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        IndexResponse indexResponse = new IndexResponse(shardId, "test-id", 1L, 2L, 3L, true);

        listener.onResponse(indexResponse);

        verify(responseObserver).onNext(any(IndexDocumentResponse.class));
        verify(responseObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnFailure() {
        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);
        IndexDocumentActionListener listener = new IndexDocumentActionListener(responseObserver);

        Exception testException = new RuntimeException("Test error");

        listener.onFailure(testException);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithConversionError() {
        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);
        IndexDocumentActionListener listener = new IndexDocumentActionListener(responseObserver);

        // Create a mock response that will cause conversion issues
        IndexResponse mockResponse = mock(IndexResponse.class);
        when(mockResponse.getIndex()).thenThrow(new RuntimeException("Conversion error"));

        listener.onResponse(mockResponse);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }
}
