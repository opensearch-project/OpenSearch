/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.get.GetResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.get.GetResult;
import org.opensearch.protobufs.GetDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for GetDocumentActionListener.
 */
public class GetDocumentActionListenerTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessFound() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L, // seqNo
            2L, // primaryTerm
            3L, // version
            true, // exists
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(), // fields
            Collections.emptyMap()  // metaFields
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseSuccessNotFound() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            -2L, // UNASSIGNED_SEQ_NO
            0L,  // UNASSIGNED_PRIMARY_TERM
            -1L, // NOT_FOUND version
            false, // exists
            null, // source
            Collections.emptyMap(), // fields
            Collections.emptyMap()  // metaFields
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnFailure() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        Exception testException = new RuntimeException("Test error");

        listener.onFailure(testException);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnFailureWithSpecificExceptions() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // Test with IllegalArgumentException
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");
        listener.onFailure(illegalArgException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Reset mock
        responseObserver = mock(StreamObserver.class);
        listener = new GetDocumentActionListener(responseObserver);

        // Test with NullPointerException
        NullPointerException nullPointerException = new NullPointerException("Null pointer");
        listener.onFailure(nullPointerException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithConversionError() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // Create a mock response that will cause conversion issues
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getIndex()).thenThrow(new RuntimeException("Conversion error"));

        listener.onResponse(mockResponse);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithNullResponse() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        listener.onResponse(null);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithVersionBoundaries() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // Test with maximum version values
        GetResult maxResult = new GetResult(
            "test-index",
            "test-id",
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse maxResponse = new GetResponse(maxResult);
        listener.onResponse(maxResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Reset mock for minimum values test
        responseObserver = mock(StreamObserver.class);
        listener = new GetDocumentActionListener(responseObserver);

        // Test with minimum version values
        GetResult minResult = new GetResult(
            "test-index",
            "test-id",
            0L,
            1L,
            1L,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse minResponse = new GetResponse(minResult);
        listener.onResponse(minResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithComplexSource() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        String complexJson = "{\"user\":{\"name\":\"John Doe\",\"age\":30},\"tags\":[\"important\",\"work\"]}";
        GetResult getResult = new GetResult(
            "complex-index",
            "complex-id",
            5L,
            3L,
            10L,
            true,
            new BytesArray(complexJson),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithEmptySource() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{}"), // Empty JSON object
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithNullSource() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            null, // null source
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithSpecialCharacters() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index-with-special_chars.and.dots",
            "test:id/with\\special@characters#and$symbols%",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"field\":\"value with special chars: @#$%^&*()\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithUnicodeCharacters() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "测试索引",
            "测试文档ID",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"标题\":\"测试文档\",\"内容\":\"这是测试内容\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        listener.onResponse(getResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    public void testConstructorWithNullObserver() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new GetDocumentActionListener(null));
        assertEquals("Response observer cannot be null", exception.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithEdgeCaseVersions() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // Test with UNASSIGNED_SEQ_NO (-2) and NOT_FOUND version (-1)
        GetResult edgeResult = new GetResult(
            "test-index",
            "test-id",
            -2L, // UNASSIGNED_SEQ_NO
            0L,  // UNASSIGNED_PRIMARY_TERM
            -1L, // NOT_FOUND
            false,
            null,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse edgeResponse = new GetResponse(edgeResult);
        listener.onResponse(edgeResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseWithLargeDocument() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // Create a large document (1MB)
        StringBuilder largeContent = new StringBuilder();
        largeContent.append("{\"data\":\"");
        for (int i = 0; i < 10000; i++) {
            largeContent.append("0123456789");
        }
        largeContent.append("\"}");

        GetResult largeResult = new GetResult(
            "large-doc-index",
            "large-doc-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray(largeContent.toString()),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse largeResponse = new GetResponse(largeResult);

        listener.onResponse(largeResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleCallsToOnResponse() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        // First call should succeed
        listener.onResponse(getResponse);
        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Second call should be ignored (stream already completed)
        listener.onResponse(getResponse);
        // Verify that onNext and onCompleted are not called again
        verify(responseObserver).onNext(any(GetDocumentResponse.class)); // Still only once
        verify(responseObserver).onCompleted(); // Still only once
    }

    @SuppressWarnings("unchecked")
    public void testOnFailureAfterOnResponse() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);

        // First call onResponse
        listener.onResponse(getResponse);
        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();

        // Then call onFailure - should be ignored
        Exception testException = new RuntimeException("Test error");
        listener.onFailure(testException);

        // Verify onError is not called after stream is already completed
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testOnResponseAfterOnFailure() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // First call onFailure
        Exception testException = new RuntimeException("Test error");
        listener.onFailure(testException);
        verify(responseObserver).onError(any(StatusRuntimeException.class));

        // Then call onResponse - should be ignored
        GetResult getResult = new GetResult(
            "test-index",
            "test-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray("{\"field\":\"value\"}"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse getResponse = new GetResponse(getResult);
        listener.onResponse(getResponse);

        // Verify onNext and onCompleted are not called after error
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    public void testMultipleOnFailureCalls() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

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
    public void testOnResponseWithBinarySource() {
        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);
        GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);

        // Test with binary data in source
        byte[] binaryData = new byte[] { 0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE };
        GetResult binaryResult = new GetResult(
            "binary-index",
            "binary-id",
            1L,
            2L,
            3L,
            true,
            new BytesArray(binaryData),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        GetResponse binaryResponse = new GetResponse(binaryResult);

        listener.onResponse(binaryResponse);

        verify(responseObserver).onNext(any(GetDocumentResponse.class));
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }
}
