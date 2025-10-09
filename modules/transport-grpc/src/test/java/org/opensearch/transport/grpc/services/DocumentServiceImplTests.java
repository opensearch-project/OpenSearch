/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import com.google.protobuf.ByteString;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.DeleteDocumentRequest;
import org.opensearch.protobufs.DeleteDocumentResponse;
import org.opensearch.protobufs.GetDocumentRequest;
import org.opensearch.protobufs.GetDocumentResponse;
import org.opensearch.protobufs.IndexDocumentRequest;
import org.opensearch.protobufs.IndexDocumentResponse;
import org.opensearch.protobufs.UpdateDocumentRequest;
import org.opensearch.protobufs.UpdateDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import io.grpc.stub.StreamObserver;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for DocumentServiceImpl.
 */
public class DocumentServiceImplTests extends OpenSearchTestCase {

    private Client mockClient;
    private DocumentServiceImpl documentService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClient = mock(Client.class);
        documentService = new DocumentServiceImpl(mockClient);
    }

    public void testConstructorWithNullClient() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DocumentServiceImpl(null));
        assertEquals("Client cannot be null", exception.getMessage());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testIndexDocument() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).index(requestCaptor.capture(), listenerCaptor.capture());

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
        assertNotNull(capturedRequest.source());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testUpdateDocument() {
        UpdateDocumentRequest request = UpdateDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.updateDocument(request, responseObserver);

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).update(requestCaptor.capture(), listenerCaptor.capture());

        UpdateRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testDeleteDocument() {
        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.deleteDocument(request, responseObserver);

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).delete(requestCaptor.capture(), listenerCaptor.capture());

        DeleteRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testGetDocument() {
        GetDocumentRequest request = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.getDocument(request, responseObserver);

        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).get(requestCaptor.capture(), listenerCaptor.capture());

        GetRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
    }

    @SuppressWarnings("unchecked")
    public void testIndexDocumentWithException() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder().setIndex("test-index").build(); // Missing required fields

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        // Should call onError due to validation failure
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testIndexDocumentWithAllOptionalFields() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .setOpType(org.opensearch.protobufs.OpType.OP_TYPE_CREATE)
            .setRouting("test-routing")
            .setRefresh(org.opensearch.protobufs.Refresh.REFRESH_TRUE)
            .setIfSeqNo(5L)
            .setIfPrimaryTerm(2L)
            .setPipeline("test-pipeline")
            .setTimeout("30s")
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).index(requestCaptor.capture(), listenerCaptor.capture());

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
        assertEquals("test-routing", capturedRequest.routing());
        assertEquals("test-pipeline", capturedRequest.getPipeline());
        assertNotNull(capturedRequest.source());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testUpdateDocumentWithAllOptionalFields() {
        UpdateDocumentRequest request = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("test-routing")
            .setRefresh(org.opensearch.protobufs.Refresh.REFRESH_WAIT_FOR)
            .setIfSeqNo(10L)
            .setIfPrimaryTerm(3L)
            .setTimeout("45s")
            .setRetryOnConflict(5)
            .setRequestBody(
                org.opensearch.protobufs.UpdateDocumentRequestBody.newBuilder()
                    .setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"updated_value\"}"))
                    .setBytesUpsert(ByteString.copyFromUtf8("{\"field\":\"upsert_value\"}"))
                    .setDocAsUpsert(true)
                    .setDetectNoop(false)
                    .setScriptedUpsert(true)
                    .build()
            )
            .build();

        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.updateDocument(request, responseObserver);

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).update(requestCaptor.capture(), listenerCaptor.capture());

        UpdateRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
        assertEquals("test-routing", capturedRequest.routing());
        assertEquals(5, capturedRequest.retryOnConflict());
        assertTrue(capturedRequest.docAsUpsert());
        assertFalse(capturedRequest.detectNoop());
        assertTrue(capturedRequest.scriptedUpsert());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDocumentWithException() {
        UpdateDocumentRequest request = UpdateDocumentRequest.newBuilder().setIndex("test-index").build(); // Missing required fields

        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.updateDocument(request, responseObserver);

        // Should call onError due to validation failure
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testDeleteDocumentWithAllOptionalFields() {
        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("test-routing")
            .setRefresh(org.opensearch.protobufs.Refresh.REFRESH_TRUE)
            .setIfSeqNo(15L)
            .setIfPrimaryTerm(4L)
            .setTimeout("60s")
            .build();

        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.deleteDocument(request, responseObserver);

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).delete(requestCaptor.capture(), listenerCaptor.capture());

        DeleteRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
        assertEquals("test-routing", capturedRequest.routing());
        assertEquals(15L, capturedRequest.ifSeqNo());
        assertEquals(4L, capturedRequest.ifPrimaryTerm());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteDocumentWithException() {
        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder().setIndex("test-index").build(); // Missing required fields

        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.deleteDocument(request, responseObserver);

        // Should call onError due to validation failure
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testGetDocumentWithAllOptionalFields() {
        GetDocumentRequest request = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("test-routing")
            .setPreference("_local")
            .setRealtime(false)
            .setRefresh(true)
            .addXSourceIncludes("field1")
            .addXSourceIncludes("field2")
            .addXSourceExcludes("field3")
            .addStoredFields("stored1")
            .addStoredFields("stored2")
            .build();

        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.getDocument(request, responseObserver);

        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).get(requestCaptor.capture(), listenerCaptor.capture());

        GetRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
        assertEquals("test-routing", capturedRequest.routing());
        assertEquals("_local", capturedRequest.preference());
        assertFalse(capturedRequest.realtime());
        assertTrue(capturedRequest.refresh());
        assertNotNull(capturedRequest.fetchSourceContext());
        assertNotNull(capturedRequest.storedFields());
    }

    @SuppressWarnings("unchecked")
    public void testGetDocumentWithException() {
        GetDocumentRequest request = GetDocumentRequest.newBuilder().setIndex("test-index").build(); // Missing required fields

        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.getDocument(request, responseObserver);

        // Should call onError due to validation failure
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testIndexDocumentWithEmptyFields() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("")
            .setId("")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        // Should call onError due to empty index name
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateDocumentWithEmptyFields() {
        UpdateDocumentRequest request = UpdateDocumentRequest.newBuilder()
            .setIndex("")
            .setId("")
            .setRequestBody(
                org.opensearch.protobufs.UpdateDocumentRequestBody.newBuilder()
                    .setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
                    .build()
            )
            .build();

        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.updateDocument(request, responseObserver);

        // Should call onError due to empty index name
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteDocumentWithEmptyFields() {
        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder().setIndex("").setId("").build();

        StreamObserver<DeleteDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.deleteDocument(request, responseObserver);

        // Should call onError due to empty index name
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetDocumentWithEmptyFields() {
        GetDocumentRequest request = GetDocumentRequest.newBuilder().setIndex("").setId("").build();

        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.getDocument(request, responseObserver);

        // Should call onError due to empty index name
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testIndexDocumentWithSpecialCharacters() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index-with-special_chars.and.dots")
            .setId("test:id/with\\special@characters#and$symbols%")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value with special chars: @#$%^&*()\"}"))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockClient).index(requestCaptor.capture(), any());

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index-with-special_chars.and.dots", capturedRequest.index());
        assertEquals("test:id/with\\special@characters#and$symbols%", capturedRequest.id());
    }

    @SuppressWarnings("unchecked")
    public void testIndexDocumentWithUnicodeCharacters() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("测试索引")
            .setId("测试文档ID")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"标题\":\"测试文档\",\"内容\":\"这是测试内容\"}"))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockClient).index(requestCaptor.capture(), any());

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("测试索引", capturedRequest.index());
        assertEquals("测试文档ID", capturedRequest.id());
    }

    @SuppressWarnings("unchecked")
    public void testIndexDocumentWithLargeDocument() {
        // Create a large document (100KB)
        StringBuilder largeDoc = new StringBuilder();
        largeDoc.append("{\"data\":\"");
        for (int i = 0; i < 10000; i++) {
            largeDoc.append("0123456789");
        }
        largeDoc.append("\"}");

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("large-doc-index")
            .setId("large-doc-id")
            .setBytesRequestBody(ByteString.copyFromUtf8(largeDoc.toString()))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockClient).index(requestCaptor.capture(), any());

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("large-doc-index", capturedRequest.index());
        assertEquals("large-doc-id", capturedRequest.id());
        assertTrue(capturedRequest.source().length() > 100000);
    }

    @SuppressWarnings("unchecked")
    public void testIndexDocumentWithAutoGeneratedId() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            // No ID field - should auto-generate
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(request, responseObserver);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockClient).index(requestCaptor.capture(), any());

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        // ID should be null for auto-generation
        assertNull(capturedRequest.id());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testClientExceptionHandling() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        // Mock client to throw exception when index is called
        doThrow(new RuntimeException("Simulated client error")).when(mockClient).index(any(IndexRequest.class), any(ActionListener.class));

        documentService.indexDocument(request, responseObserver);

        // Should call onError due to client exception
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings("unchecked")
    public void testNullRequestHandling() {
        StreamObserver<IndexDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.indexDocument(null, responseObserver);

        // Should call onError due to null request
        verify(responseObserver).onError(any());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testNullResponseObserverHandling() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        // When responseObserver is null, the service should still process the request
        // but if any exception occurs, it will fail when trying to call onError(null)
        // For a valid request, it should succeed and call the client
        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        documentService.indexDocument(request, null);

        // Should still call client.index
        verify(mockClient).index(requestCaptor.capture(), any(ActionListener.class));

        IndexRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testUpdateDocumentWithNullRequestBody() {
        UpdateDocumentRequest request = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            // No request body - this is actually valid, it will just be a no-op update
            .build();

        StreamObserver<UpdateDocumentResponse> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);

        documentService.updateDocument(request, responseObserver);

        // Should call client.update with a valid UpdateRequest (even without body)
        verify(mockClient).update(requestCaptor.capture(), any(ActionListener.class));

        UpdateRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-index", capturedRequest.index());
        assertEquals("test-id", capturedRequest.id());
    }

    @SuppressWarnings("unchecked")
    public void testGetDocumentWithSourceFiltering() {
        GetDocumentRequest request = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceIncludes("user.*")
            .addXSourceIncludes("metadata.public.*")
            .addXSourceExcludes("user.password")
            .addXSourceExcludes("metadata.private.*")
            .build();

        StreamObserver<GetDocumentResponse> responseObserver = mock(StreamObserver.class);

        documentService.getDocument(request, responseObserver);

        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        verify(mockClient).get(requestCaptor.capture(), any());

        GetRequest capturedRequest = requestCaptor.getValue();
        assertNotNull(capturedRequest.fetchSourceContext());
        assertArrayEquals(new String[] { "user.*", "metadata.public.*" }, capturedRequest.fetchSourceContext().includes());
        assertArrayEquals(new String[] { "user.password", "metadata.private.*" }, capturedRequest.fetchSourceContext().excludes());
    }
}
