/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.integration;

import com.google.protobuf.ByteString;
import org.opensearch.OpenSearchException;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.get.GetResult;
import org.opensearch.protobufs.DeleteDocumentRequest;
import org.opensearch.protobufs.DeleteDocumentResponse;
import org.opensearch.protobufs.GetDocumentRequest;
import org.opensearch.protobufs.GetDocumentResponse;
import org.opensearch.protobufs.IndexDocumentRequest;
import org.opensearch.protobufs.IndexDocumentResponse;
import org.opensearch.protobufs.UpdateDocumentRequest;
import org.opensearch.protobufs.UpdateDocumentRequestBody;
import org.opensearch.protobufs.UpdateDocumentResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.services.DocumentServiceImpl;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Integration tests for DocumentServiceImpl focusing on error handling and edge cases.
 */
public class DocumentServiceIntegrationTests extends OpenSearchTestCase {

    private Client mockClient;
    private DocumentServiceImpl documentService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClient = mock(Client.class);
        documentService = new DocumentServiceImpl(mockClient);
    }

    public void testIndexDocumentSuccessfulFlow() throws InterruptedException {
        // Setup mock client to simulate successful index operation
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            ShardId shardId = new ShardId("test-index", "test-uuid", 0);
            IndexResponse response = new IndexResponse(shardId, "test-id", 1L, 2L, 3L, true);
            listener.onResponse(response);
            return null;
        }).when(mockClient).index(any(IndexRequest.class), any());

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<IndexDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
            @Override
            public void onNext(IndexDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.indexDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("test-index", responseRef.get().getIndexDocumentResponseBody().getXIndex());
        assertEquals("test-id", responseRef.get().getIndexDocumentResponseBody().getXId());
    }

    public void testIndexDocumentWithOpenSearchException() throws InterruptedException {
        // Setup mock client to simulate OpenSearch exception
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            OpenSearchException exception = new OpenSearchException("Index not found");
            listener.onFailure(exception);
            return null;
        }).when(mockClient).index(any(IndexRequest.class), any());

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("non-existent-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<StatusRuntimeException> errorRef = new AtomicReference<>();

        StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
            @Override
            public void onNext(IndexDocumentResponse response) {
                fail("Should not receive response on error");
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    errorRef.set((StatusRuntimeException) t);
                }
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                fail("Should not complete on error");
            }
        };

        documentService.indexDocument(request, responseObserver);

        assertTrue("Error should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Error should be received", errorRef.get());
        assertTrue("Error message should contain exception details", errorRef.get().getMessage().contains("Index not found"));
    }

    public void testUpdateDocumentSuccessfulFlow() throws InterruptedException {
        // Setup mock client to simulate successful update operation
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            ShardId shardId = new ShardId("test-index", "test-uuid", 0);
            UpdateResponse response = new UpdateResponse(shardId, "test-id", 1L, 2L, 3L, UpdateResponse.Result.UPDATED);
            listener.onResponse(response);
            return null;
        }).when(mockClient).update(any(UpdateRequest.class), any());

        UpdateDocumentRequest request = UpdateDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRequestBody(
                UpdateDocumentRequestBody.newBuilder().setBytesDoc(ByteString.copyFromUtf8("{\"field\":\"updated_value\"}")).build()
            )
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<UpdateDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<UpdateDocumentResponse> responseObserver = new StreamObserver<UpdateDocumentResponse>() {
            @Override
            public void onNext(UpdateDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.updateDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("test-index", responseRef.get().getUpdateDocumentResponseBody().getXIndex());
        assertEquals("test-id", responseRef.get().getUpdateDocumentResponseBody().getXId());
        assertEquals("updated", responseRef.get().getUpdateDocumentResponseBody().getResult());
    }

    public void testDeleteDocumentSuccessfulFlow() throws InterruptedException {
        // Setup mock client to simulate successful delete operation
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            ShardId shardId = new ShardId("test-index", "test-uuid", 0);
            DeleteResponse response = new DeleteResponse(shardId, "test-id", 1L, 2L, 3L, true);
            listener.onResponse(response);
            return null;
        }).when(mockClient).delete(any(DeleteRequest.class), any());

        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DeleteDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<DeleteDocumentResponse> responseObserver = new StreamObserver<DeleteDocumentResponse>() {
            @Override
            public void onNext(DeleteDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.deleteDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("test-index", responseRef.get().getDeleteDocumentResponseBody().getXIndex());
        assertEquals("test-id", responseRef.get().getDeleteDocumentResponseBody().getXId());
        assertEquals("deleted", responseRef.get().getDeleteDocumentResponseBody().getResult());
    }

    public void testGetDocumentSuccessfulFlowFound() throws InterruptedException {
        // Setup mock client to simulate successful get operation (document found)
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
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
            GetResponse response = new GetResponse(getResult);
            listener.onResponse(response);
            return null;
        }).when(mockClient).get(any(GetRequest.class), any());

        GetDocumentRequest request = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<GetDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<GetDocumentResponse> responseObserver = new StreamObserver<GetDocumentResponse>() {
            @Override
            public void onNext(GetDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.getDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("test-index", responseRef.get().getGetDocumentResponseBody().getXIndex());
        assertEquals("test-id", responseRef.get().getGetDocumentResponseBody().getXId());
        assertTrue("Document should be found", responseRef.get().getGetDocumentResponseBody().getFound());
    }

    public void testGetDocumentSuccessfulFlowNotFound() throws InterruptedException {
        // Setup mock client to simulate successful get operation (document not found)
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResult getResult = new GetResult(
                "test-index",
                "test-id",
                -2L,
                0L,
                -1L, // UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, NOT_FOUND
                false,
                null,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            GetResponse response = new GetResponse(getResult);
            listener.onResponse(response);
            return null;
        }).when(mockClient).get(any(GetRequest.class), any());

        GetDocumentRequest request = GetDocumentRequest.newBuilder().setIndex("test-index").setId("non-existent-id").build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<GetDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<GetDocumentResponse> responseObserver = new StreamObserver<GetDocumentResponse>() {
            @Override
            public void onNext(GetDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.getDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("test-index", responseRef.get().getGetDocumentResponseBody().getXIndex());
        assertEquals("test-id", responseRef.get().getGetDocumentResponseBody().getXId());
        assertFalse("Document should not be found", responseRef.get().getGetDocumentResponseBody().getFound());
        assertEquals("-2", responseRef.get().getGetDocumentResponseBody().getXSeqNo());
        assertEquals("-1", responseRef.get().getGetDocumentResponseBody().getXVersion());
    }

    public void testConcurrentOperations() throws InterruptedException {
        // Setup mock client to simulate successful operations with delay
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            // Simulate some processing time
            new Thread(() -> {
                try {
                    Thread.sleep(100);
                    ShardId shardId = new ShardId("test-index", "test-uuid", 0);
                    IndexResponse response = new IndexResponse(shardId, "test-id", 1L, 2L, 3L, true);
                    listener.onResponse(response);
                } catch (InterruptedException e) {
                    listener.onFailure(e);
                }
            }).start();
            return null;
        }).when(mockClient).index(any(IndexRequest.class), any());

        int concurrentRequests = 10;
        CountDownLatch latch = new CountDownLatch(concurrentRequests);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        for (int i = 0; i < concurrentRequests; i++) {
            final int requestId = i;
            IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndex("test-index")
                .setId("test-id-" + requestId)
                .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value" + requestId + "\"}"))
                .build();

            StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
                @Override
                public void onNext(IndexDocumentResponse response) {
                    // Verify response
                    assertEquals("test-index", response.getIndexDocumentResponseBody().getXIndex());
                }

                @Override
                public void onError(Throwable t) {
                    errorRef.set(new Exception("Request " + requestId + " failed", t));
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };

            documentService.indexDocument(request, responseObserver);
        }

        assertTrue("All requests should complete within timeout", latch.await(10, TimeUnit.SECONDS));
        assertNull("No errors should occur during concurrent operations", errorRef.get());
    }

    public void testValidationErrorHandling() throws InterruptedException {
        // Test with invalid request (missing index)
        IndexDocumentRequest invalidRequest = IndexDocumentRequest.newBuilder()
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<StatusRuntimeException> errorRef = new AtomicReference<>();

        StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
            @Override
            public void onNext(IndexDocumentResponse response) {
                fail("Should not receive response for invalid request");
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    errorRef.set((StatusRuntimeException) t);
                }
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                fail("Should not complete for invalid request");
            }
        };

        documentService.indexDocument(invalidRequest, responseObserver);

        assertTrue("Error should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Validation error should be received", errorRef.get());
        assertEquals("Should be INVALID_ARGUMENT status", Status.Code.INVALID_ARGUMENT, errorRef.get().getStatus().getCode());
    }

    public void testLargeDocumentHandling() throws InterruptedException {
        // Setup mock client to handle large documents
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            IndexRequest request = invocation.getArgument(0);

            // Verify large document was received correctly
            assertTrue("Large document should be properly handled", request.source().length() > 100000);

            ShardId shardId = new ShardId("large-doc-index", "test-uuid", 0);
            IndexResponse response = new IndexResponse(shardId, "large-doc-id", 1L, 2L, 3L, true);
            listener.onResponse(response);
            return null;
        }).when(mockClient).index(any(IndexRequest.class), any());

        // Create a large document (1MB)
        StringBuilder largeDoc = new StringBuilder();
        largeDoc.append("{\"data\":\"");
        for (int i = 0; i < 100000; i++) {
            largeDoc.append("0123456789");
        }
        largeDoc.append("\"}");

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("large-doc-index")
            .setId("large-doc-id")
            .setBytesRequestBody(ByteString.copyFromUtf8(largeDoc.toString()))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<IndexDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
            @Override
            public void onNext(IndexDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.indexDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(10, TimeUnit.SECONDS));
        assertNull("No error should occur for large document", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("large-doc-index", responseRef.get().getIndexDocumentResponseBody().getXIndex());
        assertEquals("large-doc-id", responseRef.get().getIndexDocumentResponseBody().getXId());
    }

    public void testUnicodeDocumentHandling() throws InterruptedException {
        // Setup mock client to handle Unicode documents
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            ShardId shardId = new ShardId("unicode-index", "test-uuid", 0);
            IndexResponse response = new IndexResponse(shardId, "unicode-id", 1L, 2L, 3L, true);
            listener.onResponse(response);
            return null;
        }).when(mockClient).index(any(IndexRequest.class), any());

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("测试索引")
            .setId("测试文档ID")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"标题\":\"测试文档\",\"内容\":\"这是测试内容\",\"emoji\":\"🚀📚\"}"))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<IndexDocumentResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
            @Override
            public void onNext(IndexDocumentResponse response) {
                responseRef.set(response);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        documentService.indexDocument(request, responseObserver);

        assertTrue("Response should be received within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur for Unicode document", errorRef.get());
        assertNotNull("Response should be received", responseRef.get());
        assertEquals("unicode-index", responseRef.get().getIndexDocumentResponseBody().getXIndex());
        assertEquals("unicode-id", responseRef.get().getIndexDocumentResponseBody().getXId());
    }

    public void testTimeoutHandling() throws InterruptedException {
        // Setup mock client to simulate timeout
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            // Simulate timeout by not calling listener methods
            new Thread(() -> {
                try {
                    Thread.sleep(10000); // Long delay to simulate timeout
                } catch (InterruptedException e) {
                    listener.onFailure(new RuntimeException("Timeout"));
                }
            }).start();
            return null;
        }).when(mockClient).index(any(IndexRequest.class), any());

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setBytesRequestBody(ByteString.copyFromUtf8("{\"field\":\"value\"}"))
            .setTimeout("1s") // Short timeout
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<IndexDocumentResponse> responseObserver = new StreamObserver<IndexDocumentResponse>() {
            @Override
            public void onNext(IndexDocumentResponse response) {
                fail("Should not receive response on timeout");
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                fail("Should not complete on timeout");
            }
        };

        documentService.indexDocument(request, responseObserver);

        // Wait a bit longer than the request timeout to ensure it's handled
        assertTrue("Error should be received within timeout", latch.await(15, TimeUnit.SECONDS));
        assertNotNull("Timeout error should be received", errorRef.get());
    }
}
