/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.protobufs.services.DocumentServiceGrpc;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.listeners.BulkRequestActionListener;
import org.opensearch.transport.grpc.listeners.DeleteDocumentActionListener;
import org.opensearch.transport.grpc.listeners.GetDocumentActionListener;
import org.opensearch.transport.grpc.listeners.IndexDocumentActionListener;
import org.opensearch.transport.grpc.listeners.UpdateDocumentActionListener;
import org.opensearch.transport.grpc.proto.request.document.DeleteDocumentRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.document.GetDocumentRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.document.IndexDocumentRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.document.UpdateDocumentRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.document.bulk.BulkRequestProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of the gRPC DocumentService.
 * This class handles incoming gRPC document requests, converts them to OpenSearch requests,
 * executes them using the provided client, and returns the results back to the gRPC client.
 *
 * This implementation uses direct client calls for true single document semantics,
 * providing optimal performance and proper error handling for each operation type.
 */
public class DocumentServiceImpl extends DocumentServiceGrpc.DocumentServiceImplBase {
    private static final Logger logger = LogManager.getLogger(DocumentServiceImpl.class);
    private final Client client;

    /**
     * Creates a new DocumentServiceImpl.
     *
     * @param client Client for executing actions on the local node
     */
    public DocumentServiceImpl(Client client) {
        if (client == null) {
            throw new IllegalArgumentException("Client cannot be null");
        }
        this.client = client;
    }

    /**
     * Processes a bulk request.
     *
     * @param request The bulk request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void bulk(org.opensearch.protobufs.BulkRequest request, StreamObserver<org.opensearch.protobufs.BulkResponse> responseObserver) {
        try {
            org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);
            BulkRequestActionListener listener = new BulkRequestActionListener(responseObserver);
            client.bulk(bulkRequest, listener);
        } catch (RuntimeException e) {
            logger.warn("DocumentServiceImpl bulk failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Processes an index document request using direct client.index() call.
     * This provides true single document semantics with optimal performance.
     *
     * @param request The index document request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void indexDocument(
        org.opensearch.protobufs.IndexDocumentRequest request,
        StreamObserver<org.opensearch.protobufs.IndexDocumentResponse> responseObserver
    ) {
        try {
            IndexRequest indexRequest = IndexDocumentRequestProtoUtils.fromProto(request);
            IndexDocumentActionListener listener = new IndexDocumentActionListener(responseObserver);
            client.index(indexRequest, listener);
        } catch (RuntimeException e) {
            logger.warn("DocumentServiceImpl indexDocument failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Processes an update document request using direct client.update() call.
     * This provides true single document update semantics with proper retry handling.
     *
     * @param request The update document request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void updateDocument(
        org.opensearch.protobufs.UpdateDocumentRequest request,
        StreamObserver<org.opensearch.protobufs.UpdateDocumentResponse> responseObserver
    ) {
        try {
            UpdateRequest updateRequest = UpdateDocumentRequestProtoUtils.fromProto(request);
            UpdateDocumentActionListener listener = new UpdateDocumentActionListener(responseObserver);
            client.update(updateRequest, listener);
        } catch (RuntimeException e) {
            logger.warn("DocumentServiceImpl updateDocument failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Processes a delete document request using direct client.delete() call.
     * This provides true single document delete semantics.
     *
     * @param request The delete document request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void deleteDocument(
        org.opensearch.protobufs.DeleteDocumentRequest request,
        StreamObserver<org.opensearch.protobufs.DeleteDocumentResponse> responseObserver
    ) {
        try {
            DeleteRequest deleteRequest = DeleteDocumentRequestProtoUtils.fromProto(request);
            DeleteDocumentActionListener listener = new DeleteDocumentActionListener(responseObserver);
            client.delete(deleteRequest, listener);
        } catch (RuntimeException e) {
            logger.warn("DocumentServiceImpl deleteDocument failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Processes a get document request using direct client.get() call.
     * This provides true single document retrieval semantics.
     *
     * @param request The get document request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void getDocument(
        org.opensearch.protobufs.GetDocumentRequest request,
        StreamObserver<org.opensearch.protobufs.GetDocumentResponse> responseObserver
    ) {
        try {
            GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(request);
            GetDocumentActionListener listener = new GetDocumentActionListener(responseObserver);
            client.get(getRequest, listener);
        } catch (RuntimeException e) {
            logger.warn("DocumentServiceImpl getDocument failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }
}
