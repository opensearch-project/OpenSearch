/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.DeleteDocumentResponse;
import org.opensearch.transport.grpc.proto.response.document.DeleteDocumentResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for delete document request execution completion, handling successful and failure scenarios.
 */
public class DeleteDocumentActionListener implements ActionListener<DeleteResponse> {
    private static final Logger logger = LogManager.getLogger(DeleteDocumentActionListener.class);

    private final StreamObserver<DeleteDocumentResponse> responseObserver;

    /**
     * Constructs a new DeleteDocumentActionListener.
     *
     * @param responseObserver the gRPC stream observer to send the delete response to
     */
    public DeleteDocumentActionListener(StreamObserver<DeleteDocumentResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(DeleteResponse response) {
        try {
            DeleteDocumentResponse protoResponse = DeleteDocumentResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to convert delete response to protobuf: " + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.warn("DeleteDocumentActionListener failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
