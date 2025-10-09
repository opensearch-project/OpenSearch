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
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.UpdateDocumentResponse;
import org.opensearch.transport.grpc.proto.response.document.UpdateDocumentResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for update document request execution completion, handling successful and failure scenarios.
 */
public class UpdateDocumentActionListener implements ActionListener<UpdateResponse> {
    private static final Logger logger = LogManager.getLogger(UpdateDocumentActionListener.class);

    private final StreamObserver<UpdateDocumentResponse> responseObserver;

    /**
     * Constructs a new UpdateDocumentActionListener.
     *
     * @param responseObserver the gRPC stream observer to send the update response to
     */
    public UpdateDocumentActionListener(StreamObserver<UpdateDocumentResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(UpdateResponse response) {
        try {
            UpdateDocumentResponse protoResponse = UpdateDocumentResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to convert update response to protobuf: " + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.warn("UpdateDocumentActionListener failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
