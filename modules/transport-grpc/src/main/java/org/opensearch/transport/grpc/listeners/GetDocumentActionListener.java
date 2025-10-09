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
import org.opensearch.action.get.GetResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.GetDocumentResponse;
import org.opensearch.transport.grpc.proto.response.document.GetDocumentResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for get document request execution completion, handling successful and failure scenarios.
 */
public class GetDocumentActionListener implements ActionListener<GetResponse> {
    private static final Logger logger = LogManager.getLogger(GetDocumentActionListener.class);

    private final StreamObserver<GetDocumentResponse> responseObserver;

    /**
     * Constructs a new GetDocumentActionListener.
     *
     * @param responseObserver the gRPC stream observer to send the get response to
     */
    public GetDocumentActionListener(StreamObserver<GetDocumentResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(GetResponse response) {
        try {
            GetDocumentResponse protoResponse = GetDocumentResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to convert get response to protobuf: " + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.warn("GetDocumentActionListener failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
