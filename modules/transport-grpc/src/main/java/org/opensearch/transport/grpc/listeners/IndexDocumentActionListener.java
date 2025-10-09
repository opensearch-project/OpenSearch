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
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.IndexDocumentResponse;
import org.opensearch.transport.grpc.proto.response.document.IndexDocumentResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for index document request execution completion, handling successful and failure scenarios.
 */
public class IndexDocumentActionListener implements ActionListener<IndexResponse> {
    private static final Logger logger = LogManager.getLogger(IndexDocumentActionListener.class);

    private final StreamObserver<IndexDocumentResponse> responseObserver;

    /**
     * Constructs a new IndexDocumentActionListener.
     *
     * @param responseObserver the gRPC stream observer to send the index response to
     */
    public IndexDocumentActionListener(StreamObserver<IndexDocumentResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(IndexResponse response) {
        try {
            IndexDocumentResponse protoResponse = IndexDocumentResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Failed to convert index response to protobuf: " + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.warn("IndexDocumentActionListener failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
