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
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.DeletePITResponse;
import org.opensearch.transport.grpc.proto.response.search.DeletePitResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for PIT deletion request completion.
 */
public class DeletePitRequestActionListener implements ActionListener<DeletePitResponse> {
    private static final Logger logger = LogManager.getLogger(DeletePitRequestActionListener.class);

    private final StreamObserver<DeletePITResponse> responseObserver;

    /**
     * Creates a listener that forwards PIT deletion responses to the gRPC stream.
     *
     * @param responseObserver the observer that receives the protobuf response
     */
    public DeletePitRequestActionListener(StreamObserver<DeletePITResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(DeletePitResponse response) {
        try {
            DeletePITResponse protoResponse = DeletePitResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            logger.error("Failed to convert delete PIT response to protobuf: {}", e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.debug("DeletePitRequestActionListener failed to process delete PIT request: {}", e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
