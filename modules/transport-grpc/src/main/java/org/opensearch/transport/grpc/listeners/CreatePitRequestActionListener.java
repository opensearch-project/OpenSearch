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
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.grpc.proto.response.search.CreatePitResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import java.io.IOException;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for PIT creation request completion.
 */
public class CreatePitRequestActionListener implements ActionListener<CreatePitResponse> {
    private static final Logger logger = LogManager.getLogger(CreatePitRequestActionListener.class);

    private final StreamObserver<org.opensearch.protobufs.CreatePITResponse> responseObserver;

    public CreatePitRequestActionListener(StreamObserver<org.opensearch.protobufs.CreatePITResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(CreatePitResponse response) {
        try {
            org.opensearch.protobufs.CreatePITResponse protoResponse = CreatePitResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException | IOException e) {
            logger.error("Failed to convert create PIT response to protobuf: {}", e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.debug("CreatePitRequestActionListener failed to process create PIT request: {}", e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
