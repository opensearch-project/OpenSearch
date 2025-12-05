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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.protobufs.GlobalParams;
import org.opensearch.transport.grpc.proto.response.search.SearchResponseProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import java.io.IOException;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for search request execution completion, handling successful and failure scenarios.
 */
public class SearchRequestActionListener implements ActionListener<SearchResponse> {
    private static final Logger logger = LogManager.getLogger(SearchRequestActionListener.class);

    private final StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver;
    private final GlobalParams params;

    /**
     * Constructs a new SearchRequestActionListener.
     *
     * @param responseObserver the gRPC stream observer to send the search response to
     * @param params parameters that are going to change how responses and errors are handled
     */
    public SearchRequestActionListener(StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver, GlobalParams params) {
        super();
        this.responseObserver = responseObserver;
        this.params = params;
    }

    @Override
    public void onResponse(SearchResponse response) {
        // Search execution succeeded. Convert the opensearch internal response to protobuf
        try {
            org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(response, params);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException | IOException e) {
            logger.error("Failed to convert search response to protobuf: " + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e, params);
            responseObserver.onError(grpcError);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.debug("SearchRequestActionListener failed to process search request: " + e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e, params);
        responseObserver.onError(grpcError);
    }
}
