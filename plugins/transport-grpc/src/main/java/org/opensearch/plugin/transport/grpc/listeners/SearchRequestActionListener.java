/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.plugin.transport.grpc.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.transport.grpc.proto.response.search.SearchResponseProtoUtils;

import java.io.IOException;

import io.grpc.stub.StreamObserver;

/**
 * Listener for search request execution completion, handling successful and failure scenarios.
 */
public class SearchRequestActionListener implements ActionListener<SearchResponse> {
    private static final Logger logger = LogManager.getLogger(SearchRequestActionListener.class);

    private final StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver;

    /**
     * Constructs a new SearchRequestActionListener.
     *
     * @param responseObserver the gRPC stream observer to send the search response to
     */
    public SearchRequestActionListener(StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver) {
        super();
        this.responseObserver = responseObserver;
    }

    @Override
    public void onResponse(SearchResponse response) {
        // Search execution succeeded. Convert the opensearch internal response to protobuf
        try {
            org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException | IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.error("SearchRequestActionListener failed to process search request:" + e.getMessage());
        responseObserver.onError(e);
    }
}
