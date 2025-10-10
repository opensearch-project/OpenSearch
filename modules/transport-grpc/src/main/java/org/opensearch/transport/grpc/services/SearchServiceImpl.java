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
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.listeners.SearchRequestActionListener;
import org.opensearch.transport.grpc.proto.request.search.SearchRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import java.io.IOException;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of the gRPC SearchService.
 * This class handles incoming gRPC search requests, converts them to OpenSearch search requests,
 * executes them using the provided client, and returns the results back to the gRPC client.
 */
public class SearchServiceImpl extends SearchServiceGrpc.SearchServiceImplBase {
    private static final Logger logger = LogManager.getLogger(SearchServiceImpl.class);
    private final Client client;
    private final AbstractQueryBuilderProtoUtils queryUtils;

    /**
     * Creates a new SearchServiceImpl.
     *
     * @param client Client for executing actions on the local node
     * @param queryUtils Query utils instance for parsing protobuf queries
     */
    public SearchServiceImpl(Client client, AbstractQueryBuilderProtoUtils queryUtils) {
        if (client == null) {
            throw new IllegalArgumentException("Client cannot be null");
        }
        if (queryUtils == null) {
            throw new IllegalArgumentException("Query utils cannot be null");
        }

        this.client = client;
        this.queryUtils = queryUtils;
    }

    /**
     * Processes a search request.
     *
     * @param request The search request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void search(
        org.opensearch.protobufs.SearchRequest request,
        StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver
    ) {

        try {
            org.opensearch.action.search.SearchRequest searchRequest = SearchRequestProtoUtils.prepareRequest(request, client, queryUtils);
            SearchRequestActionListener listener = new SearchRequestActionListener(responseObserver);
            client.search(searchRequest, listener);
        } catch (RuntimeException | IOException e) {
            logger.debug("SearchServiceImpl failed to process search request, request=" + request + ", error=" + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }
}
