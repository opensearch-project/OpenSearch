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
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.protobufs.services.DocumentServiceGrpc;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.listeners.BulkRequestActionListener;
import org.opensearch.transport.grpc.proto.request.document.bulk.BulkRequestProtoUtils;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the gRPC Document Service.
 */
public class DocumentServiceImpl extends DocumentServiceGrpc.DocumentServiceImplBase {
    private static final Logger logger = LogManager.getLogger(DocumentServiceImpl.class);
    private final Client client;
    private final CircuitBreakerService circuitBreakerService;

    /**
     * Creates a new DocumentServiceImpl.
     *
     * @param client Client for executing actions on the local node
     * @param circuitBreakerService Circuit breaker service for memory protection
     */
    public DocumentServiceImpl(Client client, CircuitBreakerService circuitBreakerService) {
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
    }

    /**
     * Processes a bulk request.
     * Checks circuit breakers before processing, similar to how REST API handles requests.
     *
     * @param request The bulk request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void bulk(org.opensearch.protobufs.BulkRequest request, StreamObserver<org.opensearch.protobufs.BulkResponse> responseObserver) {
        final int contentLength = request.getSerializedSize();
        CircuitBreaker inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        final AtomicBoolean closed = new AtomicBoolean(false);

        try {
            inFlightRequestsBreaker.addEstimateBytesAndMaybeBreak(contentLength, "<grpc_bulk_request>");

            org.opensearch.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);

            BulkRequestActionListener baseListener = new BulkRequestActionListener(responseObserver);
            org.opensearch.core.action.ActionListener<org.opensearch.action.bulk.BulkResponse> wrappedListener =
                new org.opensearch.core.action.ActionListener<org.opensearch.action.bulk.BulkResponse>() {
                    @Override
                    public void onResponse(org.opensearch.action.bulk.BulkResponse response) {
                        try {
                            baseListener.onResponse(response);
                        } finally {
                            if (closed.compareAndSet(false, true)) {
                                inFlightRequestsBreaker.addWithoutBreaking(-contentLength);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            baseListener.onFailure(e);
                        } finally {
                            if (closed.compareAndSet(false, true)) {
                                inFlightRequestsBreaker.addWithoutBreaking(-contentLength);
                            }
                        }
                    }
                };

            client.bulk(bulkRequest, wrappedListener);
        } catch (RuntimeException e) {
            if (closed.compareAndSet(false, true)) {
                inFlightRequestsBreaker.addWithoutBreaking(-contentLength);
            }
            logger.debug("DocumentServiceImpl failed: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }
}
