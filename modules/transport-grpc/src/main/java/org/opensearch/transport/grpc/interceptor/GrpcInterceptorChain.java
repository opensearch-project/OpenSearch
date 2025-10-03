/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.transport.grpc.spi.OrderedGrpcInterceptor;

import java.util.List;
import java.util.Objects;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Simple gRPC interceptor chain that executes OrderedGrpcInterceptors in order and handles exceptions
 */
public class GrpcInterceptorChain implements ServerInterceptor {

    private static final Logger logger = LogManager.getLogger(GrpcInterceptorChain.class);
    private final List<OrderedGrpcInterceptor> interceptors;

    /**
     * Constructs a GrpcInterceptorChain with the provided list of ordered interceptors.
     * @param interceptors List of OrderedGrpcInterceptor instances to be applied in order
     */
    public GrpcInterceptorChain(List<OrderedGrpcInterceptor> interceptors) {
        this.interceptors = Objects.requireNonNull(interceptors);
    }

    /**
     * Intercepts a gRPC call, executing the chain of interceptors in order.
     * Uses an iterative approach similar to OpenSearch's ActionFilter chain.
     * @param call object to receive response messages
     * @param headers which can contain extra call metadata
     * @param next next processor in the interceptor chain
     * @return a listener for processing incoming request messages
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next
    ) {
        // Build the chain iteratively from end to start, similar to ActionFilter pattern
        ServerCallHandler<ReqT, RespT> currentHandler = next;

        // Iterate backwards through interceptors to build the chain
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            final OrderedGrpcInterceptor interceptor = interceptors.get(i);
            final ServerCallHandler<ReqT, RespT> nextHandler = currentHandler;
            final int index = i;

            // Wrap each interceptor with exception handling
            currentHandler = new ServerCallHandler<ReqT, RespT>() {
                @Override
                public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
                    try {
                        return interceptor.getInterceptor().interceptCall(call, headers, nextHandler);
                    } catch (Exception e) {
                        logger.error("Interceptor at index [{}] failed: {}", index, e.getMessage());
                        // Close the call with error
                        call.close(Status.INTERNAL.withDescription("Interceptor failure: " + e.getMessage()), headers);
                        return new ServerCall.Listener<ReqT>() {
                        };
                    }
                }
            };
        }

        // Start the chain
        return currentHandler.startCall(call, headers);
    }
}
