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

import java.util.List;

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
        this.interceptors = interceptors;
    }


    /**
     * Intercepts a gRPC call, executing the chain of interceptors in order.
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
        return executeChain(call, headers, next, 0);
    }

    /**
     * Recursively executes the interceptor chain, handling exceptions and ensuring the call is closed on failure.
     */

    private <ReqT, RespT> ServerCall.Listener<ReqT> executeChain(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next,
        int index
    ) {
        if (index >= interceptors.size()) {
            // All interceptors processed, call the actual service
            return next.startCall(call, headers);
        }

        OrderedGrpcInterceptor currentInterceptor = interceptors.get(index);

        try {
            // Create handler for the rest of the chain
            ServerCallHandler<ReqT, RespT> chainHandler = new ServerCallHandler<ReqT, RespT>() {
                @Override
                public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
                    return executeChain(call, headers, next, index + 1);
                }
            };

            // Execute current interceptor
            return currentInterceptor.getInterceptor().interceptCall(call, headers, chainHandler);

        } catch (Exception e) {
            logger.error("Interceptor at index [{}] failed: {}", index, e.getMessage());
            // Close the call with error
            call.close(Status.INTERNAL.withDescription("Interceptor failure: " + e.getMessage()), headers);
            return new ServerCall.Listener<ReqT>() {
            };
        }
    }
}
