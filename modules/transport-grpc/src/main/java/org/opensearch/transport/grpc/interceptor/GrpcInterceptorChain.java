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
import io.grpc.StatusRuntimeException;

/**
 * Simple gRPC interceptor chain that executes OrderedGrpcInterceptors in order and handles exceptions
 */
public class GrpcInterceptorChain implements ServerInterceptor {

    private static final Logger logger = LogManager.getLogger(GrpcInterceptorChain.class);

    private static final ServerCall.Listener<Object> EMPTY_LISTENER = new ServerCall.Listener<>() {
    };

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
        ServerCallHandler<ReqT, RespT> currentHandler = next;

        // This ensures forward execution: interceptor[0] -> interceptor[1] -> ... -> service
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
                    } catch (StatusRuntimeException sre) {
                        // Interceptor threw a gRPC status - respect it (e.g., PERMISSION_DENIED, UNAUTHENTICATED)
                        logger.error(
                            "Interceptor at index [{}] failed with status [{}]: {}",
                            index,
                            sre.getStatus().getCode(),
                            sre.getMessage()
                        );
                        call.close(sre.getStatus(), headers);
                        return emptyListener();
                    } catch (Exception e) {
                        // Unexpected exception - wrap in INTERNAL for safety
                        logger.error("Interceptor at index [{}] failed unexpectedly: {}", index, e.getMessage());
                        call.close(Status.INTERNAL.withDescription("Interceptor failure: " + e.getMessage()), headers);
                        return emptyListener();
                    }
                }
            };
        }

        // Start the chain execution
        return currentHandler.startCall(call, headers);
    }

    /**
     * Returns a reusable empty listener to minimize object allocation on interceptor failures.
     * @param <ReqT> the request type
     * @return an empty ServerCall.Listener
     */
    @SuppressWarnings("unchecked")
    private static <ReqT> ServerCall.Listener<ReqT> emptyListener() {
        return (ServerCall.Listener<ReqT>) EMPTY_LISTENER;
    }
}
