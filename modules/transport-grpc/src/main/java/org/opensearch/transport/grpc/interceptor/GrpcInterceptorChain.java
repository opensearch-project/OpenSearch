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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.transport.grpc.spi.GrpcInterceptorProvider.OrderedGrpcInterceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * gRPC interceptor chain that executes OrderedGrpcInterceptors in order and handles exceptions.
 * Captures OpenSearch ThreadContext after interceptors run and restores it during callback execution
 * to survive thread switches between gRPC threads and OpenSearch executor threads.
 */
public class GrpcInterceptorChain implements ServerInterceptor {

    private static final Logger logger = LogManager.getLogger(GrpcInterceptorChain.class);

    private static final ServerCall.Listener<Object> EMPTY_LISTENER = new ServerCall.Listener<>() {
    };

    private final List<OrderedGrpcInterceptor> interceptors = new ArrayList<>();
    private final ThreadContext threadContext;

    /**
     * Constructs an empty GrpcInterceptorChain.
     *
     * @param threadContext The ThreadContext to capture and propagate
     */
    public GrpcInterceptorChain(ThreadContext threadContext) {
        this.threadContext = Objects.requireNonNull(threadContext, "ThreadContext cannot be null");
    }

    /**
     * Constructs a GrpcInterceptorChain with the provided list of ordered interceptors.
     *
     * @param threadContext The ThreadContext to capture and propagate
     * @param interceptors  List of OrderedGrpcInterceptor instances to be applied in order
     */
    public GrpcInterceptorChain(ThreadContext threadContext, List<OrderedGrpcInterceptor> interceptors) {
        this.threadContext = Objects.requireNonNull(threadContext, "ThreadContext cannot be null");
        this.interceptors.addAll(Objects.requireNonNull(interceptors));
    }

    /**
     * Adds interceptors to the chain.
     *
     * @param interceptors List of OrderedGrpcInterceptor instances to be added
     */
    public void addInterceptors(List<OrderedGrpcInterceptor> interceptors) {
        this.interceptors.addAll(Objects.requireNonNull(interceptors));
    }

    /**
     * Intercepts a gRPC call, executing the chain of interceptors in order.
     * Captures ThreadContext after interceptors execute and restores it in all listener callbacks.
     *
     * @param call    object to receive response messages
     * @param headers which can contain extra call metadata
     * @param next    next processor in the interceptor chain
     * @return a listener for processing incoming request messages
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next
    ) {
        ServerCallHandler<ReqT, RespT> currentHandler = next;

        for (int i = interceptors.size() - 1; i >= 0; i--) {
            final OrderedGrpcInterceptor interceptor = interceptors.get(i);
            final ServerCallHandler<ReqT, RespT> nextHandler = currentHandler;
            final int index = i;

            currentHandler = new ServerCallHandler<ReqT, RespT>() {
                @Override
                public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
                    try {
                        return interceptor.getInterceptor().interceptCall(call, headers, nextHandler);
                    } catch (StatusRuntimeException sre) {
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

        ServerCall.Listener<ReqT> delegate = currentHandler.startCall(call, headers);

        // Capture ThreadContext state AFTER interceptors have executed using newRestorableContext
        // This follows the same pattern as ContextPreservingActionListener.
        // Interceptors may have added transients/headers that need to propagate across thread switches.
        final Supplier<ThreadContext.StoredContext> contextSupplier = threadContext.newRestorableContext(false);

        // Wrap the listener to restore ThreadContext in all callbacks
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            private void runWithThreadContext(Runnable r) {
                try (ThreadContext.StoredContext ignored = contextSupplier.get()) {
                    r.run();
                }
            }

            @Override
            public void onMessage(ReqT message) {
                runWithThreadContext(() -> super.onMessage(message));
            }

            @Override
            public void onHalfClose() {
                runWithThreadContext(super::onHalfClose);
            }

            @Override
            public void onReady() {
                runWithThreadContext(super::onReady);
            }

            @Override
            public void onCancel() {
                runWithThreadContext(super::onCancel);
            }

            @Override
            public void onComplete() {
                runWithThreadContext(super::onComplete);
            }
        };
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
