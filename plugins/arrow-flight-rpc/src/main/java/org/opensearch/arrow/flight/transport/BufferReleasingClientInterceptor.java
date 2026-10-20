/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.Status;

/**
 * gRPC {@link ClientInterceptor} that closes Arrow Flight response messages whose buffers
 * were parsed off the wire but never delivered to the application.
 *
 * <p>The Flight {@code DoGet}/{@code DoExchange} response marshaller allocates {@code ArrowBuf}s
 * the moment a frame arrives. The parsed message sits in gRPC's per-call {@code MessagesAvailable}
 * queue until the application calls {@code FlightStream.next()}. If the stream is cancelled
 * (server-side error, client cancel, deadline) before that happens, gRPC drops the queued
 * messages without invoking {@link AutoCloseable#close()} on them, so the buffers stay accounted
 * against the flight allocator forever.
 *
 * <p>This interceptor wraps the response marshaller, tracks every parsed message that is
 * {@link AutoCloseable}, removes entries when {@code Listener.onMessage} hands the message off
 * to the application, and closes anything still in the queue when {@code Listener.onClose} fires.
 *
 * @opensearch.internal
 */
final class BufferReleasingClientInterceptor implements ClientInterceptor {

    private static final Logger logger = LogManager.getLogger(BufferReleasingClientInterceptor.class);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method,
        CallOptions callOptions,
        Channel next
    ) {
        TrackingMarshaller<RespT> tracking = new TrackingMarshaller<>(method.getResponseMarshaller());
        MethodDescriptor<ReqT, RespT> wrapped = method.toBuilder(method.getRequestMarshaller(), tracking).build();
        return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(wrapped, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onMessage(RespT message) {
                        super.onMessage(message);
                        tracking.markDelivered(message);
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        try {
                            super.onClose(status, trailers);
                        } finally {
                            tracking.releaseUndelivered();
                        }
                    }
                }, headers);
            }
        };
    }

    private static final class TrackingMarshaller<T> implements Marshaller<T> {
        private final Marshaller<T> delegate;
        private final ConcurrentLinkedQueue<T> outstanding = new ConcurrentLinkedQueue<>();

        TrackingMarshaller(Marshaller<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputStream stream(T value) {
            return delegate.stream(value);
        }

        @Override
        public T parse(InputStream stream) {
            T value = delegate.parse(stream);
            if (value instanceof AutoCloseable) {
                outstanding.add(value);
            }
            return value;
        }

        void markDelivered(T value) {
            outstanding.remove(value);
        }

        void releaseUndelivered() {
            T value;
            while ((value = outstanding.poll()) != null) {
                if (value instanceof AutoCloseable c) {
                    try {
                        c.close();
                    } catch (Exception e) {
                        logger.warn("failed to release Arrow Flight message buffer on stream termination", e);
                    }
                }
            }
        }
    }
}
