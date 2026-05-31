/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status;

public class BufferReleasingClientInterceptorTests extends OpenSearchTestCase {

    /**
     * Closeable stand-in for an ArrowMessage. Increments a counter on each close().
     */
    private static final class CountingCloseable implements AutoCloseable {
        private final AtomicInteger closeCount;

        CountingCloseable(AtomicInteger closeCount) {
            this.closeCount = closeCount;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    /**
     * Captures the wrapped MethodDescriptor and the listener so the test can drive the
     * call as if it were gRPC.
     */
    private static final class CapturingChannel extends Channel {
        MethodDescriptor<byte[], CountingCloseable> capturedMethod;
        ClientCall.Listener<CountingCloseable> capturedListener;

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
            @SuppressWarnings("unchecked")
            MethodDescriptor<byte[], CountingCloseable> typed = (MethodDescriptor<byte[], CountingCloseable>) methodDescriptor;
            capturedMethod = typed;
            return new ClientCall<ReqT, RespT>() {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    @SuppressWarnings("unchecked")
                    ClientCall.Listener<CountingCloseable> typedListener = (ClientCall.Listener<CountingCloseable>) responseListener;
                    capturedListener = typedListener;
                }

                @Override
                public void request(int numMessages) {}

                @Override
                public void cancel(String message, Throwable cause) {}

                @Override
                public void halfClose() {}

                @Override
                public void sendMessage(ReqT message) {}
            };
        }

        @Override
        public String authority() {
            return "test";
        }
    }

    private MethodDescriptor<byte[], CountingCloseable> buildMethod(AtomicInteger closeCount, List<CountingCloseable> parsed) {
        Marshaller<byte[]> requestMarshaller = new Marshaller<>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                return new byte[0];
            }
        };
        Marshaller<CountingCloseable> responseMarshaller = new Marshaller<>() {
            @Override
            public InputStream stream(CountingCloseable value) {
                return new ByteArrayInputStream(new byte[0]);
            }

            @Override
            public CountingCloseable parse(InputStream stream) {
                CountingCloseable c = new CountingCloseable(closeCount);
                parsed.add(c);
                return c;
            }
        };
        return MethodDescriptor.<byte[], CountingCloseable>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName("test/Stream")
            .setRequestMarshaller(requestMarshaller)
            .setResponseMarshaller(responseMarshaller)
            .build();
    }

    public void testUndeliveredMessagesClosedOnTermination() {
        AtomicInteger closeCount = new AtomicInteger();
        List<CountingCloseable> parsed = new ArrayList<>();
        MethodDescriptor<byte[], CountingCloseable> method = buildMethod(closeCount, parsed);

        BufferReleasingClientInterceptor interceptor = new BufferReleasingClientInterceptor();
        CapturingChannel channel = new CapturingChannel();

        ClientCall<byte[], CountingCloseable> wrappedCall = interceptor.interceptCall(method, CallOptions.DEFAULT, channel);
        AtomicReference<Status> closeStatus = new AtomicReference<>();
        wrappedCall.start(new ClientCall.Listener<>() {
            @Override
            public void onClose(Status status, Metadata trailers) {
                closeStatus.set(status);
            }
        }, new Metadata());

        // Simulate gRPC parsing 3 messages off the wire via the (now-tracking) marshaller.
        Marshaller<CountingCloseable> tracking = channel.capturedMethod.getResponseMarshaller();
        CountingCloseable m0 = tracking.parse(new ByteArrayInputStream(new byte[0]));
        CountingCloseable m1 = tracking.parse(new ByteArrayInputStream(new byte[0]));
        CountingCloseable m2 = tracking.parse(new ByteArrayInputStream(new byte[0]));

        // Application consumes m0 only; m1 and m2 stay queued.
        channel.capturedListener.onMessage(m0);

        // Stream cancelled (server error / client cancel / deadline — all flow through onClose).
        channel.capturedListener.onClose(Status.CANCELLED, new Metadata());

        assertSame(Status.CANCELLED, closeStatus.get());
        // Interceptor closed the two undelivered messages; the delivered one is the application's responsibility.
        assertEquals("expected interceptor to close 2 undelivered messages", 2, closeCount.get());
        assertEquals(3, parsed.size());
        assertSame(parsed.get(0), m0);
    }

    public void testNoDoubleCloseWhenAllMessagesDelivered() {
        AtomicInteger closeCount = new AtomicInteger();
        List<CountingCloseable> parsed = new ArrayList<>();
        MethodDescriptor<byte[], CountingCloseable> method = buildMethod(closeCount, parsed);

        BufferReleasingClientInterceptor interceptor = new BufferReleasingClientInterceptor();
        CapturingChannel channel = new CapturingChannel();

        ClientCall<byte[], CountingCloseable> wrappedCall = interceptor.interceptCall(method, CallOptions.DEFAULT, channel);
        wrappedCall.start(new ClientCall.Listener<>() {
        }, new Metadata());

        Marshaller<CountingCloseable> tracking = channel.capturedMethod.getResponseMarshaller();
        CountingCloseable m0 = tracking.parse(new ByteArrayInputStream(new byte[0]));
        CountingCloseable m1 = tracking.parse(new ByteArrayInputStream(new byte[0]));

        channel.capturedListener.onMessage(m0);
        channel.capturedListener.onMessage(m1);
        channel.capturedListener.onClose(Status.OK, new Metadata());

        assertEquals("interceptor must not close messages already delivered to the application", 0, closeCount.get());
    }

    public void testNonCloseableResponsesPassThrough() {
        AtomicInteger closeCount = new AtomicInteger();
        // String responses — not AutoCloseable, must not be tracked.
        Marshaller<byte[]> requestMarshaller = new Marshaller<>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                return new byte[0];
            }
        };
        Marshaller<String> responseMarshaller = new Marshaller<>() {
            @Override
            public InputStream stream(String value) {
                return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public String parse(InputStream stream) {
                return "hello";
            }
        };
        MethodDescriptor<byte[], String> method = MethodDescriptor.<byte[], String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("test/Unary")
            .setRequestMarshaller(requestMarshaller)
            .setResponseMarshaller(responseMarshaller)
            .build();

        BufferReleasingClientInterceptor interceptor = new BufferReleasingClientInterceptor();
        AtomicReference<MethodDescriptor<byte[], String>> captured = new AtomicReference<>();
        AtomicReference<ClientCall.Listener<String>> capturedListener = new AtomicReference<>();
        Channel channel = new Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> m, CallOptions callOptions) {
                @SuppressWarnings("unchecked")
                MethodDescriptor<byte[], String> typed = (MethodDescriptor<byte[], String>) m;
                captured.set(typed);
                return new ClientCall<ReqT, RespT>() {
                    @Override
                    public void start(Listener<RespT> l, Metadata h) {
                        @SuppressWarnings("unchecked")
                        ClientCall.Listener<String> typedListener = (ClientCall.Listener<String>) l;
                        capturedListener.set(typedListener);
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String msg, Throwable c) {}

                    @Override
                    public void halfClose() {}

                    @Override
                    public void sendMessage(ReqT msg) {}
                };
            }

            @Override
            public String authority() {
                return "test";
            }
        };

        ClientCall<byte[], String> wrappedCall = interceptor.interceptCall(method, CallOptions.DEFAULT, channel);
        wrappedCall.start(new ClientCall.Listener<>() {
        }, new Metadata());

        // Parse a non-AutoCloseable response — must not blow up; nothing to close.
        String parsed = captured.get().getResponseMarshaller().parse(new ByteArrayInputStream(new byte[0]));
        assertEquals("hello", parsed);
        capturedListener.get().onClose(Status.CANCELLED, new Metadata());
        assertEquals(0, closeCount.get());
    }
}
