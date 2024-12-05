/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.ByteBufferStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.channels.TraceableTcpTransportChannel;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.nativeprotocol.NativeOutboundHandler;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Native handler for inbound data
 *
 * @opensearch.internal
 */
public class NativeMessageHandler implements ProtocolMessageHandler {

    private static final Logger logger = LogManager.getLogger(NativeMessageHandler.class);

    private final ThreadPool threadPool;
    private final NativeOutboundHandler outboundHandler;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;
    private final Transport.ResponseHandlers responseHandlers;
    private final Transport.RequestHandlers requestHandlers;

    private final Tracer tracer;

    NativeMessageHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        Transport.RequestHandlers requestHandlers,
        Transport.ResponseHandlers responseHandlers,
        Tracer tracer,
        TransportKeepAlive keepAlive
    ) {
        this.threadPool = threadPool;
        this.outboundHandler = new NativeOutboundHandler(nodeName, version, features, statsTracker, threadPool, bigArrays, outboundHandler);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handshaker = handshaker;
        this.requestHandlers = requestHandlers;
        this.responseHandlers = responseHandlers;
        this.tracer = tracer;
        this.keepAlive = keepAlive;
    }

    // Empty stream constant to avoid instantiating a new stream for empty messages.
    private static final StreamInput EMPTY_STREAM_INPUT = new ByteBufferStreamInput(ByteBuffer.wrap(BytesRef.EMPTY_BYTES));

    @Override
    public void messageReceived(
        TcpChannel channel,
        ProtocolInboundMessage message,
        long startTime,
        long slowLogThresholdMs,
        TransportMessageListener messageListener
    ) throws IOException {
        InboundMessage inboundMessage = (InboundMessage) message;
        TransportLogger.logInboundMessage(channel, inboundMessage);
        if (inboundMessage.isPing()) {
            keepAlive.receiveKeepAlive(channel);
        } else {
            handleMessage(channel, inboundMessage, startTime, slowLogThresholdMs, messageListener);
        }
    }

    private void handleMessage(
        TcpChannel channel,
        InboundMessage message,
        long startTime,
        long slowLogThresholdMs,
        TransportMessageListener messageListener
    ) throws IOException {
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        assert header.needsToReadVariableHeader() == false;
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            threadContext.setHeaders(header.getHeaders());
            threadContext.putTransient("_remote_address", remoteAddress);
            if (header.isRequest()) {
                handleRequest(channel, header, message, messageListener);
            } else {
                // Responses do not support short circuiting currently
                assert message.isShortCircuit() == false;
                final TransportResponseHandler<?> handler;
                long requestId = header.getRequestId();
                if (header.isHandshake()) {
                    handler = handshaker.removeHandlerForHandshake(requestId);
                } else {
                    TransportResponseHandler<? extends TransportResponse> theHandler = responseHandlers.onResponseReceived(
                        requestId,
                        messageListener
                    );
                    if (theHandler == null && header.isError()) {
                        handler = handshaker.removeHandlerForHandshake(requestId);
                    } else {
                        handler = theHandler;
                    }
                }
                // ignore if its null, the service logs it
                if (handler != null) {
                    final StreamInput streamInput;
                    if (message.getContentLength() > 0 || header.getVersion().equals(Version.CURRENT) == false) {
                        streamInput = namedWriteableStream(message.openOrGetStreamInput());
                        assertRemoteVersion(streamInput, header.getVersion());
                        if (header.isError()) {
                            handlerResponseError(requestId, streamInput, handler);
                        } else {
                            handleResponse(requestId, remoteAddress, streamInput, handler);
                        }
                    } else {
                        assert header.isError() == false;
                        handleResponse(requestId, remoteAddress, EMPTY_STREAM_INPUT, handler);
                    }
                }

            }
        } finally {
            final long took = threadPool.relativeTimeInMillis() - startTime;
            final long logThreshold = slowLogThresholdMs;
            if (logThreshold > 0 && took > logThreshold) {
                logger.warn(
                    "handling inbound transport message [{}] took [{}ms] which is above the warn threshold of [{}ms]",
                    message,
                    took,
                    logThreshold
                );
            }
        }
    }

    private Map<String, Collection<String>> extractHeaders(Map<String, String> headers) {
        return headers.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> Collections.singleton(e.getValue())));
    }

    private <T extends TransportRequest> void handleRequest(
        TcpChannel channel,
        Header header,
        InboundMessage message,
        TransportMessageListener messageListener
    ) throws IOException {
        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final Version version = header.getVersion();
        final Map<String, Collection<String>> headers = extractHeaders(header.getHeaders().v1());
        Span span = tracer.startSpan(SpanBuilder.from(action, channel), headers);
        try (SpanScope spanScope = tracer.withSpanInScope(span)) {
            if (header.isHandshake()) {
                messageListener.onRequestReceived(requestId, action);
                // Cannot short circuit handshakes
                assert message.isShortCircuit() == false;
                final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                assertRemoteVersion(stream, header.getVersion());
                final TcpTransportChannel transportChannel = new TcpTransportChannel(
                    outboundHandler,
                    channel,
                    action,
                    requestId,
                    version,
                    header.getFeatures(),
                    header.isCompressed(),
                    header.isHandshake(),
                    message.takeBreakerReleaseControl()
                );
                TransportChannel traceableTransportChannel = TraceableTcpTransportChannel.create(transportChannel, span, tracer);
                try {
                    handshaker.handleHandshake(traceableTransportChannel, requestId, stream);
                } catch (Exception e) {
                    if (Version.CURRENT.isCompatible(header.getVersion())) {
                        sendErrorResponse(action, traceableTransportChannel, e);
                    } else {
                        logger.warn(
                            new ParameterizedMessage(
                                "could not send error response to handshake received on [{}] using wire format version [{}], closing channel",
                                channel,
                                header.getVersion()
                            ),
                            e
                        );
                        channel.close();
                    }
                }
            } else {
                final TcpTransportChannel transportChannel = new TcpTransportChannel(
                    outboundHandler,
                    channel,
                    action,
                    requestId,
                    version,
                    header.getFeatures(),
                    header.isCompressed(),
                    header.isHandshake(),
                    message.takeBreakerReleaseControl()
                );
                TransportChannel traceableTransportChannel = TraceableTcpTransportChannel.create(transportChannel, span, tracer);
                try {
                    messageListener.onRequestReceived(requestId, action);
                    if (message.isShortCircuit()) {
                        sendErrorResponse(action, traceableTransportChannel, message.getException());
                    } else {
                        final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                        assertRemoteVersion(stream, header.getVersion());
                        final RequestHandlerRegistry<T> reg = requestHandlers.getHandler(action);
                        assert reg != null;

                        final T request = newRequest(requestId, action, stream, reg);
                        request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                        checkStreamIsFullyConsumed(requestId, action, stream);

                        final String executor = reg.getExecutor();
                        if (ThreadPool.Names.SAME.equals(executor)) {
                            try {
                                reg.processMessageReceived(request, traceableTransportChannel);
                            } catch (Exception e) {
                                sendErrorResponse(reg.getAction(), traceableTransportChannel, e);
                            }
                        } else {
                            threadPool.executor(executor).execute(new RequestHandler<>(reg, request, traceableTransportChannel));
                        }
                    }
                } catch (Exception e) {
                    sendErrorResponse(action, traceableTransportChannel, e);
                }
            }
        } catch (Exception e) {
            span.setError(e);
            span.endSpan();
            throw e;
        }
    }

    /**
     * Creates new request instance out of input stream. Throws IllegalStateException if the end of
     * the stream was reached before the request is fully deserialized from the stream.
     * @param <T> transport request type
     * @param requestId request identifier
     * @param action action name
     * @param stream stream
     * @param reg request handler registry
     * @return new request instance
     * @throws IOException IOException
     * @throws IllegalStateException IllegalStateException
     */
    private <T extends TransportRequest> T newRequest(
        final long requestId,
        final String action,
        final StreamInput stream,
        final RequestHandlerRegistry<T> reg
    ) throws IOException {
        try {
            return reg.newRequest(stream);
        } catch (final EOFException e) {
            // Another favor of (de)serialization issues is when stream contains less bytes than
            // the request handler needs to deserialize the payload.
            throw new IllegalStateException(
                "Message fully read (request) but more data is expected for requestId ["
                    + requestId
                    + "], action ["
                    + action
                    + "]; resetting",
                e
            );
        }
    }

    /**
     * Checks if the stream is fully consumed and throws the exceptions if that is not the case.
     * @param requestId request identifier
     * @param action action name
     * @param stream stream
     * @throws IOException IOException
     */
    private void checkStreamIsFullyConsumed(final long requestId, final String action, final StreamInput stream) throws IOException {
        // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
        final int nextByte = stream.read();

        // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
        if (nextByte != -1) {
            throw new IllegalStateException(
                "Message not fully read (request) for requestId ["
                    + requestId
                    + "], action ["
                    + action
                    + "], available ["
                    + stream.available()
                    + "]; resetting"
            );
        }
    }

    /**
     * Checks if the stream is fully consumed and throws the exceptions if that is not the case.
     * @param requestId request identifier
     * @param handler response handler
     * @param stream stream
     * @param error "true" if response represents error, "false" otherwise
     * @throws IOException IOException
     */
    private void checkStreamIsFullyConsumed(
        final long requestId,
        final TransportResponseHandler<?> handler,
        final StreamInput stream,
        final boolean error
    ) throws IOException {
        if (stream != EMPTY_STREAM_INPUT) {
            // Check the entire message has been read
            final int nextByte = stream.read();
            // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
            if (nextByte != -1) {
                throw new IllegalStateException(
                    "Message not fully read (response) for requestId ["
                        + requestId
                        + "], handler ["
                        + handler
                        + "], error ["
                        + error
                        + "]; resetting"
                );
            }
        }
    }

    private static void sendErrorResponse(String actionName, TransportChannel transportChannel, Exception e) {
        try {
            transportChannel.sendResponse(e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn(() -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", actionName), inner);
        }
    }

    private <T extends TransportResponse> void handleResponse(
        final long requestId,
        InetSocketAddress remoteAddress,
        final StreamInput stream,
        final TransportResponseHandler<T> handler
    ) {
        final T response;
        try {
            response = handler.read(stream);
            response.remoteAddress(new TransportAddress(remoteAddress));
            checkStreamIsFullyConsumed(requestId, handler, stream, false);
        } catch (Exception e) {
            final Exception serializationException = new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler + "]",
                e
            );
            logger.warn(new ParameterizedMessage("Failed to deserialize response from [{}]", remoteAddress), serializationException);
            handleException(handler, serializationException);
            return;
        }
        final String executor = handler.executor();
        if (ThreadPool.Names.SAME.equals(executor)) {
            doHandleResponse(handler, response);
        } else {
            threadPool.executor(executor).execute(() -> doHandleResponse(handler, response));
        }
    }

    private <T extends TransportResponse> void doHandleResponse(TransportResponseHandler<T> handler, T response) {
        try {
            handler.handleResponse(response);
        } catch (Exception e) {
            handleException(handler, new ResponseHandlerFailureTransportException(e));
        }
    }

    private void handlerResponseError(final long requestId, StreamInput stream, final TransportResponseHandler<?> handler) {
        Exception error;
        try {
            error = stream.readException();
            checkStreamIsFullyConsumed(requestId, handler, stream, true);
        } catch (Exception e) {
            error = new TransportSerializationException(
                "Failed to deserialize exception response from stream for handler [" + handler + "]",
                e
            );
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler<?> handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(() -> {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
            }
        });
    }

    private StreamInput namedWriteableStream(StreamInput delegate) {
        return new NamedWriteableAwareStreamInput(delegate, namedWriteableRegistry);
    }

    static void assertRemoteVersion(StreamInput in, Version version) {
        assert version.equals(in.getVersion()) : "Stream version [" + in.getVersion() + "] does not match version [" + version + "]";
    }

    /**
     * Internal request handler
     *
     * @opensearch.internal
     */
    private static class RequestHandler<T extends TransportRequest> extends AbstractRunnable {
        private final RequestHandlerRegistry<T> reg;
        private final T request;
        private final TransportChannel transportChannel;

        RequestHandler(RequestHandlerRegistry<T> reg, T request, TransportChannel transportChannel) {
            this.reg = reg;
            this.request = request;
            this.transportChannel = transportChannel;
        }

        @Override
        protected void doRun() throws Exception {
            reg.processMessageReceived(request, transportChannel);
        }

        @Override
        public boolean isForceExecution() {
            return reg.isForceExecution();
        }

        @Override
        public void onFailure(Exception e) {
            sendErrorResponse(reg.getAction(), transportChannel, e);
        }
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        outboundHandler.setMessageListener(listener);
    }

}
