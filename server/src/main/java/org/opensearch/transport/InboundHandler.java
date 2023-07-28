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
import org.opensearch.action.admin.cluster.node.info.ProtobufNodeInfo;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.ProtobufTransportNodesInfoAction.NodeInfoRequest;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodeStats;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.ProtobufTransportNodesStatsAction.NodeStatsRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.ByteBufferStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.server.proto.ClusterStateRequestProto.ClusterStateReq;
import org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateRes;
import org.opensearch.server.proto.NodesInfoProto.NodesInfo;
import org.opensearch.server.proto.NodesInfoRequestProto.NodesInfoReq;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.server.proto.NodesStatsRequestProto.NodesStatsReq;
import org.opensearch.server.proto.OutboundMessageProto.OutboundMsg;
import org.opensearch.threadpool.ThreadPool;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handler for inbound data
 *
 * @opensearch.internal
 */
public class InboundHandler {

    private static final Logger logger = LogManager.getLogger(InboundHandler.class);

    private final ThreadPool threadPool;
    private final OutboundHandler outboundHandler;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;
    private final Transport.ResponseHandlers responseHandlers;
    private final Transport.RequestHandlers requestHandlers;
    private final Transport.ProtobufRequestHandlers protobufRequestHandlers;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;
    // private volatile ProtobufTransportMessageListener protobufMessageListener = ProtobufTransportMessageListener.NOOP_LISTENER;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    InboundHandler(
        ThreadPool threadPool,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        TransportKeepAlive keepAlive,
        Transport.RequestHandlers requestHandlers,
        Transport.ProtobufRequestHandlers protobufRequestHandlers,
        Transport.ResponseHandlers responseHandlers
    ) {
        this.threadPool = threadPool;
        this.outboundHandler = outboundHandler;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handshaker = handshaker;
        this.keepAlive = keepAlive;
        this.requestHandlers = requestHandlers;
        this.protobufRequestHandlers = protobufRequestHandlers;
        this.responseHandlers = responseHandlers;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    // void setProtobufMessageListener(ProtobufTransportMessageListener listener) {
    //     if (protobufMessageListener == ProtobufTransportMessageListener.NOOP_LISTENER) {
    //         protobufMessageListener = listener;
    //     } else {
    //         throw new IllegalStateException("Cannot set message listener twice");
    //     }
    // }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.getMillis();
    }

    void inboundMessage(TcpChannel channel, InboundMessage message) throws Exception {
        System.out.println("InboundHandler.inboundMessage");
        System.out.println("message: " + message);
        final long startTime = threadPool.relativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        TransportLogger.logInboundMessage(channel, message);

        if (message.isPing()) {
            keepAlive.receiveKeepAlive(channel);
        } else {
            messageReceived(channel, message, startTime);
        }
    }

    void inboundMessageProtobuf(TcpChannel channel, BytesReference message) throws IOException {
        System.out.println("InboundHandler.inboundMessageProtobuf");
        System.out.println("message: " + message);
        final long startTime = threadPool.relativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        ProtobufOutboundMessage protobufOutboundMessage = new ProtobufOutboundMessage(BytesReference.toBytes(message)); 
        messageReceivedProtobuf(channel, protobufOutboundMessage, startTime);
    }

    // Empty stream constant to avoid instantiating a new stream for empty messages.
    private static final StreamInput EMPTY_STREAM_INPUT = new ByteBufferStreamInput(ByteBuffer.wrap(BytesRef.EMPTY_BYTES));

    private static final CodedInputStream EMPTY_CODED_INPUT_STREAM = CodedInputStream.newInstance(BytesRef.EMPTY_BYTES);

    private void messageReceivedProtobuf(TcpChannel channel, ProtobufOutboundMessage message, long startTime) throws IOException {
        System.out.println("InboundHandler.messageReceivedProtobuf");
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final org.opensearch.server.proto.OutboundMessageProto.OutboundMsg.Header header = message.getHeader();
        System.out.println("header: " + header);
        System.out.println("message: " + message);
        System.out.println("remoteAddress: " + remoteAddress);

        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            final Tuple<Map<String, String>, Map<String, Set<String>>> headers = new Tuple<Map<String,String>,Map<String,Set<String>>>(message.getRequestHeaders(), message.getResponseHandlers());
            threadContext.setHeaders(headers);
            threadContext.putTransient("_remote_address", remoteAddress);
            if (TransportStatus.isRequest(header.getStatus().byteAt(0))) {
                System.out.println("InboundHandler.messageReceivedProtobuf isRequest");
                System.out.println("Header: " + header);
                System.out.println("message: " + message);
                handleRequestProtobuf(channel, header, message);
            } else {
                System.out.println("InboundHandler.messageReceivedProtobuf isResponse");
                long requestId = header.getRequestId();
                TransportResponseHandler<? extends TransportResponse> handler = responseHandlers.onResponseReceived(
                    requestId,
                    messageListener
                );
                System.out.println("Found the handler: " + handler);
                if (handler != null) {
                    System.out.println("Handler for the response is: " + handler);
                    System.out.println("Handler for the response is: " + handler.toString());
                    System.out.println("The final message is: " + message);
                    if (handler.toString().contains("Protobuf")) {
                        System.out.println("Trying protobuf");
                        handleProtobufResponse(requestId, remoteAddress, message, handler);
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

    private void messageReceived(TcpChannel channel, InboundMessage message, long startTime) throws IOException {
        System.out.println("InboundHandler.messageReceived");
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        System.out.println("header: " + header);
        System.out.println("message: " + message);
        System.out.println("remoteAddress: " + remoteAddress);
        assert header.needsToReadVariableHeader() == false;

        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            threadContext.setHeaders(header.getHeaders());
            threadContext.putTransient("_remote_address", remoteAddress);
            if (header.isRequest()) {
                System.out.println("InboundHandler.messageReceived isRequest");
                System.out.println("Header: " + header);
                handleRequest(channel, header, message);
            } else {
                System.out.println("InboundHandler.messageReceived isResponse else");
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
                    System.out.println("Found the handler: " + handler);
                }
                // ignore if its null, the service logs it
                if (handler != null) {
                    System.out.println("Handler for the response is: " + handler);
                    System.out.println("Handler for the response is: " + handler.toString());
                    System.out.println("Teh final message is: " + message);
                    if (handler.toString().contains("Protobuf")) {
                        System.out.println("Trying protobuf");
                        System.out.println("finale message inside protobuf is: " + message);
                        System.out.println("finale message inside protobuf is: " + message.getContentLength());
                        final CodedInputStream streamInput;
                        if (message.getContentLength() > 0 || header.getVersion().equals(Version.CURRENT) == false) {
                            System.out.println("Message content length is greater than 0 or header version is not current");
                            StreamInput streamInput1 = namedWriteableStream(message.openOrGetStreamInput());
                            streamInput = CodedInputStream.newInstance(streamInput1);
                            System.out.println("Protobuf input is: " + streamInput);
                            // assertRemoteVersion(streamInput, header.getVersion());
                            if (header.isError()) {
                                handlerResponseErrorProtobuf(requestId, streamInput, handler);
                            } else {
                                handleResponseProtobuf(requestId, remoteAddress, streamInput, handler);
                            }
                        } else {
                            System.out.println("Protobuf else condition");
                            assert header.isError() == false;
                            handleResponseProtobuf(requestId, remoteAddress, EMPTY_CODED_INPUT_STREAM, handler);
                        }
                    } else {
                        final StreamInput streamInput;
                        System.out.println("Trying stream");
                        if (message.getContentLength() > 0 || header.getVersion().equals(Version.CURRENT) == false) {
                            System.out.println("if condition");
                            streamInput = namedWriteableStream(message.openOrGetStreamInput());
                            System.out.println("Stream input is: " + streamInput);
                            assertRemoteVersion(streamInput, header.getVersion());
                            if (header.isError()) {
                                handlerResponseError(requestId, streamInput, handler);
                            } else {
                                handleResponse(requestId, remoteAddress, streamInput, handler);
                            }
                        } else {
                            System.out.println("else condition");
                            assert header.isError() == false;
                            handleResponse(requestId, remoteAddress, EMPTY_STREAM_INPUT, handler);
                        }
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

    private <T extends TransportRequest> void handleRequestProtobuf(TcpChannel channel, org.opensearch.server.proto.OutboundMessageProto.OutboundMsg.Header header, ProtobufOutboundMessage message) throws IOException {
        OutboundMsg outboundMsg = message.getMessage();
        final String action = outboundMsg.getAction();
        final long requestId = header.getRequestId();
        final Version version = Version.fromString(outboundMsg.getVersion());
        System.out.println("InboundHandler.handleRequestProtobuf");
        System.out.println("Header: " + header);
        System.out.println("Action: " + action);
        System.out.println("RequestId: " + requestId);
        System.out.println("Version: " + version);
        System.out.println("Message: " + outboundMsg);
        final TransportChannel transportChannel = new TcpTransportChannel(
            outboundHandler,
            channel,
            action,
            requestId,
            version,
            outboundMsg.getFeaturesList().stream().map(String::valueOf).collect(Collectors.toSet()),
            false,
            false,
            () -> {}
        );
        try {
            messageListener.onRequestReceived(requestId, action);
            final ProtobufRequestHandlerRegistry<T> reg = protobufRequestHandlers.getHandler(action);
            assert reg != null;
            if (outboundMsg.hasClusterStateReq()) {
                System.out.println("InboundHandler.handleRequestProtobuf hasClusterStateReq");
                final ClusterStateReq clusterStateReq = outboundMsg.getClusterStateReq();
                System.out.println("ClusterStateReq: " + clusterStateReq);
                ProtobufClusterStateRequest protobufClusterStateRequest = new ProtobufClusterStateRequest(clusterStateReq.toByteArray());
                final T request = (T) protobufClusterStateRequest;
                request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                System.out.println("Transport request: " + request);
                final String executor = reg.getExecutor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    try {
                        reg.processMessageReceived(request, transportChannel);
                    } catch (Exception e) {
                        sendErrorResponse(reg.getAction(), transportChannel, e);
                    }
                } else {
                    threadPool.executor(executor).execute(new ProtobufRequestHandler<>(reg, request, transportChannel));
                }
            } else if (outboundMsg.hasNodesInfoReq()) {
                System.out.println("InboundHandler.handleRequestProtobuf hasNodesInfoReq");
                final NodesInfoReq nodesInfoReq = outboundMsg.getNodesInfoReq();
                System.out.println("NodesInfoReq: " + nodesInfoReq);
                try {
                    ProtobufNodesInfoRequest protobufNodesInfoRequest = new ProtobufNodesInfoRequest(nodesInfoReq.toByteArray());
                    System.out.println("Converted node info request: " + protobufNodesInfoRequest);
                    final T request = (T) new NodeInfoRequest(protobufNodesInfoRequest);
                    request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                    System.out.println("Transport request: " + request);
                    final String executor = reg.getExecutor();
                    if (ThreadPool.Names.SAME.equals(executor)) {
                        try {
                            reg.processMessageReceived(request, transportChannel);
                        } catch (Exception e) {
                            sendErrorResponse(reg.getAction(), transportChannel, e);
                        }
                    } else {
                        System.out.println("In else condition for thread pool");
                        threadPool.executor(executor).execute(new ProtobufRequestHandler<>(reg, request, transportChannel));
                    }
                } catch (Exception e) {
                    System.out.println("Exception: " + e);
                }             
            } else if (outboundMsg.hasNodesStatsReq()) {
                System.out.println("InboundHandler.handleRequestProtobuf hasNodesStatsReq");
                final NodesStatsReq nodesStatsReq = outboundMsg.getNodesStatsReq();
                System.out.println("NodesStatsReq: " + nodesStatsReq);
                try {
                    ProtobufNodesStatsRequest protobufNodesStatsRequest = new ProtobufNodesStatsRequest(nodesStatsReq.toByteArray());
                    System.out.println("Converted node stats request: " + protobufNodesStatsRequest);
                    final T request = (T) new NodeStatsRequest(protobufNodesStatsRequest);
                    request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                    System.out.println("Transport request: " + request);
                    final String executor = reg.getExecutor();
                    if (ThreadPool.Names.SAME.equals(executor)) {
                        try {
                            reg.processMessageReceived(request, transportChannel);
                        } catch (Exception e) {
                            sendErrorResponse(reg.getAction(), transportChannel, e);
                        }
                    } else {
                        System.out.println("In else condition for thread pool");
                        threadPool.executor(executor).execute(new ProtobufRequestHandler<>(reg, request, transportChannel));
                    }
                } catch (Exception e) {
                    System.out.println("Exception: " + e);
                }             
            }
        } catch (Exception e) {
            sendErrorResponse(action, transportChannel, e);
        }
    }

    private <T extends TransportRequest> void handleRequest(TcpChannel channel, Header header, InboundMessage message) throws IOException {
        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final Version version = header.getVersion();
        System.out.println("InboundHandler.handleRequest");
        System.out.println("Header: " + header);
        System.out.println("Action: " + action);
        System.out.println("RequestId: " + requestId);
        System.out.println("Version: " + version);
        System.out.println("Message: " + message);
        if (header.isHandshake()) {
            System.out.println("InboundHandler.handleRequest isHandshake");
            messageListener.onRequestReceived(requestId, action);
            // Cannot short circuit handshakes
            assert message.isShortCircuit() == false;
            final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
            assertRemoteVersion(stream, header.getVersion());
            final TransportChannel transportChannel = new TcpTransportChannel(
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
            try {
                handshaker.handleHandshake(transportChannel, requestId, stream);
            } catch (Exception e) {
                if (Version.CURRENT.isCompatible(header.getVersion())) {
                    sendErrorResponse(action, transportChannel, e);
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
            System.out.println("InboundHandler.handleRequest else isNotHandshake");
            final TransportChannel transportChannel = new TcpTransportChannel(
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
            try {
                messageListener.onRequestReceived(requestId, action);
                if (message.isShortCircuit()) {
                    sendErrorResponse(action, transportChannel, message.getException());
                } else {
                    try {
                        final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                        assertRemoteVersion(stream, header.getVersion());
                        final RequestHandlerRegistry<T> reg = requestHandlers.getHandler(action);
                        assert reg != null;

                        final T request = newRequest(requestId, action, stream, reg);
                        request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                        checkStreamIsFullyConsumed(requestId, action, stream);

                        System.out.println("Deserialized request: " + request);
                        final String executor = reg.getExecutor();
                        if (ThreadPool.Names.SAME.equals(executor)) {
                            try {
                                reg.processMessageReceived(request, transportChannel);
                            } catch (Exception e) {
                                sendErrorResponse(reg.getAction(), transportChannel, e);
                            }
                        } else {
                            System.out.println("In else condition for thread pool normal");
                            threadPool.executor(executor).execute(new RequestHandler<>(reg, request, transportChannel));
                        }
                    } catch (Exception e) {
                        System.out.println("Coming in here to try protobuf");
                        // final CodedInputStream stream = message.openOrGetProtobufCodedInput();
                        // final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                        // assertRemoteVersion(stream, header.getVersion());
                        final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                        CodedInputStream codedInputStream = CodedInputStream.newInstance(stream);
                        final ProtobufRequestHandlerRegistry<T> reg = protobufRequestHandlers.getHandler(action);
                        assert reg != null;

                        final T request = newRequestProtobuf(requestId, action, codedInputStream, reg);
                        request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                        // checkStreamIsFullyConsumed(requestId, action, stream);

                        System.out.println("Deserialized request: " + request);
                        final String executor = reg.getExecutor();
                        if (ThreadPool.Names.SAME.equals(executor)) {
                            try {
                                reg.processMessageReceived(request, transportChannel);
                            } catch (Exception exp) {
                                sendErrorResponse(reg.getAction(), transportChannel, exp);
                            }
                        } else {
                            // threadPool.executor(executor).execute(new RequestHandler<>(reg, request, transportChannel));
                        }
                    }
                }
            } catch (Exception e) {
                sendErrorResponse(action, transportChannel, e);
            }
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

    private <T extends TransportRequest> T newRequestProtobuf(
        final long requestId,
        final String action,
        final CodedInputStream stream,
        final ProtobufRequestHandlerRegistry<T> reg
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
        System.out.println("handleResponse");
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

    private <T extends TransportResponse> void handleProtobufResponse(
        final long requestId,
        InetSocketAddress remoteAddress,
        final ProtobufOutboundMessage message,
        final TransportResponseHandler<T> handler
    ) throws IOException {
        System.out.println("handleProtobufResponse");
        try {
            OutboundMsg outboundMsg = message.getMessage();
            if (outboundMsg.hasClusterStateRes()) {
                System.out.println("InboundHandler.handleProtobufResponse hasClusterStateRes");
                final ClusterStateRes clusterStateRes = outboundMsg.getClusterStateRes();
                System.out.println("ClusterStateRes: " + clusterStateRes);
                ProtobufClusterStateResponse protobufClusterStateResponse = new ProtobufClusterStateResponse(clusterStateRes.toByteArray());
                final T response = (T) protobufClusterStateResponse;
                response.remoteAddress(new TransportAddress(remoteAddress));
                System.out.println("Transport response: " + response);

                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    doHandleResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> doHandleResponse(handler, response));
                }
            } else if (outboundMsg.hasNodesInfoRes()) {
                System.out.println("InboundHandler.handleProtobufResponse hasNodesInfoRes");
                final NodesInfo nodesInfoRes = outboundMsg.getNodesInfoRes();
                System.out.println("NodesInfoRes: " + nodesInfoRes);
                ProtobufNodeInfo protobufNodeInfo = new ProtobufNodeInfo(nodesInfoRes.toByteArray());
                final T response = (T) protobufNodeInfo;
                response.remoteAddress(new TransportAddress(remoteAddress));
                System.out.println("Transport nodes response: " + response);

                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    doHandleResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> doHandleResponse(handler, response));
                }
            } else if (outboundMsg.hasNodesStatsRes()) {
                System.out.println("InboundHandler.handleProtobufResponse hasNodesStatsRes");
                final NodesStats nodesStatsRes = outboundMsg.getNodesStatsRes();
                System.out.println("NodesStatsRes: " + nodesStatsRes);
                ProtobufNodeStats protobufNodeStats = new ProtobufNodeStats(nodesStatsRes.toByteArray());
                final T response = (T) protobufNodeStats;
                response.remoteAddress(new TransportAddress(remoteAddress));
                System.out.println("Transport nodes stats response: " + response);

                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    doHandleResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> doHandleResponse(handler, response));
                }
            }
        } catch (Exception e) {
            final Exception serializationException = new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler + "]",
                e
            );
            logger.warn(new ParameterizedMessage("Failed to deserialize response from [{}]", remoteAddress), serializationException);
            handleException(handler, serializationException);
            return;
        }
    }

    private <T extends TransportResponse> void handleResponseProtobuf(
        final long requestId,
        InetSocketAddress remoteAddress,
        final CodedInputStream stream,
        final TransportResponseHandler<T> handler
    ) throws IOException {
        System.out.println("handleResponseProtobuf");
        System.out.println("seeing if the stream has something");
        System.out.println(stream.readTag());
        final T response;
        try {
            response = handler.read(stream);
            response.remoteAddress(new TransportAddress(remoteAddress));
            // checkStreamIsFullyConsumed(requestId, handler, stream, false);
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

    private void handlerResponseErrorProtobuf(final long requestId, CodedInputStream stream, final TransportResponseHandler<?> handler) {
        Exception error;
        try {
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(stream);
            error = protobufStreamInput.readException();
            // checkStreamIsFullyConsumed(requestId, handler, stream, true);
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

    /**
     * Internal request handler
     *
     * @opensearch.internal
     */
    private static class ProtobufRequestHandler<T extends TransportRequest> extends AbstractRunnable {
        private final ProtobufRequestHandlerRegistry<T> reg;
        private final T request;
        private final TransportChannel transportChannel;

        ProtobufRequestHandler(ProtobufRequestHandlerRegistry<T> reg, T request, TransportChannel transportChannel) {
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
}
