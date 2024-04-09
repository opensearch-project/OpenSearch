/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobufprotocol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.server.proto.QueryFetchSearchResultProto.QueryFetchSearchResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtocolInboundMessage;
import org.opensearch.transport.ProtocolMessageHandler;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.ResponseHandlerFailureTransportException;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportSerializationException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * Protobuf handler for inbound data
 *
 * @opensearch.internal
 */
public class ProtobufMessageHandler implements ProtocolMessageHandler {

    private static final Logger logger = LogManager.getLogger(ProtobufMessageHandler.class);

    private final ThreadPool threadPool;
    private final Transport.ResponseHandlers responseHandlers;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    public ProtobufMessageHandler(ThreadPool threadPool, Transport.ResponseHandlers responseHandlers) {
        this.threadPool = threadPool;
        this.responseHandlers = responseHandlers;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.getMillis();
    }

    @Override
    public void messageReceived(
        TcpChannel channel,
        ProtocolInboundMessage message,
        long startTime,
        long slowLogThresholdMs,
        TransportMessageListener messageListener
    ) throws IOException {
        ProtobufInboundMessage nodeToNodeMessage = (ProtobufInboundMessage) message;
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final org.opensearch.server.proto.NodeToNodeMessageProto.NodeToNodeMessage.Header header = nodeToNodeMessage.getHeader();

        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            final Tuple<Map<String, String>, Map<String, Set<String>>> headers = new Tuple<Map<String, String>, Map<String, Set<String>>>(
                nodeToNodeMessage.getRequestHeaders(),
                nodeToNodeMessage.getResponseHandlers()
            );
            threadContext.setHeaders(headers);
            threadContext.putTransient("_remote_address", remoteAddress);

            long requestId = header.getRequestId();
            TransportResponseHandler<? extends TransportResponse> handler = responseHandlers.onResponseReceived(requestId, messageListener);
            if (handler != null) {
                handleProtobufResponse(requestId, remoteAddress, nodeToNodeMessage, handler);
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

    private <T extends TransportResponse> void handleProtobufResponse(
        final long requestId,
        InetSocketAddress remoteAddress,
        final ProtobufInboundMessage message,
        final TransportResponseHandler<T> handler
    ) throws IOException {
        try {
            org.opensearch.server.proto.NodeToNodeMessageProto.NodeToNodeMessage receivedMessage = message.getMessage();
            if (receivedMessage.hasQueryFetchSearchResult()) {
                final QueryFetchSearchResult queryFetchSearchResult = receivedMessage.getQueryFetchSearchResult();
                org.opensearch.search.fetch.QueryFetchSearchResult queryFetchSearchResult2 =
                    new org.opensearch.search.fetch.QueryFetchSearchResult(queryFetchSearchResult);
                final T response = (T) queryFetchSearchResult2;
                response.remoteAddress(new TransportAddress(remoteAddress));

                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    doHandleResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> doHandleResponse(handler, response));
                }
            } else if (receivedMessage.hasQuerySearchResult()) {
                final org.opensearch.server.proto.QuerySearchResultProto.QuerySearchResult querySearchResult = receivedMessage
                    .getQuerySearchResult();
                QuerySearchResult querySearchResult2 = new QuerySearchResult(querySearchResult);
                final T response = (T) querySearchResult2;
                response.remoteAddress(new TransportAddress(remoteAddress));

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

    private <T extends TransportResponse> void doHandleResponse(TransportResponseHandler<T> handler, T response) {
        try {
            handler.handleResponse(response);
        } catch (Exception e) {
            handleException(handler, new ResponseHandlerFailureTransportException(e));
        }
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
}
