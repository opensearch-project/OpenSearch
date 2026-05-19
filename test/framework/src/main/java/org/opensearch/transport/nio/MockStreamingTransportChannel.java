/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.ProtocolOutboundHandler;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;
import org.opensearch.transport.stream.StreamingTransportChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A mock transport channel that supports streaming responses for testing purposes.
 * This channel extends TcpTransportChannel to provide sendResponseBatch functionality
 *
 * @opensearch.internal
 */
class MockStreamingTransportChannel extends TcpTransportChannel implements StreamingTransportChannel {
    private static final Logger logger = LogManager.getLogger(MockStreamingTransportChannel.class);

    private final AtomicBoolean streamOpen = new AtomicBoolean(true);
    private final Transport.ResponseHandlers responseHandlers;
    private final TransportMessageListener messageListener;
    private final Queue<TransportResponse> bufferedResponses = new ConcurrentLinkedQueue<>();

    public MockStreamingTransportChannel(
        ProtocolOutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        Version version,
        Set<String> features,
        boolean compressResponse,
        boolean isHandshake,
        Releasable breakerRelease,
        Transport.ResponseHandlers responseHandlers,
        TransportMessageListener messageListener
    ) {
        super(outboundHandler, channel, action, requestId, version, features, compressResponse, isHandshake, breakerRelease);
        this.responseHandlers = responseHandlers;
        this.messageListener = messageListener;
    }

    @Override
    public void sendResponseBatch(TransportResponse response) throws StreamException {
        if (!streamOpen.get()) {
            throw new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed for requestId [" + requestId + "]");
        }

        try {
            // Buffer the response for later delivery when stream is completed
            bufferedResponses.add(response);
            logger.debug(
                "Buffered response {} for action[{}] and requestId[{}]. Total buffered: {}",
                response.getClass().getSimpleName(),
                action,
                requestId,
                bufferedResponses.size()
            );
        } catch (Exception e) {
            streamOpen.set(false);
            // Release resources on failure
            release(true);
            throw new StreamException(StreamErrorCode.INTERNAL, "Error buffering response batch", e);
        }
    }

    @Override
    public void completeStream() {
        if (streamOpen.compareAndSet(true, false)) {
            logger.debug(
                "Completing stream for action[{}] and requestId[{}]. Processing {} buffered responses",
                action,
                requestId,
                bufferedResponses.size()
            );

            try {
                // Get the response handler and call handleStreamResponse with all buffered responses
                TransportResponseHandler<?> handler = responseHandlers.onResponseReceived(requestId, messageListener);
                if (handler == null) {
                    throw new StreamException(StreamErrorCode.INTERNAL, "No response handler found for requestId [" + requestId + "]");
                }

                // Create MockStreamTransportResponse with all buffered responses
                List<TransportResponse> responsesCopy = new ArrayList<>(bufferedResponses);
                StreamTransportResponse<TransportResponse> streamResponse = new MockStreamTransportResponse<>(responsesCopy);

                @SuppressWarnings("unchecked")
                TransportResponseHandler<TransportResponse> typedHandler = (TransportResponseHandler<TransportResponse>) handler;
                logger.debug(
                    "Calling handleStreamResponse for action[{}] and requestId[{}] with {} responses",
                    action,
                    requestId,
                    responsesCopy.size()
                );
                typedHandler.handleStreamResponse(streamResponse);
            } catch (Exception e) {
                // Release resources on failure
                release(true);
                throw new StreamException(StreamErrorCode.INTERNAL, "Error completing stream", e);
            } finally {
                // Release circuit breaker resources when stream is completed
                release(false);
            }
        } else {
            logger.warn("CompleteStream called on already closed stream with action[{}] and requestId[{}]", action, requestId);
            throw new StreamException(StreamErrorCode.UNAVAILABLE, "MockStreamingTransportChannel stream already closed.");
        }
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        // For streaming channels, regular sendResponse is not supported
        // Clients should use sendResponseBatch instead
        throw new UnsupportedOperationException(
            "sendResponse() is not supported for streaming requests in MockStreamingTransportChannel. Use sendResponseBatch() instead."
        );
    }

    @Override
    public String getChannelType() {
        return "mock-stream-transport";
    }
}
