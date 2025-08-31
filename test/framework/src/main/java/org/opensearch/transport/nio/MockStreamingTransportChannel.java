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

            boolean releaseNeeded = true;
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
                
                // Success - release normally
                release(false);
                releaseNeeded = false;
            } catch (Exception e) {
                // Release resources on failure
                release(true);
                releaseNeeded = false;
                throw new StreamException(StreamErrorCode.INTERNAL, "Error completing stream", e);
            } finally {
                // Only release if not already released
                if (releaseNeeded) {
                    release(false);
                }
            }
        } else {
            logger.warn("CompleteStream called on already closed stream with action[{}] and requestId[{}]", action, requestId);
            // Don't throw exception here as stream is already closed
            // This can happen when onFailure calls sendResponse which releases, then completeStream is called
        }
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        // For streaming channels, regular sendResponse is not supported for normal responses
        // But we need to support it for exception responses
        // Call parent's sendResponse which will handle release
        super.sendResponse(response);
    }
    
    @Override  
    public void sendResponse(Exception exception) throws IOException {
        // Mark stream as closed to prevent further operations
        streamOpen.set(false);
        // Call parent's sendResponse which will handle the exception and release
        super.sendResponse(exception);
    }

    @Override
    public String getChannelType() {
        return "mock-stream-transport";
    }
}
