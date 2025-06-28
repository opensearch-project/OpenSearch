/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtocolOutboundHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.nativeprotocol.NativeOutboundMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Outbound handler for Arrow Flight streaming responses.
 *
 * @opensearch.internal
 */
class FlightOutboundHandler extends ProtocolOutboundHandler {
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;
    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final FlightStatsCollector statsCollector;

    public FlightOutboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        FlightStatsCollector statsCollector
    ) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.statsCollector = statsCollector;
    }

    @Override
    public void sendRequest(
        DiscoveryNode node,
        TcpChannel channel,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        Version channelVersion,
        boolean compressRequest,
        boolean isHandshake
    ) throws IOException, TransportException {
        // TODO: Implement request sending if needed
        throw new UnsupportedOperationException("sendRequest not implemented for FlightOutboundHandler");
    }

    @Override
    public void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        throw new UnsupportedOperationException(
            "sendResponse() is not supported for streaming requests in FlightOutboundHandler; use sendResponseBatch()"
        );
    }

    public void sendResponseBatch(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake,
        final ActionListener<Void> listener
    ) {
        if (!(channel instanceof FlightServerChannel flightChannel)) {
            throw new IllegalStateException("Expected FlightServerChannel, got " + channel.getClass().getName());
        }
        try {
            // Create NativeOutboundMessage for headers
            NativeOutboundMessage.Response headerMessage = new NativeOutboundMessage.Response(
                threadPool.getThreadContext(),
                features,
                out -> {},
                Version.min(version, nodeVersion),
                requestId,
                isHandshake,
                compress
            );

            // Serialize headers
            ByteBuffer headerBuffer;
            try (BytesStreamOutput bytesStream = new BytesStreamOutput()) {
                BytesReference headerBytes = headerMessage.serialize(bytesStream);
                headerBuffer = ByteBuffer.wrap(headerBytes.toBytesRef().bytes);
            }

            try (VectorStreamOutput out = new VectorStreamOutput(flightChannel.getAllocator())) {
                response.writeTo(out);
                flightChannel.sendBatch(headerBuffer, out, listener);
                messageListener.onResponseSent(requestId, action, response);

                // Track server outbound response
                if (statsCollector != null) {
                    statsCollector.incrementServerBatchesSent();
                }
            }
        } catch (Exception e) {
            if (statsCollector != null) {
                statsCollector.incrementSerializationErrors();
            }
            listener.onFailure(new TransportException("Failed to send response batch for action [" + action + "]", e));
            messageListener.onResponseSent(requestId, action, e);
        }
    }

    public void completeStream(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final ActionListener<Void> listener
    ) {
        if (!(channel instanceof FlightServerChannel flightChannel)) {
            throw new IllegalStateException("Expected FlightServerChannel, got " + channel.getClass().getName());
        }
        try {
            flightChannel.completeStream(listener);
            // listener.onResponse(null);
            // TODO - do we need to call onResponseSent() for messageListener; its already called for individual batches
            // messageListener.onResponseSent(requestId, action, null);
        } catch (Exception e) {
            listener.onFailure(new TransportException("Failed to complete stream for action [" + action + "]", e));
            messageListener.onResponseSent(requestId, action, e);
        }
    }

    @Override
    public void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final Exception error
    ) throws IOException {
        if (!(channel instanceof FlightServerChannel)) {
            throw new IllegalStateException("Expected FlightServerChannel, got " + channel.getClass().getName());
        }
        NativeOutboundMessage.Response headerMessage = new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            out -> {},
            Version.min(version, nodeVersion),
            requestId,
            false,
            false
        );
        // Serialize headers
        ByteBuffer headerBuffer;
        try (BytesStreamOutput bytesStream = new BytesStreamOutput()) {
            BytesReference headerBytes = headerMessage.serialize(bytesStream);
            headerBuffer = ByteBuffer.wrap(headerBytes.toBytesRef().bytes);
        }
        FlightServerChannel flightChannel = (FlightServerChannel) channel;
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        try {
            flightChannel.sendError(headerBuffer, error, listener);
        } catch (Exception e) {
            listener.onFailure(new TransportException("Failed to send error response for action [" + action + "]", e));
        }
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }
}
