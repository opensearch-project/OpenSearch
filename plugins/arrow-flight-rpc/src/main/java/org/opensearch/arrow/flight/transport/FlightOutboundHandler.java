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

import org.apache.arrow.flight.FlightRuntimeException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
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
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Outbound handler for Arrow Flight streaming responses.
 * It must invoke messageListener and relay any exception back to the caller and not supress them
 * @opensearch.internal
 */
class FlightOutboundHandler extends ProtocolOutboundHandler {
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;
    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;

    public FlightOutboundHandler(String nodeName, Version version, String[] features, StatsTracker statsTracker, ThreadPool threadPool) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
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

    public synchronized void sendResponseBatch(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        // TODO add support for compression
        if (!(channel instanceof FlightServerChannel flightChannel)) {
            throw new IllegalStateException("Expected FlightServerChannel, got " + channel.getClass().getName());
        }
        try {
            try (VectorStreamOutput out = new VectorStreamOutput(flightChannel.getAllocator(), flightChannel.getRoot())) {
                response.writeTo(out);
                flightChannel.sendBatch(getHeaderBuffer(requestId, nodeVersion, features), out);
                messageListener.onResponseSent(requestId, action, response);
            }
        } catch (StreamException e) {
            messageListener.onResponseSent(requestId, action, e);
            // Let StreamException propagate as is - it will be converted to FlightRuntimeException at a higher level
            throw e;
        } catch (FlightRuntimeException e) {
            messageListener.onResponseSent(requestId, action, e);
            throw FlightErrorMapper.fromFlightException(e);
        } catch (Exception e) {
            messageListener.onResponseSent(requestId, action, e);
            throw e;
        }
    }

    public void completeStream(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action
    ) {
        if (!(channel instanceof FlightServerChannel flightChannel)) {
            throw new IllegalStateException("Expected FlightServerChannel, got " + channel.getClass().getName());
        }
        try {
            flightChannel.completeStream();
            messageListener.onResponseSent(requestId, action, TransportResponse.Empty.INSTANCE);
        } catch (FlightRuntimeException e) {
            messageListener.onResponseSent(requestId, action, e);
            throw FlightErrorMapper.fromFlightException(e);
        } catch (Exception e) {
            messageListener.onResponseSent(requestId, action, e);
            throw e;
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
        if (!(channel instanceof FlightServerChannel flightServerChannel)) {
            throw new IllegalStateException("Expected FlightServerChannel, got " + channel.getClass().getName());
        }
        try {
            Exception flightError = error;
            if (error instanceof StreamException) {
                flightError = FlightErrorMapper.toFlightException((StreamException) error);
            }
            flightServerChannel.sendError(getHeaderBuffer(requestId, version, features), flightError);
            messageListener.onResponseSent(requestId, action, error);
        } catch (Exception e) {
            messageListener.onResponseSent(requestId, action, e);
            throw e;
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

    private ByteBuffer getHeaderBuffer(long requestId, Version nodeVersion, Set<String> features) throws IOException {
        // Just a way( probably inefficient) to serialize header to reuse existing logic present in
        // NativeOutboundMessage.Response#writeVariableHeader()
        NativeOutboundMessage.Response headerMessage = new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            out -> {},
            Version.min(version, nodeVersion),
            requestId,
            false,
            false
        );
        try (BytesStreamOutput bytesStream = new BytesStreamOutput()) {
            BytesReference headerBytes = headerMessage.serialize(bytesStream);
            return ByteBuffer.wrap(headerBytes.toBytesRef().bytes);
        }
    }
}
