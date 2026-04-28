/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nio;

import org.opensearch.Version;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Header;
import org.opensearch.transport.NativeMessageHandler;
import org.opensearch.transport.OutboundHandler;
import org.opensearch.transport.ProtocolOutboundHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportHandshaker;
import org.opensearch.transport.TransportKeepAlive;
import org.opensearch.transport.TransportMessageListener;

import java.util.Set;

/**
 * A message handler that extends NativeMessageHandler to mock streaming transport channels.
 *
 * @opensearch.internal
 */
class MockNativeMessageHandler extends NativeMessageHandler {

    // Actions that require streaming transport channels
    private static final Set<String> STREAMING_ACTIONS = Set.of(
        "indices:data/read/search[phase/query]",
        "indices:data/read/search[phase/fetch/id]",
        "indices:data/read/search[free_context]",
        "indices:data/read/search/stream"
    );

    private final ThreadPool threadPool;
    private final Transport.ResponseHandlers responseHandlers;
    private final TransportMessageListener messageListener;

    public MockNativeMessageHandler(
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
        TransportKeepAlive keepAlive,
        TransportMessageListener messageListener
    ) {
        super(
            nodeName,
            version,
            features,
            statsTracker,
            threadPool,
            bigArrays,
            outboundHandler,
            namedWriteableRegistry,
            handshaker,
            requestHandlers,
            responseHandlers,
            tracer,
            keepAlive
        );
        this.threadPool = threadPool;
        this.responseHandlers = responseHandlers;
        this.messageListener = messageListener;
    }

    @Override
    protected TcpTransportChannel createTcpTransportChannel(
        ProtocolOutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        Version version,
        Header header,
        Releasable breakerRelease
    ) {
        // Determine if this action requires streaming support
        if (requiresStreaming(action)) {
            return new MockStreamingTransportChannel(
                outboundHandler,
                channel,
                action,
                requestId,
                version,
                header.getFeatures(),
                header.isCompressed(),
                header.isHandshake(),
                breakerRelease,
                responseHandlers,
                messageListener
            );
        } else {
            // Use standard TcpTransportChannel for non-streaming actions
            return super.createTcpTransportChannel(outboundHandler, channel, action, requestId, version, header, breakerRelease);
        }
    }

    /**
     * Determines if the given action requires streaming transport channel support.
     *
     * @param action the transport action name
     * @return true if the action requires streaming support, false otherwise
     */
    private boolean requiresStreaming(String action) {
        return STREAMING_ACTIONS.contains(action) || action.contains("stream");
    }
}
