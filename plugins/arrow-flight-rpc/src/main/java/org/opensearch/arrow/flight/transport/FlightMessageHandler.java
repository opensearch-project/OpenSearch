/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

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
import org.opensearch.transport.RequestHandlerRegistry;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportHandshaker;
import org.opensearch.transport.TransportKeepAlive;

class FlightMessageHandler extends NativeMessageHandler {

    // The base class keeps its own private copy; retained here so createTcpTransportChannel can look up the
    // per-action outbound buffer threshold to apply to the freshly created FlightServerChannel.
    private final Transport.RequestHandlers requestHandlers;

    public FlightMessageHandler(
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
        this.requestHandlers = requestHandlers;
    }

    @Override
    protected ProtocolOutboundHandler createNativeOutboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler outboundHandler
    ) {
        return new FlightOutboundHandler(nodeName, version, features, statsTracker, threadPool);
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
        // The action is known here, so apply any per-action outbound buffer threshold the handler declared to
        // this stream's channel. Actions that declare none keep the transport-wide default (watermark untouched).
        if (channel instanceof FlightServerChannel flightServerChannel) {
            final RequestHandlerRegistry<?> reg = requestHandlers.getHandler(action);
            if (reg != null) {
                flightServerChannel.setOutboundBufferThreshold(reg.getOutboundBufferThresholdBytes());
            }
        }
        return new FlightTransportChannel(
            (FlightOutboundHandler) outboundHandler,
            channel,
            action,
            requestId,
            version,
            header.getFeatures(),
            header.isCompressed(),
            header.isHandshake(),
            breakerRelease
        );
    }
}
