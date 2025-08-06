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
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportHandshaker;
import org.opensearch.transport.TransportKeepAlive;

class FlightMessageHandler extends NativeMessageHandler {

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
