/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.InboundHandler;
import org.opensearch.transport.OutboundHandler;
import org.opensearch.transport.ProtocolMessageHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportHandshaker;
import org.opensearch.transport.TransportKeepAlive;
import org.opensearch.transport.TransportProtocol;

import java.util.Map;

class FlightInboundHandler extends InboundHandler {

    public FlightInboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        TransportKeepAlive keepAlive,
        Transport.RequestHandlers requestHandlers,
        Transport.ResponseHandlers responseHandlers,
        Tracer tracer
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
            keepAlive,
            requestHandlers,
            responseHandlers,
            tracer
        );
    }

    @Override
    protected Map<TransportProtocol, ProtocolMessageHandler> createProtocolMessageHandlers(
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
        return Map.of(
            TransportProtocol.NATIVE,
            new FlightMessageHandler(
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
            )
        );
    }
}
