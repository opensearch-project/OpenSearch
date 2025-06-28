/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.InboundPipeline;
import org.opensearch.transport.Transport;

/**
 * FlightProducer implementation for handling Arrow Flight requests.
 */
class ArrowFlightProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final FlightTransport flightTransport;
    private final ThreadPool threadPool;
    private final Transport.RequestHandlers requestHandlers;
    private final FlightServerMiddleware.Key<ServerHeaderMiddleware> middlewareKey;
    private final FlightStatsCollector statsCollector;

    public ArrowFlightProducer(
        FlightTransport flightTransport,
        BufferAllocator allocator,
        FlightServerMiddleware.Key<ServerHeaderMiddleware> middlewareKey,
        FlightStatsCollector statsCollector
    ) {
        this.threadPool = flightTransport.getThreadPool();
        this.requestHandlers = flightTransport.getRequestHandlers();
        this.flightTransport = flightTransport;
        this.middlewareKey = middlewareKey;
        this.allocator = allocator;
        this.statsCollector = statsCollector;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        long startTime = System.nanoTime();
        try {
            FlightServerChannel channel = new FlightServerChannel(
                listener,
                allocator,
                context.getMiddleware(middlewareKey),
                statsCollector
            );
            BytesArray buf = new BytesArray(ticket.getBytes());

            // Track server-side inbound request stats
            if (statsCollector != null) {
                statsCollector.incrementServerRequestsReceived();
                statsCollector.incrementServerRequestsCurrent();
                statsCollector.addBytesReceived(buf.length());
            }

            // TODO: check the feasibility of create InboundPipeline once
            try (
                InboundPipeline pipeline = new InboundPipeline(
                    flightTransport.getVersion(),
                    flightTransport.getStatsTracker(),
                    flightTransport.getPageCacheRecycler(),
                    threadPool::relativeTimeInMillis,
                    flightTransport.getInflightBreaker(),
                    requestHandlers::getHandler,
                    flightTransport::inboundMessage
                );
                ReleasableBytesReference reference = ReleasableBytesReference.wrap(buf)
            ) {
                // nothing changes in inbound logic, so reusing native transport inbound pipeline
                pipeline.handleBytes(channel, reference);

                // Request timing is now tracked in FlightServerChannel from start to completion
            }
        } catch (FlightRuntimeException ex) {
            if (statsCollector != null) {
                statsCollector.incrementFlightServerErrors();
                statsCollector.incrementStreamsFailed();
                statsCollector.decrementServerRequestsCurrent();
            }
            listener.error(ex);
            throw ex;
        } catch (Exception ex) {
            if (statsCollector != null) {
                statsCollector.incrementSerializationErrors();
                statsCollector.incrementStreamsFailed();
                statsCollector.decrementServerRequestsCurrent();
            }
            FlightRuntimeException fre = CallStatus.INTERNAL.withCause(ex).withDescription("Unexpected server error").toRuntimeException();
            listener.error(fre);
            throw fre;
        }
    }
}
