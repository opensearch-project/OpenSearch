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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.InboundPipeline;
import org.opensearch.transport.Transport;

/**
 * FlightProducer implementation for handling Arrow Flight requests.
 */
public class ArrowFlightProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final FlightTransport flightTransport;
    private final ThreadPool threadPool;
    private final Transport.RequestHandlers requestHandlers;
    private static final Logger logger = LogManager.getLogger(ArrowFlightProducer.class);
    private final FlightServerMiddleware.Key<ServerHeaderMiddleware> middlewareKey;

    public ArrowFlightProducer(FlightTransport flightTransport, BufferAllocator allocator, FlightServerMiddleware.Key<ServerHeaderMiddleware> middlewareKey) {
        this.threadPool = flightTransport.getThreadPool();
        this.requestHandlers = flightTransport.getRequestHandlers();
        this.flightTransport = flightTransport;
        this.middlewareKey = middlewareKey;
        this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            FlightServerChannel channel = new FlightServerChannel(listener, allocator, context.getMiddleware(middlewareKey));
            listener.setUseZeroCopy(true);
            BytesArray buf = new BytesArray(ticket.getBytes());
            InboundPipeline pipeline = new InboundPipeline(
                flightTransport.getVersion(),
                flightTransport.getStatsTracker(),
                flightTransport.getPageCacheRecycler(),
                threadPool::relativeTimeInMillis,
                flightTransport.getInflightBreaker(),
                requestHandlers::getHandler,
                flightTransport::inboundMessage
            );
            // nothing changes in inbound logic, so reusing native transport inbound pipeline
            try (ReleasableBytesReference reference = ReleasableBytesReference.wrap(buf)) {
                pipeline.handleBytes(channel, reference);
            }
        } catch (FlightRuntimeException ex) {
            listener.error(ex);
            throw ex;
        } catch (Exception ex) {
            FlightRuntimeException fre = CallStatus.INTERNAL.withCause(ex).withDescription("Unexpected server error").toRuntimeException();
            listener.error(fre);
            throw fre;
        }
    }
}
