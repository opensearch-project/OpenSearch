/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
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
    private final InboundPipeline pipeline;

    public ArrowFlightProducer(FlightTransport flightTransport, BufferAllocator allocator) {
        final ThreadPool threadPool = flightTransport.getThreadPool();
        final Transport.RequestHandlers requestHandlers = flightTransport.getRequestHandlers();
        this.pipeline = new InboundPipeline(
            flightTransport.getVersion(),
            flightTransport.getStatsTracker(),
            flightTransport.getPageCacheRecycler(),
            threadPool::relativeTimeInMillis,
            flightTransport.getInflightBreaker(),
            requestHandlers::getHandler,
            flightTransport::inboundMessage
        );
        this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            FlightServerChannel channel = new FlightServerChannel(listener, allocator);
            BytesArray buf = new BytesArray(ticket.getBytes());
            try (ReleasableBytesReference reference = ReleasableBytesReference.wrap(buf)) {
                pipeline.handleBytes(channel, reference);
            }
        } catch (Exception e) {
            listener.error(CallStatus.INTERNAL.withCause(e).withDescription("Failed to process stream" + e).toRuntimeException());
        }
    }
}
