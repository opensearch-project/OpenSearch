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
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.InboundPipeline;
import org.opensearch.transport.Transport;
import org.opensearch.transport.stream.StreamException;

import java.util.concurrent.ExecutorService;

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
    private final ExecutorService executor;

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
        this.executor = threadPool.executor(ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        ServerHeaderMiddleware middleware = context.getMiddleware(middlewareKey);
        // thread switch is needed to free up grpc thread without delegating it to request handler to do the thread switch.
        // It is also necessary for the cancellation from client to work correctly, the grpc thread which started it must be released
        // https://github.com/apache/arrow/issues/38668
        executor.execute(() -> {
            FlightCallTracker callTracker = statsCollector.createServerCallTracker();
            FlightServerChannel channel = new FlightServerChannel(
                listener,
                allocator,
                middleware,
                callTracker,
                flightTransport.getNextFlightExecutor()
            );
            try {
                BytesArray buf = new BytesArray(ticket.getBytes());
                callTracker.recordRequestBytes(buf.ramBytesUsed());
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
                }
            } catch (StreamException e) {
                FlightRuntimeException flightException = FlightErrorMapper.toFlightException(e);
                listener.error(flightException);
                channel.close();
                throw flightException;
            } catch (FlightRuntimeException ex) {
                listener.error(ex);
                // FlightServerChannel is always closed in FlightTransportChannel at the time of release.
                // we still try to close it here as the FlightServerChannel might not be created when this execution occurs.
                // other times, the close is redundant and harmless as double close is handled gracefully.
                channel.close();
                throw ex;
            } catch (Exception ex) {
                FlightRuntimeException fre = CallStatus.INTERNAL.withCause(ex)
                    .withDescription("Unexpected server error")
                    .toRuntimeException();
                listener.error(fre);
                channel.close();
                throw fre;
            }
        });
    }
}
