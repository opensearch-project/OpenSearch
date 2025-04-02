/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;

import java.util.Collections;
import java.util.Optional;

/**
 * BaseFlightProducer extends NoOpFlightProducer to provide stream management functionality
 * for Arrow Flight in OpenSearch. This class handles data streaming based on tickets,
 * manages backpressure, and coordinates between stream providers and server stream listeners.
 * It runs on the gRPC transport thread.
 */
public class BaseFlightProducer extends NoOpFlightProducer {
    private final FlightClientManager flightClientManager;
    private final FlightStreamManager streamManager;
    private final BufferAllocator allocator;
    private static final Logger logger = LogManager.getLogger(BaseFlightProducer.class);

    /**
     * Constructs a new BaseFlightProducer.
     *
     * @param flightClientManager The manager for handling client connections
     * @param streamManager The manager for stream operations
     * @param allocator The buffer allocator for Arrow memory management
     */
    public BaseFlightProducer(FlightClientManager flightClientManager, FlightStreamManager streamManager, BufferAllocator allocator) {
        this.flightClientManager = flightClientManager;
        this.streamManager = streamManager;
        this.allocator = allocator;
    }

    /**
     * Handles data streaming for a given Arrow Flight Ticket. This method runs on the gRPC transport thread
     * and manages the entire streaming process, including backpressure and error handling.
     * <p>
     * Error Handling Strategy:
     * - Log errors for debugging/investigation when they occur during stream processing
     * - Update listener with error status for client notification
     * - Throw exceptions only for unrecoverable errors that should terminate the stream
     *
     * @param context The call context (unused in this implementation)
     * @param ticket The Arrow Flight Ticket containing stream information
     * @param listener The server stream listener for data flow
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        StreamTicket streamTicket;
        try {
            streamTicket = streamManager.getStreamTicketFactory().fromBytes(ticket.getBytes());
        } catch (Exception e) {
            logger.debug("Failed to parse Arrow Flight Ticket into StreamTicket", e);
            listener.error(
                CallStatus.INVALID_ARGUMENT.withDescription("Invalid ticket format: " + e.getMessage()).withCause(e).toRuntimeException()
            );
            return;
        }
        try {
            FlightStreamManager.StreamProducerHolder streamProducerHolder = acquireStreamProducer(streamTicket, ticket, listener);
            if (streamProducerHolder == null) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Stream not found").toRuntimeException());
                return;
            }
            try (StreamProducer<VectorSchemaRoot, BufferAllocator> producer = streamProducerHolder.producer()) {
                StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob = producer.createJob(allocator);
                if (context.isCancelled()) {
                    handleCancellation(batchedJob, listener);
                    return;
                }
                processStream(streamProducerHolder, batchedJob, context, listener);
            }
        } catch (Exception e) {
            logger.error("Unexpected error during stream processing for ticket: " + streamTicket, e);
            listener.error(
                CallStatus.INTERNAL.withDescription("Internal server error: " + e.getMessage()).withCause(e).toRuntimeException()
            );
        }
    }

    /**
     * Retrieves FlightInfo for a given descriptor, handling both local and remote cases.
     * The descriptor's command is expected to contain a serialized StreamTicket.
     *
     * @param context The call context
     * @param descriptor The flight descriptor containing stream information
     * @return FlightInfo for the requested stream
     * @throws RuntimeException if the requested info cannot be retrieved
     */
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        StreamTicket streamTicket;
        try {
            streamTicket = streamManager.getStreamTicketFactory().fromBytes(descriptor.getCommand());
        } catch (Exception e) {
            logger.debug("Failed to parse flight descriptor command into StreamTicket", e);
            throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid descriptor format: " + e.getMessage())
                .withCause(e)
                .toRuntimeException();
        }

        if (streamTicket.getNodeId().equals(flightClientManager.getLocalNodeId())) {
            return getLocalFlightInfo(streamTicket, descriptor);
        } else {
            return getRemoteFlightInfo(streamTicket, descriptor);
        }
    }

    private FlightStreamManager.StreamProducerHolder acquireStreamProducer(
        StreamTicket streamTicket,
        Ticket ticket,
        ServerStreamListener listener
    ) {
        if (streamTicket.getNodeId().equals(flightClientManager.getLocalNodeId())) {
            return streamManager.removeStreamProducer(streamTicket).orElse(null);
        }
        Optional<FlightClient> remoteClient = flightClientManager.getFlightClient(streamTicket.getNodeId());
        if (remoteClient.isPresent()) {
            try {
                StreamProducer<VectorSchemaRoot, BufferAllocator> proxyProvider = createProxyProducer(remoteClient.get(), ticket, listener);
                if (proxyProvider != null) {
                    return FlightStreamManager.StreamProducerHolder.create(proxyProvider, allocator);
                }
            } catch (Exception e) {
                logger.error("Failed to create stream producer", e);
                listener.error(
                    CallStatus.INTERNAL.withDescription("Failed to create stream producer: " + e.getMessage())
                        .withCause(e)
                        .toRuntimeException()
                );
            }
        }
        return null;
    }

    private StreamProducer<VectorSchemaRoot, BufferAllocator> createProxyProducer(
        FlightClient remoteClient,
        Ticket ticket,
        ServerStreamListener listener
    ) {

        try (FlightStream flightStream = remoteClient.getStream(ticket)) {
            if (flightStream == null) {
                logger.error("Failed to obtain flight stream");
                listener.error(CallStatus.INTERNAL.withDescription("Failed to create remote flight stream").toRuntimeException());
                return null;
            }
            return new ProxyStreamProducer(new FlightStreamReader(flightStream));
        } catch (Exception e) {
            logger.error("Error creating proxy producer", e);
            listener.error(
                CallStatus.INTERNAL.withDescription("Failed to create proxy producer: " + e.getMessage()).withCause(e).toRuntimeException()
            );
            return null;
        }
    }

    private void processStream(
        FlightStreamManager.StreamProducerHolder streamProducerHolder,
        StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob,
        CallContext context,
        ServerStreamListener listener
    ) {
        listener.setOnCancelHandler(batchedJob::onCancel);
        BackpressureStrategy backpressureStrategy = new BaseBackpressureStrategy(null, batchedJob::onCancel);
        backpressureStrategy.register(listener);

        StreamProducer.FlushSignal flushSignal = createFlushSignal(batchedJob, listener, backpressureStrategy);

        try (VectorSchemaRoot root = streamProducerHolder.getRoot()) {
            listener.start(root);
            batchedJob.run(root, flushSignal);
            listener.completed();
        }
    }

    private StreamProducer.FlushSignal createFlushSignal(
        StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob,
        ServerStreamListener listener,
        BackpressureStrategy backpressureStrategy
    ) {
        return (timeout) -> {
            BackpressureStrategy.WaitResult result = backpressureStrategy.waitForListener(timeout.millis());
            switch (result) {
                case READY:
                    listener.putNext();
                    break;
                case TIMEOUT:
                    batchedJob.onCancel();
                    listener.error(CallStatus.TIMED_OUT.withDescription("Stream deadline exceeded").toRuntimeException());
                    break;
                case CANCELLED:
                    batchedJob.onCancel();
                    listener.error(CallStatus.CANCELLED.withDescription("Stream cancelled by client").toRuntimeException());
                    break;
                default:
                    batchedJob.onCancel();
                    logger.error("Unexpected backpressure result: {}", result);
                    listener.error(CallStatus.INTERNAL.withDescription("Error waiting for client: " + result).toRuntimeException());
            }
        };
    }

    private void handleCancellation(StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob, ServerStreamListener listener) {
        try {
            batchedJob.onCancel();
            listener.error(CallStatus.CANCELLED.withDescription("Stream cancelled before processing").toRuntimeException());
        } catch (Exception e) {
            logger.error("Error during cancellation handling", e);
            listener.error(
                CallStatus.INTERNAL.withDescription("Error during cancellation: " + e.getMessage()).withCause(e).toRuntimeException()
            );
        }
    }

    private FlightInfo getLocalFlightInfo(StreamTicket streamTicket, FlightDescriptor descriptor) {
        Optional<FlightStreamManager.StreamProducerHolder> streamProducerHolder = streamManager.getStreamProducer(streamTicket);
        if (streamProducerHolder.isPresent()) {
            Optional<Location> location = flightClientManager.getFlightClientLocation(streamTicket.getNodeId());
            if (location.isPresent()) {
                try {
                    Ticket ticket = new Ticket(descriptor.getCommand());
                    var schema = streamProducerHolder.get().getRoot().getSchema();
                    FlightEndpoint endpoint = new FlightEndpoint(ticket, location.get());
                    FlightInfo.Builder infoBuilder = FlightInfo.builder(schema, descriptor, Collections.singletonList(endpoint))
                        .setRecords(streamProducerHolder.get().producer().estimatedRowCount());
                    return infoBuilder.build();
                } catch (Exception e) {
                    logger.error("Failed to build FlightInfo", e);
                    throw CallStatus.INTERNAL.withDescription("Error creating FlightInfo: " + e.getMessage())
                        .withCause(e)
                        .toRuntimeException();
                }
            } else {
                logger.debug("Failed to determine location for node: {}", streamTicket.getNodeId());
                throw CallStatus.UNAVAILABLE.withDescription("Internal error determining location").toRuntimeException();
            }
        } else {
            logger.debug("FlightInfo not found for ticket: {}", streamTicket);
            throw CallStatus.NOT_FOUND.withDescription("FlightInfo not found").toRuntimeException();
        }
    }

    private FlightInfo getRemoteFlightInfo(StreamTicket streamTicket, FlightDescriptor descriptor) {
        Optional<FlightClient> remoteClient = flightClientManager.getFlightClient(streamTicket.getNodeId());
        if (remoteClient.isPresent()) {
            try {
                return remoteClient.get().getInfo(descriptor);
            } catch (Exception e) {
                logger.error("Failed to get remote FlightInfo", e);
                throw CallStatus.INTERNAL.withDescription("Error retrieving remote FlightInfo: " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException();
            }
        } else {
            logger.warn("No remote client available for node: {}", streamTicket.getNodeId());
            throw CallStatus.UNAVAILABLE.withDescription("Client doesn't support Stream").toRuntimeException();
        }
    }
}
