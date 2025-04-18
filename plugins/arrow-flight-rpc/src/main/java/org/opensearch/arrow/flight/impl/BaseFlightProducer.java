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
import org.apache.arrow.flight.FlightRuntimeException;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * BaseFlightProducer extends NoOpFlightProducer to provide stream management functionality
 * for Arrow Flight in OpenSearch. This class handles data streaming based on tickets,
 * manages backpressure, and coordinates between stream providers and server stream listeners.
 * It runs on the gRPC transport thread.
 * <p>
 * Error handling strategy:
 * 1. Add all errors to listener.
 * 2. All FlightRuntimeException which are not INTERNAL should not be logged.
 * 3. All FlightRuntimeException which are INTERNAL should be logged with error or warn (depending on severity).
 */
public class BaseFlightProducer extends NoOpFlightProducer {
    private static final Logger logger = LogManager.getLogger(BaseFlightProducer.class);
    private final FlightClientManager flightClientManager;
    private final FlightStreamManager streamManager;
    private final BufferAllocator allocator;

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
     * @param context The call context (unused in this implementation)
     * @param ticket The Arrow Flight Ticket containing stream information
     * @param listener The server stream listener for data flow
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            StreamTicket streamTicket = parseTicket(ticket);
            FlightStreamManager.StreamProducerHolder producerHolder = acquireStreamProducer(streamTicket, ticket).orElseThrow(() -> {
                FlightRuntimeException ex = CallStatus.NOT_FOUND.withDescription("Stream not found").toRuntimeException();
                listener.error(ex);
                return ex;
            });
            processStreamWithProducer(context, producerHolder, listener);
        } catch (FlightRuntimeException ex) {
            listener.error(ex);
            throw ex;
        } catch (Exception ex) {
            logger.error("Unexpected error during stream processing", ex);
            FlightRuntimeException fre = CallStatus.INTERNAL.withCause(ex).withDescription("Unexpected server error").toRuntimeException();
            listener.error(fre);
            throw fre;
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
        StreamTicket streamTicket = parseDescriptor(descriptor);
        return streamTicket.getNodeId().equals(flightClientManager.getLocalNodeId())
            ? getLocalFlightInfo(streamTicket, descriptor)
            : getRemoteFlightInfo(streamTicket, descriptor);
    }

    private StreamTicket parseTicket(Ticket ticket) {
        try {
            return streamManager.getStreamTicketFactory().fromBytes(ticket.getBytes());
        } catch (Exception e) {
            logger.debug("Failed to parse Arrow Flight Ticket", e);
            throw CallStatus.INVALID_ARGUMENT.withCause(e).withDescription("Invalid ticket format: " + e.getMessage()).toRuntimeException();
        }
    }

    private StreamTicket parseDescriptor(FlightDescriptor descriptor) {
        try {
            return streamManager.getStreamTicketFactory().fromBytes(descriptor.getCommand());
        } catch (Exception e) {
            logger.debug("Failed to parse flight descriptor command", e);
            throw CallStatus.INVALID_ARGUMENT.withCause(e)
                .withDescription("Invalid descriptor format: " + e.getMessage())
                .toRuntimeException();
        }
    }

    private Optional<FlightStreamManager.StreamProducerHolder> acquireStreamProducer(StreamTicket streamTicket, Ticket ticket) {
        if (streamTicket.getNodeId().equals(flightClientManager.getLocalNodeId())) {
            return streamManager.removeStreamProducer(streamTicket);
        }
        return flightClientManager.getFlightClient(streamTicket.getNodeId())
            .map(client -> createProxyProducer(client, ticket))
            .filter(Optional::isPresent)
            .orElse(Optional.empty());
    }

    private Optional<FlightStreamManager.StreamProducerHolder> createProxyProducer(FlightClient remoteClient, Ticket ticket) {
        try (FlightStream flightStream = remoteClient.getStream(ticket)) {
            return Optional.ofNullable(flightStream)
                .map(fs -> new ProxyStreamProducer(new FlightStreamReader(fs)))
                .map(proxy -> FlightStreamManager.StreamProducerHolder.create(proxy, allocator))
                .or(() -> {
                    logger.warn("Remote client returned null flight stream for ticket");
                    return Optional.empty();
                });
        } catch (Exception e) {
            logger.warn("Failed to create proxy producer for remote stream", e);
            throw CallStatus.INTERNAL.withCause(e).withDescription("Unable to create proxy stream: " + e.getMessage()).toRuntimeException();
        }
    }

    private void processStreamWithProducer(
        CallContext context,
        FlightStreamManager.StreamProducerHolder producerHolder,
        ServerStreamListener listener
    ) throws IOException {
        try (StreamProducer<VectorSchemaRoot, BufferAllocator> producer = producerHolder.producer()) {
            StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob = producer.createJob(allocator);
            if (context.isCancelled()) {
                handleCancellation(batchedJob, listener);
                return;
            }
            processStream(producerHolder, batchedJob, listener);
        }
    }

    private void processStream(
        FlightStreamManager.StreamProducerHolder producerHolder,
        StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob,
        ServerStreamListener listener
    ) {
        BackpressureStrategy backpressureStrategy = new CustomCallbackBackpressureStrategy(null, batchedJob::onCancel);
        backpressureStrategy.register(listener);
        StreamProducer.FlushSignal flushSignal = createFlushSignal(batchedJob, listener, backpressureStrategy);

        try (VectorSchemaRoot root = producerHolder.getRoot()) {
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
        return timeout -> {
            BackpressureStrategy.WaitResult result = backpressureStrategy.waitForListener(timeout.millis());
            switch (result) {
                case READY:
                    listener.putNext();
                    break;
                case TIMEOUT:
                    batchedJob.onCancel();
                    throw CallStatus.TIMED_OUT.withDescription("Stream deadline exceeded").toRuntimeException();
                case CANCELLED:
                    batchedJob.onCancel();
                    throw CallStatus.CANCELLED.withDescription("Stream cancelled by client").toRuntimeException();
                default:
                    batchedJob.onCancel();
                    logger.error("Unexpected backpressure result: {}", result);
                    throw CallStatus.INTERNAL.withDescription("Unexpected backpressure error: " + result).toRuntimeException();
            }
        };
    }

    private void handleCancellation(StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob, ServerStreamListener listener) {
        try {
            batchedJob.onCancel();
            throw CallStatus.CANCELLED.withDescription("Stream cancelled before processing").toRuntimeException();
        } catch (Exception e) {
            logger.error("Unexpected error during cancellation", e);
            throw CallStatus.INTERNAL.withCause(e).withDescription("Error during cancellation: " + e.getMessage()).toRuntimeException();
        }
    }

    private FlightInfo getLocalFlightInfo(StreamTicket streamTicket, FlightDescriptor descriptor) {
        FlightStreamManager.StreamProducerHolder producerHolder = streamManager.getStreamProducer(streamTicket).orElseThrow(() -> {
            logger.debug("FlightInfo not found for ticket: {}", streamTicket);
            return CallStatus.NOT_FOUND.withDescription("FlightInfo not found").toRuntimeException();
        });

        Location location = flightClientManager.getFlightClientLocation(streamTicket.getNodeId()).orElseThrow(() -> {
            logger.warn("Failed to determine location for node: {}", streamTicket.getNodeId());
            return CallStatus.INTERNAL.withDescription("Internal error determining location").toRuntimeException();
        });

        try {
            Ticket ticket = new Ticket(descriptor.getCommand());
            var schema = producerHolder.getRoot().getSchema();
            FlightEndpoint endpoint = new FlightEndpoint(ticket, location);
            return FlightInfo.builder(schema, descriptor, Collections.singletonList(endpoint))
                .setRecords(producerHolder.producer().estimatedRowCount())
                .build();
        } catch (Exception e) {
            logger.error("Failed to build FlightInfo", e);
            throw CallStatus.INTERNAL.withCause(e).withDescription("Error creating FlightInfo: " + e.getMessage()).toRuntimeException();
        }
    }

    private FlightInfo getRemoteFlightInfo(StreamTicket streamTicket, FlightDescriptor descriptor) {
        FlightClient remoteClient = flightClientManager.getFlightClient(streamTicket.getNodeId()).orElseThrow(() -> {
            logger.warn("No remote client available for node: {}", streamTicket.getNodeId());
            return CallStatus.INTERNAL.withDescription("Client doesn't support Stream").toRuntimeException();
        });

        try {
            return remoteClient.getInfo(descriptor);
        } catch (Exception e) {
            logger.error("Failed to get remote FlightInfo", e);
            throw CallStatus.INTERNAL.withCause(e)
                .withDescription("Error retrieving remote FlightInfo: " + e.getMessage())
                .toRuntimeException();
        }
    }
}
