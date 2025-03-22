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
 * for Arrow Flight in OpenSearch. This class handles the retrieval and streaming of data
 * based on provided tickets, managing backpressure, and coordinating between the stream
 * provider and the server stream listener.
 */
public class BaseFlightProducer extends NoOpFlightProducer {
    private final FlightClientManager flightClientManager;
    private final FlightStreamManager streamManager;
    private final BufferAllocator allocator;
    private static final Logger logger = LogManager.getLogger(BaseFlightProducer.class);

    /**
     * Constructs a new BaseFlightProducer.
     *
     * @param flightClientManager The FlightClientManager to handle client connections.
     * @param streamManager The StreamManager to handle stream operations, including
     *                      retrieving and removing streams based on tickets.
     * @param allocator The BufferAllocator for memory management in Arrow operations.
     */
    public BaseFlightProducer(FlightClientManager flightClientManager, FlightStreamManager streamManager, BufferAllocator allocator) {
        this.flightClientManager = flightClientManager;
        this.streamManager = streamManager;
        this.allocator = allocator;
    }

    /**
     * Handles the retrieval and streaming of data based on the provided ticket.
     * This method orchestrates the entire process of setting up the stream,
     * managing backpressure, and handling data flow to the client.
     *
     * @param context The call context (unused in this implementation)
     * @param ticket The ticket containing stream information
     * @param listener The server stream listener to handle the data flow
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        StreamTicket streamTicket = streamManager.getStreamTicketFactory().fromBytes(ticket.getBytes());
        FlightStreamManager.StreamProducerHolder streamProducerHolder = null;
        try {
            if (streamTicket.getNodeId().equals(flightClientManager.getLocalNodeId())) {
                streamProducerHolder = streamManager.removeStreamProducer(streamTicket).orElse(null);

            } else {
                Optional<FlightClient> remoteClient = flightClientManager.getFlightClient(streamTicket.getNodeId());
                if (remoteClient.isEmpty()) {
                    listener.error(
                        CallStatus.UNAVAILABLE.withDescription("Either server is not up yet or node does not support Streams.").cause()
                    );
                    throw new RuntimeException("Either server is not up yet or node does not support Streams.");
                }
                StreamProducer<VectorSchemaRoot, BufferAllocator> proxyProvider = new ProxyStreamProducer(
                    new FlightStreamReader(remoteClient.get().getStream(ticket))
                );
                streamProducerHolder = FlightStreamManager.StreamProducerHolder.create(proxyProvider, allocator);
            }
            if (streamProducerHolder == null) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Stream not found").toRuntimeException());
                return;
            }
            StreamProducer<VectorSchemaRoot, BufferAllocator> producer = streamProducerHolder.producer();
            StreamProducer.BatchedJob<VectorSchemaRoot> batchedJob = producer.createJob(allocator);
            if (context.isCancelled()) {
                batchedJob.onCancel();
                listener.error(CallStatus.CANCELLED.cause());
                return;
            }
            listener.setOnCancelHandler(batchedJob::onCancel);
            BackpressureStrategy backpressureStrategy = new BaseBackpressureStrategy(null, batchedJob::onCancel);
            backpressureStrategy.register(listener);
            StreamProducer.FlushSignal flushSignal = (timeout) -> {
                BackpressureStrategy.WaitResult result = backpressureStrategy.waitForListener(timeout.millis());
                if (result.equals(BackpressureStrategy.WaitResult.READY)) {
                    listener.putNext();
                } else if (result.equals(BackpressureStrategy.WaitResult.TIMEOUT)) {
                    listener.error(CallStatus.TIMED_OUT.cause());
                    throw new RuntimeException("Stream deadline exceeded for consumption");
                } else if (result.equals(BackpressureStrategy.WaitResult.CANCELLED)) {

                    batchedJob.onCancel();
                    listener.error(CallStatus.CANCELLED.cause());
                    throw new RuntimeException("Stream cancelled by client");
                } else if (result.equals(BackpressureStrategy.WaitResult.OTHER)) {
                    batchedJob.onCancel();
                    listener.error(CallStatus.INTERNAL.toRuntimeException());
                    throw new RuntimeException("Error while waiting for client: " + result);
                } else {
                    batchedJob.onCancel();
                    listener.error(CallStatus.INTERNAL.toRuntimeException());
                    throw new RuntimeException("Error while waiting for client: " + result);
                }
            };
            try (VectorSchemaRoot root = streamProducerHolder.getRoot()) {
                listener.start(root);
                batchedJob.run(root, flushSignal);
            }
            listener.completed();
        } catch (Exception e) {
            listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).withCause(e).cause());
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            if (streamProducerHolder != null) {
                try {
                    if (streamProducerHolder.producer() != null) {
                        streamProducerHolder.producer().close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Retrieves FlightInfo for the given FlightDescriptor, handling both local and remote cases.
     *
     * @param context The call context
     * @param descriptor The FlightDescriptor containing stream information
     * @return FlightInfo for the requested stream
     */
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        // TODO: this api should only be used internally
        StreamTicket streamTicket = streamManager.getStreamTicketFactory().fromBytes(descriptor.getCommand());
        if (streamTicket.getNodeId().equals(flightClientManager.getLocalNodeId())) {
            Optional<FlightStreamManager.StreamProducerHolder> streamProducerHolder = streamManager.getStreamProducer(streamTicket);
            if (streamProducerHolder.isEmpty()) {
                throw CallStatus.NOT_FOUND.withDescription("FlightInfo not found").toRuntimeException();
            }
            Optional<Location> location = flightClientManager.getFlightClientLocation(streamTicket.getNodeId());
            if (location.isEmpty()) {
                throw CallStatus.UNAVAILABLE.withDescription("Internal error while determining location information from ticket.")
                    .toRuntimeException();
            }
            FlightEndpoint endpoint = new FlightEndpoint(new Ticket(descriptor.getCommand()), location.get());
            FlightInfo.Builder infoBuilder;
            try {
                infoBuilder = FlightInfo.builder(
                    streamProducerHolder.get().getRoot().getSchema(),
                    descriptor,
                    Collections.singletonList(endpoint)
                ).setRecords(streamProducerHolder.get().producer().estimatedRowCount());
            } catch (Exception e) {
                throw CallStatus.INTERNAL.withDescription("Internal error while creating VectorSchemaRoot.").toRuntimeException();
            }
            return infoBuilder.build();
        } else {
            Optional<FlightClient> remoteClient = flightClientManager.getFlightClient(streamTicket.getNodeId());
            if (remoteClient.isEmpty()) {
                throw CallStatus.UNAVAILABLE.withDescription("Client doesn't support Stream").toRuntimeException();
            }
            return remoteClient.get().getInfo(descriptor);
        }
    }
}
