/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.arrow.spi.StreamTicketFactory;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * FlightStreamManager is a concrete implementation of StreamManager that provides
 * an abstraction layer for managing Arrow Flight streams in OpenSearch.
 * It encapsulates the details of Flight client operations, allowing consumers to
 * work with streams without direct exposure to Flight internals.
 */
public class FlightStreamManager implements StreamManager {
    private static final Logger logger = LogManager.getLogger(FlightStreamManager.class);

    private FlightStreamTicketFactory ticketFactory;
    private FlightClientManager clientManager;
    private Supplier<BufferAllocator> allocatorSupplier;
    private final Cache<String, StreamProducerHolder> streamProducers;

    // Default cache settings (TODO: Make configurable via settings)
    private static final TimeValue DEFAULT_CACHE_EXPIRE = TimeValue.timeValueMinutes(10);
    private static final int MAX_WEIGHT = 1000;

    /**
     * Holds a StreamProducer along with its metadata and resources
     */
    record StreamProducerHolder(StreamProducer<VectorSchemaRoot, BufferAllocator> producer, BufferAllocator allocator, long creationTime,
        AtomicReference<VectorSchemaRoot> root) {
        public StreamProducerHolder {
            Objects.requireNonNull(producer, "StreamProducer cannot be null");
            Objects.requireNonNull(allocator, "BufferAllocator cannot be null");
        }

        static StreamProducerHolder create(StreamProducer<VectorSchemaRoot, BufferAllocator> producer, BufferAllocator allocator) {
            return new StreamProducerHolder(producer, allocator, System.nanoTime(), new AtomicReference<>(null));
        }

        boolean isExpired() {
            return System.nanoTime() - creationTime > producer.getJobDeadline().getNanos();
        }

        /**
         * Gets the VectorSchemaRoot associated with the StreamProducer.
         * If the root is not set, it creates a new one using the provided BufferAllocator.
         */
        public VectorSchemaRoot getRoot() {
            return root.updateAndGet(current -> current != null ? current : producer.createRoot(allocator));
        }
    }

    /**
     * Constructs a new FlightStreamManager.
     */
    public FlightStreamManager() {
        this.streamProducers = CacheBuilder.<String, StreamProducerHolder>builder()
            .setExpireAfterWrite(DEFAULT_CACHE_EXPIRE)
            .setMaximumWeight(MAX_WEIGHT)
            .removalListener(n -> {
                if (n.getRemovalReason() != RemovalReason.EXPLICIT) {
                    try (var unused = n.getValue().producer()) {} catch (IOException e) {
                        logger.error("Error closing stream producer, this may cause memory leaks.", e);
                    }
                }
            })
            .build();
    }

    /**
     * Sets the allocator supplier for this FlightStreamManager.
     * @param allocatorSupplier The supplier for BufferAllocator instances used for memory management.
     *                          This parameter is required to be non-null.
     */
    public void setAllocatorSupplier(Supplier<BufferAllocator> allocatorSupplier) {
        this.allocatorSupplier = Objects.requireNonNull(allocatorSupplier, "Allocator supplier cannot be null");
    }

    /**
     * Sets the FlightClientManager for managing Flight clients.
     *
     * @param clientManager The FlightClientManager instance (must be non-null).
     */
    public void setClientManager(FlightClientManager clientManager) {
        this.clientManager = Objects.requireNonNull(clientManager, "FlightClientManager cannot be null");
        this.ticketFactory = new FlightStreamTicketFactory(clientManager::getLocalNodeId);
    }

    /**
     * Registers a new stream producer with the StreamManager.
     * @param provider The StreamProducer instance to register.
     * @param parentTaskId The parent task ID associated with the stream.
     * @return A StreamTicket representing the registered stream.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <VectorRoot, Allocator> StreamTicket registerStream(StreamProducer<VectorRoot, Allocator> provider, TaskId parentTaskId) {
        StreamTicket ticket = ticketFactory.newTicket();
        try {
            streamProducers.computeIfAbsent(
                ticket.getTicketId(),
                ticketId -> StreamProducerHolder.create(
                    (StreamProducer<VectorSchemaRoot, BufferAllocator>) provider,
                    allocatorSupplier.get()
                )
            );
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return ticket;
    }

    /**
     * Retrieves a StreamReader for the given StreamTicket.
     * @param ticket The StreamTicket representing the stream to retrieve.
     * @return A StreamReader instance for the specified stream.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <VectorRoot> StreamReader<VectorRoot> getStreamReader(StreamTicket ticket) {
        FlightClient flightClient = clientManager.getFlightClient(ticket.getNodeId())
            .orElseThrow(() -> new RuntimeException("Flight client not found for node [" + ticket.getNodeId() + "]."));
        FlightStream stream = flightClient.getStream(new Ticket(ticket.toBytes()));
        return (StreamReader<VectorRoot>) new FlightStreamReader(stream);
    }

    /**
     * Retrieves the StreamTicketFactory used by this StreamManager.
     * @return The StreamTicketFactory instance associated with this StreamManager.
     */
    @Override
    public StreamTicketFactory getStreamTicketFactory() {
        return ticketFactory;
    }

    /**
     * Gets the StreamProducer associated with a ticket if it hasn't expired based on its deadline.
     *
     * @param ticket The StreamTicket identifying the stream
     * @return Optional of StreamProducerHolder containing the producer if found and not expired
     */
    Optional<StreamProducerHolder> getStreamProducer(StreamTicket ticket) {
        String ticketId = ticket.getTicketId();
        StreamProducerHolder holder = streamProducers.get(ticketId);
        if (holder == null) {
            logger.debug("No stream producer found for ticket [{}]", ticketId);
            return Optional.empty();
        }

        if (holder.isExpired()) {
            logger.debug("Stream producer for ticket [{}] has expired", ticketId);
            streamProducers.remove(ticketId);
            return Optional.empty();
        }
        return Optional.of(holder);
    }

    /**
     * Gets and removes the StreamProducer associated with a ticket.
     * Ensure that close is called on the StreamProducer after use.
     * @param ticket The StreamTicket identifying the stream
     * @return Optional of StreamProducerHolder containing the producer if found
     */
    public Optional<StreamProducerHolder> removeStreamProducer(StreamTicket ticket) {
        String ticketId = ticket.getTicketId();
        StreamProducerHolder holder = streamProducers.get(ticketId);
        if (holder == null) {
            return Optional.empty();
        }
        streamProducers.remove(ticketId);
        return Optional.of(holder);
    }

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    @Override
    public void close() throws Exception {
        streamProducers.invalidateAll();
    }
}
