/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.OSFlightClient;
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
import org.opensearch.common.SetOnce;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
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
    private final Supplier<BufferAllocator> allocatorSupplier;
    private final Cache<String, StreamProducerHolder> streamProducers;
    // TODO read from setting
    private static final TimeValue DEFAULT_CACHE_EXPIRE = TimeValue.timeValueMinutes(10); // Maximum cache time

    /**
     * Holds a StreamProducer along with its metadata and resources
     */
    public record StreamProducerHolder(StreamProducer producer, BufferAllocator allocator, long creationTime, SetOnce<
        VectorSchemaRoot> root) {
        public StreamProducerHolder {
            Objects.requireNonNull(producer, "StreamProducer cannot be null");
            Objects.requireNonNull(allocator, "BufferAllocator cannot be null");
        }

        static StreamProducerHolder create(StreamProducer producer, BufferAllocator allocator) {
            return new StreamProducerHolder(producer, allocator, System.currentTimeMillis(), new SetOnce<>());
        }

        boolean isExpired() {
            return System.currentTimeMillis() - creationTime > producer.getJobDeadline().getMillis();
        }

        /**
         * Gets the VectorSchemaRoot associated with the StreamProducer.
         * If the root is not set, it creates a new one using the provided BufferAllocator.
         */
        public VectorSchemaRoot getRoot() throws Exception {
            root.trySet(producer.createRoot(allocator));
            return root.get();
        }
    }

    /**
     * Constructs a new FlightStreamManager.
     * @param allocatorSupplier The supplier for BufferAllocator instances used for memory management.
     *                          This parameter is required to be non-null.

     */
    public FlightStreamManager(Supplier<BufferAllocator> allocatorSupplier) {
        this.allocatorSupplier = allocatorSupplier;
        RemovalListener<String, StreamProducerHolder> onProducerRemoval = (notification) -> {
            String ticketId = notification.getKey();
            StreamProducerHolder holder = notification.getValue();
            if (holder != null) {
                try {
                    holder.producer.close();
                    logger.debug("Closed stream producer for ticket [{}], reason: {}", ticketId, notification.getRemovalReason());
                } catch (Exception e) {
                    logger.warn("Error closing stream producer for ticket [{}]. {}", ticketId, e.getMessage());
                }
            }
        };
        this.streamProducers = CacheBuilder.<String, StreamProducerHolder>builder()
            .setExpireAfterWrite(DEFAULT_CACHE_EXPIRE)
            .removalListener(onProducerRemoval)
            .build();
    }

    /**
     * Sets the FlightClientManager for this FlightStreamManager.
     * @param clientManager The FlightClientManager instance to use for Flight client operations.
     *                      This parameter is required to be non-null.
     */
    public void setClientManager(FlightClientManager clientManager) {
        this.clientManager = clientManager;
        this.ticketFactory = new FlightStreamTicketFactory(clientManager::getLocalNodeId);
    }

    /**
     * Registers a new stream producer with the StreamManager.
     * @param provider The StreamProducer instance to register.
     * @param parentTaskId The parent task ID associated with the stream.
     * @return A StreamTicket representing the registered stream.
     */
    @Override
    public StreamTicket registerStream(StreamProducer provider, TaskId parentTaskId) {
        Objects.requireNonNull(provider, "StreamProducer cannot be null");
        StreamTicket ticket = ticketFactory.newTicket();
        streamProducers.put(ticket.getTicketId(), StreamProducerHolder.create(provider, allocatorSupplier.get()));
        return ticket;
    }

    /**
     * Retrieves a StreamReader for the given StreamTicket.
     * @param ticket The StreamTicket representing the stream to retrieve.
     * @return A StreamReader instance for the specified stream.
     */
    @Override
    public StreamReader getStreamReader(StreamTicket ticket) {
        Optional<OSFlightClient> flightClient = clientManager.getFlightClient(ticket.getNodeId());
        if (flightClient.isEmpty()) {
            throw new RuntimeException("Flight client not found for node [" + ticket.getNodeId() + "].");
        }
        FlightStream stream = flightClient.get().getStream(new Ticket(ticket.toBytes()));
        return new FlightStreamReader(stream);
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
    public Optional<StreamProducerHolder> getStreamProducer(StreamTicket ticket) {
        Objects.requireNonNull(ticket, "StreamTicket cannot be null");
        StreamProducerHolder holder = streamProducers.get(ticket.getTicketId());
        if (holder != null) {
            if (holder.isExpired()) {
                removeStreamProducer(ticket);
                return Optional.empty();
            }
            return Optional.of(holder);
        }
        return Optional.empty();
    }

    /**
     * Gets and removes the StreamProducer associated with a ticket.
     * Ensure that close is called on the StreamProducer after use.
     * @param ticket The StreamTicket identifying the stream
     * @return Optional of StreamProducerHolder containing the producer if found
     */
    public Optional<StreamProducerHolder> removeStreamProducer(StreamTicket ticket) {
        Objects.requireNonNull(ticket, "StreamTicket cannot be null");

        String ticketId = ticket.getTicketId();
        StreamProducerHolder holder = streamProducers.get(ticketId);

        if (holder != null) {
            streamProducers.invalidate(ticketId);
            return Optional.of(holder);
        }
        return Optional.empty();
    }

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    @Override
    public void close() throws Exception {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streamProducers.values().forEach(holder -> {
            try {
                holder.producer().close();
            } catch (IOException e) {
                logger.error("Error closing stream producer, this may cause memory leaks.", e);
            }
        });
        streamProducers.invalidateAll();
    }
}
