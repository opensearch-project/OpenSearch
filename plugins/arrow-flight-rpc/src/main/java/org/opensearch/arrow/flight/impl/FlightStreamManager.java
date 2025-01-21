/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.arrow.spi.StreamTicketFactory;
import org.opensearch.common.SetOnce;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

import java.util.function.Supplier;

/**
 * FlightStreamManager is a concrete implementation of StreamManager that provides
 * an abstraction layer for managing Arrow Flight streams in OpenSearch.
 * It encapsulates the details of Flight client operations, allowing consumers to
 * work with streams without direct exposure to Flight internals.
 */
public class FlightStreamManager implements StreamManager {

    private FlightStreamTicketFactory ticketFactory;
    private FlightClientManager clientManager;
    private final Supplier<BufferAllocator> allocatorSupplier;
    private final Cache<String, StreamProducerHolder> streamProducers;
    private static final TimeValue expireAfter = TimeValue.timeValueMinutes(2);
    private static final long MAX_PRODUCERS = 10000;

    /**
     * Constructs a new FlightStreamManager.
     * @param allocatorSupplier The supplier for BufferAllocator instances used for memory management.
     *                          This parameter is required to be non-null.

     */
    public FlightStreamManager(Supplier<BufferAllocator> allocatorSupplier) {
        this.allocatorSupplier = allocatorSupplier;
        this.streamProducers = CacheBuilder.<String, StreamProducerHolder>builder()
            .setExpireAfterWrite(expireAfter)
            .setMaximumWeight(MAX_PRODUCERS)
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
        StreamTicket ticket = ticketFactory.newTicket();
        streamProducers.put(ticket.getTicketId(), new StreamProducerHolder(provider, allocatorSupplier.get()));
        return ticket;
    }

    /**
     * Retrieves a StreamReader for the given StreamTicket.
     * @param ticket The StreamTicket representing the stream to retrieve.
     * @return A StreamReader instance for the specified stream.
     */
    @Override
    public StreamReader getStreamReader(StreamTicket ticket) {
        FlightStream stream = clientManager.getFlightClient(ticket.getNodeId()).getStream(new Ticket(ticket.toBytes()));
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
     * Retrieves the ArrowStreamProvider associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The ArrowStreamProvider associated with the ticket, or null if not found.
     */
    public StreamProducerHolder getStreamProducer(StreamTicket ticket) {
        return streamProducers.get(ticket.getTicketId());
    }

    /**
     * Removes the StreamProducer with the given StreamTicket.
     *
     * @param ticket The StreamTicket against StreamProducer to remove.
     */
    public void removeStreamProducer(StreamTicket ticket) {
        streamProducers.invalidate(ticket.getTicketId());
    }

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    public void close() {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streamProducers.invalidateAll();
    }

    /**
     * Holds a StreamProducer and its associated VectorSchemaRoot.
     */
    public static class StreamProducerHolder {
        private final StreamProducer producer;
        private final SetOnce<VectorSchemaRoot> root;
        private final BufferAllocator allocator;

        /**
         * Constructs a new StreamProducerHolder.
         *
         * @param producer The StreamProducer instance.
         * @param allocator The BufferAllocator to use for creating the VectorSchemaRoot.
         */
        public StreamProducerHolder(StreamProducer producer, BufferAllocator allocator) {
            this.producer = producer;
            this.allocator = allocator;
            this.root = new SetOnce<>();
        }

        /**
         * Gets the StreamProducer instance.
         */
        public StreamProducer getProducer() {
            return producer;
        }

        /**
         * Gets the VectorSchemaRoot associated with the StreamProducer.
         * If the root is not set, it creates a new one using the provided BufferAllocator.
         */
        public VectorSchemaRoot getRoot() {
            root.trySet(producer.createRoot(allocator));
            return root.get();
        }
    }
}
