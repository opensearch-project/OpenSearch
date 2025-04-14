/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.arrow.spi.StreamTicketFactory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Default implementation of StreamTicketFactory
 */
@ExperimentalApi
public class FlightStreamTicketFactory implements StreamTicketFactory {

    private final Supplier<String> nodeId;

    /**
     * Constructs a new DefaultStreamTicketFactory instance.
     *
     * @param nodeId A Supplier that provides the node ID for the StreamTicket
     */
    public FlightStreamTicketFactory(Supplier<String> nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Creates a new StreamTicket with a unique ticket ID.
     *
     * @return A new StreamTicket instance
     */
    @Override
    public StreamTicket newTicket() {
        return new FlightStreamTicket(generateUniqueTicket(), nodeId.get());
    }

    /**
     * Deserializes a StreamTicket from its byte representation.
     *
     * @param bytes The byte array containing the serialized ticket data
     * @return A StreamTicket instance reconstructed from the byte array
     * @throws IllegalArgumentException if bytes is null or invalid
     */
    @Override
    public StreamTicket fromBytes(byte[] bytes) {
        return FlightStreamTicket.fromBytes(bytes);
    }

    private String generateUniqueTicket() {
        return UUID.randomUUID().toString();
    }
}
