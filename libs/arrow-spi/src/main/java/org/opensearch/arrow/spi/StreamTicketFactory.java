/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Factory interface for creating and managing StreamTicket instances.
 * This factory provides methods to create and deserialize StreamTickets,
 * ensuring consistent ticket creation.
 */
@ExperimentalApi
public interface StreamTicketFactory {
    /**
     * Creates a new StreamTicket
     *
     * @return A new StreamTicket instance
     */
    StreamTicket newTicket();

    /**
     * Deserializes a StreamTicket from its byte representation.
     *
     * @param bytes The byte array containing the serialized ticket data
     * @return A StreamTicket instance reconstructed from the byte array
     * @throws IllegalArgumentException if bytes is null or invalid
     */
    StreamTicket fromBytes(byte[] bytes);
}
