/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.tasks.TaskId;

/**
 * Interface for managing Arrow data streams between producers and consumers.
 * StreamManager handles the registration of producers, stream access control via tickets,
 * and coordinates the lazy initialization of Arrow resources. It ensures proper lifecycle
 * management of streaming resources across distributed nodes.
 *
 * <p>Implementation of this interface should ensure thread-safety and proper resource cleanup.
 * The manager uses tickets as a mechanism to securely transfer stream access rights between
 * producers and consumers.</p>
 */
@ExperimentalApi
public interface StreamManager extends AutoCloseable {

    /**
     * Registers a stream producer and returns a ticket for stream access.
     * The registration stores the producer reference but delays Arrow resource
     * initialization until the first consumer connects.
     *
     * @param producer The StreamProducer that will generate Arrow data
     * @param parentTaskId The TaskId that identifies the parent operation creating this stream
     * @return A StreamTicket that can be used to access the stream
     * @throws IllegalArgumentException if producer is null or parentTaskId is invalid
     */
    StreamTicket registerStream(StreamProducer producer, TaskId parentTaskId);

    /**
     * Creates a stream reader for consuming Arrow data using a valid ticket.
     * This method may trigger lazy initialization of Arrow resources if this is
     * the first access to the stream.
     *
     * @param ticket The StreamTicket obtained from registerStream
     * @return A StreamReader for consuming the Arrow data
     * @throws IllegalArgumentException if the ticket is invalid
     * @throws IllegalStateException if the stream has been cancelled or closed
     */
    StreamReader getStreamReader(StreamTicket ticket);

    /**
     * Gets the StreamTicketFactory instance associated with this StreamManager.
     *
     * @return the StreamTicketFactory instance
     */
    StreamTicketFactory getStreamTicketFactory();
}
