/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.util.List;

/**
 * A consumer for reading messages from an ingestion shard.
 * @param <T> the type of the pointer to the message
 * @param <M> the type of the message
 */
@ExperimentalApi
public interface IngestionShardConsumer<T extends IngestionShardPointer, M extends Message> extends Closeable {

    /**
     * A read result containing the pointer and the message
     * @param <T> the type of the pointer to the message
     * @param <M> the type of the message
     */
    @ExperimentalApi
    class ReadResult<T, M> {
        T pointer;
        M message;

        /**
         * Create a new read result
         * @param pointer the pointer to the message
         * @param message the message
         */
        public ReadResult(T pointer, M message) {
            this.pointer = pointer;
            this.message = message;
        }

        /**
         * @return the pointer to the message
         */
        public T getPointer() {
            return pointer;
        }

        /**
         * @return the message
         */
        public M getMessage() {
            return message;
        }
    }

    /**
     * Read the next set of messages from the source
     * @param pointer the pointer to start reading from,
     * @param includeStart whether to include the start pointer in the read
     * @param maxMessages, the maximum number of messages to read, or -1 for no limit
     * @param timeoutMillis the maximum time to wait for messages
     * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
     * milliseconds
     * @return a list of messages read from the source
     */
    List<ReadResult<T, M>> readNext(T pointer, boolean includeStart, long maxMessages, int timeoutMillis)
        throws java.util.concurrent.TimeoutException;

    /**
     * Read the next set of messages from the source using the previous pointer. An exception is thrown if no previous pointer is available.
     * This method is used as an optimization for consecutive reads.
     * @param maxMessages the maximum number of messages to read, or -1 for no limit
     * @param timeoutMillis the maximum time to wait for messages
     * @return a list of messages read from the source
     * @throws java.util.concurrent.TimeoutException
     */
    List<ReadResult<T, M>> readNext(long maxMessages, int timeoutMillis) throws java.util.concurrent.TimeoutException;

    /**
     * @return the earliest pointer in the shard
     */
    IngestionShardPointer earliestPointer();

    /**
     * @return the latest pointer in the shard. The pointer points to the next offset of the last message in the stream.
     */
    IngestionShardPointer latestPointer();

    /**
     * Returns an ingestion shard pointer based on the provided timestamp in milliseconds.
     *
     * @param timestampMillis the timestamp in milliseconds
     * @return the ingestion shard pointer corresponding to the given timestamp
     */
    IngestionShardPointer pointerFromTimestampMillis(long timestampMillis);

    /**
     * Returns an ingestion shard pointer based on the provided offset.
     *
     * @param offset the offset value
     * @return the ingestion shard pointer corresponding to the given offset
     */
    IngestionShardPointer pointerFromOffset(String offset);

    /**
     * @return the shard id
     */
    int getShardId();
}
