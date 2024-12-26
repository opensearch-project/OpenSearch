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

@ExperimentalApi
public interface IngestionShardConsumer<T extends IngestionShardPointer, M extends Message> extends Closeable {

    @ExperimentalApi
    class ReadResult<T, M> {
        T pointer;
        M message;

        public ReadResult(T pointer, M message) {
            this.pointer = pointer;
            this.message = message;
        }

        public T getPointer() {
            return pointer;
        }

        public M getMessage() {
            return message;
        }
    }

    /**
     * Read the next set of messages from the source
     * @param pointer the pointer to start reading from, inclusive
     * @param maxMessages, the maximum number of messages to read, or -1 for no limit
     * @param timeoutMillis the maximum time to wait for messages
     * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
     * milliseconds
     * @return a list of messages read from the source
     */
    List<ReadResult<T, M>> readNext(T pointer, long maxMessages, int timeoutMillis) throws java.util.concurrent.TimeoutException;

    /**
     * @return the next pointer to read from
     */
    T nextPointer();


    IngestionShardPointer earliestPointer();

    IngestionShardPointer latestPointer();

    int getShardId();
}
