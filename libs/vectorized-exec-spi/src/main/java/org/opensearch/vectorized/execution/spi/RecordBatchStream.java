/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.spi;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a stream of record batches from a DataFusion query execution.
 * This interface provides access to query results in a streaming fashion.
 */
public interface RecordBatchStream extends AutoCloseable {

    /**
     * Check if there are more record batches available in the stream.
     *
     * @return true if more batches are available, false otherwise
     */
    boolean hasNext();

    /**
     * Get the schema of the record batches in this stream.
     * @return the schema object
     */
    Object getSchema();

    /**
     * Get the next record batch from the stream.
     *
     * @return the next record batch as a byte array, or null if no more batches
     */
    CompletableFuture<Object> next();

    /**
     * Close the stream and free associated resources.
     */
    @Override
    void close();
}
