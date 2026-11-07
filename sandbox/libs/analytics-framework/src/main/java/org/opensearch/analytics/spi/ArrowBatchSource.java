/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Source for one sequential Arrow input stream.
 *
 * <p>Implementations return ownership of every non-null batch to the caller. A null
 * batch signals EOF. Sources are single-consumer and close must be idempotent.
 * Implementations that can block should override {@link #cancel()} so a concurrent
 * cancellation request can make {@link #nextBatch()} return promptly.
 *
 * @opensearch.internal
 */
public interface ArrowBatchSource extends AutoCloseable {

    /** Allocator that owns returned batches and exported Arrow C Data buffers. */
    BufferAllocator allocator();

    /** Returns the next owned batch, or {@code null} at EOF. */
    VectorSchemaRoot nextBatch() throws Exception;

    /**
     * Requests cooperative cancellation.
     *
     * <p>This method can run concurrently with {@link #nextBatch()}. Implementations must
     * return promptly and must not close resources still in use by that call. The default
     * is a no-op for compatibility; such sources can delay release until a pending {@link #nextBatch()} call returns.
     */
    default void cancel() {}

    @Override
    void close();
}
