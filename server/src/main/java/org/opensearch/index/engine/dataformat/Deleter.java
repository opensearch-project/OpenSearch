/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.util.Queue;

/**
 * Buffers deletes for one {@link Writer} generation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Deleter extends Closeable {

    /**
     * Returns the generation number of this deleter, matching its paired writer.
     *
     * @return the generation number
     */
    long generation();

    /**
     * Deactivates the deleter and drains its buffered document IDs.
     *
     * @return buffered IDs to apply to the parent writer, or an empty queue if already inactive
     */
    Queue<String> deactivate();

    /**
     * Buffers a document ID for deletion when this generation retires.
     *
     * @param id the document ID
     * @return {@code true} when buffered
     * @throws IllegalStateException if inactive
     */
    boolean recordBufferedDeletes(String id);

    /** Returns whether this deleter accepts deletes. */
    boolean isActive();

    /**
     * Records a row-id delete for application during the paired writer's flush.
     *
     * @param rowId insertion row id within the writer generation
     */
    default void recordPositionalDelete(long rowId) {
        throw new UnsupportedOperationException("Positional delete is not supported by this deleter");
    }

    /**
     * Returns heap used by this deleter's own buffered state. State owned by the paired writer is
     * accounted by that writer.
     *
     * @return estimated bytes
     */
    default long ramBytesUsed() {
        return 0L;
    }
}
