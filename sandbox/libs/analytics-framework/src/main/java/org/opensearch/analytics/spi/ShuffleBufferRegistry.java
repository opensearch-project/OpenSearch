/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Per-node lookup that hands back a {@link ShuffleBufferAccess} for a given
 * {@code (queryId, targetStageId, partitionIndex)} tuple. Backend hash-shuffle scan handlers
 * call {@link #getOrCreate} when their {@code ShuffleScanInstructionNode} apply fires, then
 * {@link ShuffleBufferAccess#awaitReady} on the returned handle.
 *
 * <p>Implemented by analytics-engine's {@code ShuffleBufferManager}; the SPI exposes only the
 * consumer-side surface so backend handlers don't need a compile-time dependency on the
 * engine plugin's internals.
 *
 * @opensearch.internal
 */
public interface ShuffleBufferRegistry {

    /**
     * Returns the buffer for {@code (queryId, targetStageId, partitionIndex)}, creating it if
     * absent. Producer-side transport calls also use this lazy-creation pattern so the
     * consumer's first call here doesn't race the producer's first packet.
     */
    ShuffleBufferAccess getOrCreate(String queryId, int targetStageId, int partitionIndex);

    /**
     * Removes the buffer for {@code (queryId, targetStageId, partitionIndex)} after its consuming
     * worker task has drained it. The buffer holds the partition's shuffled payload as on-heap
     * {@code byte[]} lists; without removal these accumulate for the JVM's lifetime across queries
     * and eventually OOM the node. Called from the consumer fragment's terminal path (success or
     * failure). Idempotent — a no-op when the buffer is absent.
     */
    void removeBuffer(String queryId, int targetStageId, int partitionIndex);

    /**
     * Removes ALL buffers belonging to {@code queryId} (every stage / partition on this node).
     * Backstop for the per-buffer {@link #removeBuffer} cleanup: called when a query terminates
     * abnormally (task cancellation / abort) where individual consumer tasks may never reach their
     * own terminal path. Idempotent — a no-op when the query has no buffers.
     *
     * @return the number of buffers removed
     */
    int clearForQuery(String queryId);
}
