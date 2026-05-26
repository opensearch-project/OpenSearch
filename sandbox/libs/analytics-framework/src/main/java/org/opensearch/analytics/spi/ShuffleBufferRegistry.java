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
}
