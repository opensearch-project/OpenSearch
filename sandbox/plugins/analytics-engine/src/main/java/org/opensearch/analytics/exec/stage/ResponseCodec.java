/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.core.action.ActionResponse;

/**
 * Decodes a transport response into an Arrow {@link VectorSchemaRoot} for
 * the coordinator-side sink. Implementations handle the specific wire
 * format — {@code Object[]} rows (current), Arrow IPC (Flight), or any
 * future format.
 *
 * <p>The codec is injected into {@link ShardFragmentStageExecution} at
 * construction time by the scheduler. Swapping the codec swaps the
 * serialization format without touching stage execution logic.
 *
 * @param <R> the transport response type
 * @opensearch.internal
 */
@FunctionalInterface
public interface ResponseCodec<R extends ActionResponse> {

    /**
     * Decodes a transport response into an Arrow {@link VectorSchemaRoot}.
     * The returned VSR is owned by the caller (the sink).
     *
     * @param response  the transport response
     * @param allocator the buffer allocator for Arrow vectors
     * @return a new VectorSchemaRoot; caller owns and must close it
     */
    VectorSchemaRoot decode(R response, BufferAllocator allocator);
}
