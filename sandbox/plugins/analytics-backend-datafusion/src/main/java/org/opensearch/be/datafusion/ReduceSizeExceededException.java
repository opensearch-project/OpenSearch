/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BroadcastSizeExceededException;

/**
 * Thrown by {@link DatafusionMemtableReduceSink} when buffering the next reduce input batch
 * would exceed the per-query coordinator memory budget (the limit of the query's Arrow
 * allocator, set by {@code analytics.coordinator.buffer_limit}).
 *
 * <p>The coordinator-centric reduce buffers every reduced input fully in off-heap Arrow memory
 * before executing the join/aggregate. For large inputs (e.g. a TPC-H sf=10 join) that buffer
 * can exceed the per-query budget. Rather than letting a raw {@link org.apache.arrow.memory.OutOfMemoryException}
 * fire mid-allocation — which leaks the in-flight buffers and surfaces as an opaque
 * {@code TaskCancelledException} — the sink accounts for the buffer size <em>before</em> each
 * allocation and fails fast with this exception when the budget would be exceeded. Mirrors
 * {@link BroadcastSizeExceededException}.
 *
 * <p>Operators have three remediations: raise the cap ({@code analytics.coordinator.buffer_limit}),
 * narrow the query (better filters / fewer columns on the reduced inputs), or flip
 * {@code analytics.mpp.enabled=false} to revert to a non-MPP path.
 *
 * @opensearch.internal
 */
public final class ReduceSizeExceededException extends RuntimeException {

    public ReduceSizeExceededException(long observed, long limit) {
        super(
            "Coordinator-reduce buffer exceeded the per-query memory budget "
                + "(observed="
                + observed
                + " bytes, limit="
                + limit
                + " bytes). "
                + "Raise analytics.coordinator.buffer_limit, narrow the query, or set analytics.mpp.enabled=false."
        );
    }
}
