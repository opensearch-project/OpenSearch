/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Thrown on a data node when a SINGLE hash-shuffle query's accumulated Arrow-IPC payload would exceed
 * the hard per-query footprint budget (the byte budget derived from
 * {@code analytics.mpp.shuffle.node_budget_percent} — a percent of max heap; node budget == per-query
 * max, so a lone query may use the whole budget but no single query may exceed it).
 *
 * <p>The shuffle consumer is buffer-all-then-drain: a worker blocks until both producer sides finish,
 * then drains the accumulated {@code byte[]} chunks (held ON the JVM heap), so a node's live shuffle
 * footprint is the SUM across all its buffers. A query whose own shuffle exceeds the per-query budget
 * can never be consumed even on an otherwise idle node — so this breach is <b>non-retryable by
 * design</b>: it fails the query fast rather than letting the on-heap buffer grow until the node OOMs.
 * This is distinct from the soft backpressure-reject path (admission returning {@code REJECT_RETRY}
 * when the NODE total is momentarily over budget but the query still fits its share → sender retry),
 * which handles transient cross-query contention that WILL clear as other queries release buffers.
 *
 * <p><b>Cross-boundary contract type</b> (hence in the SPI, not a backend module): the engine sets
 * the budget, the buffer manager enforces it on the producer-transport path, and the engine
 * recognizes the breach to surface an actionable failure. Mirrors {@link BroadcastSizeExceededException}
 * and the backend's {@code ReduceSizeExceededException}.
 *
 * <p>Carries observed and limit byte counts. Operator remediations: raise
 * {@code analytics.mpp.shuffle.node_budget_percent} (or give the node more heap), narrow the query,
 * or set {@code analytics.mpp.enabled=false}.
 *
 * @opensearch.internal
 */
public final class ShuffleBufferExceededException extends RuntimeException {

    private final long observedBytes;
    private final long limitBytes;

    public ShuffleBufferExceededException(long observed, long limit) {
        super(
            "Hash-shuffle query exceeded its per-query on-heap budget "
                + "(observed="
                + observed
                + " bytes, limit="
                + limit
                + " bytes). "
                + "Raise analytics.mpp.shuffle.node_budget_percent (or give the node more heap), "
                + "narrow the query, or set analytics.mpp.enabled=false."
        );
        this.observedBytes = observed;
        this.limitBytes = limit;
    }

    /** Bytes the buffer would have held had the chunk been accepted. */
    public long observedBytes() {
        return observedBytes;
    }

    /** The configured hard ceiling, in bytes. */
    public long limitBytes() {
        return limitBytes;
    }
}
