/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Write-only interface for feeding Arrow batches into a stage exchange.
 * Producers (shard scan stages, local compute stages) call {@link #feed}
 * to push data; they never read from the sink.
 *
 * <p>Implementations are backend-specific and created via {@link ExchangeSinkProvider}.
 * A coordinator-side sink runs the root stage computation (final aggregate, sort, etc.)
 * over the batches it receives.
 *
 * <p>Implementations must be thread-safe — multiple shard response handlers
 * may call {@link #feed} concurrently.
 *
 * @opensearch.internal
 */
public interface ExchangeSink {

    /**
     * Ingest an Arrow batch into this sink. On normal return the sink has taken ownership of the
     * batch and is responsible for releasing it when no longer needed.
     *
     * <p><b>All-or-nothing ownership.</b> Ownership transfers only on normal return. If this method
     * throws, it must NOT have taken ownership of {@code batch} — neither the root nor any of its
     * child vectors — so the caller still owns {@code batch} in full and is responsible for closing
     * it exactly once. Implementations that begin consuming/transferring buffers before they might
     * throw must either complete the transfer or roll it back before propagating, so the caller can
     * safely {@code batch.close()} without risking a double-release. (Callers such as
     * {@code Stitcher.finish} rely on this to free the batch on the feed-throws path.)
     */
    void feed(VectorSchemaRoot batch);

    /**
     * Ingest an Arrow batch with a per-source ordinal — the index of the
     * producer task within its stage's resolved target list (e.g.
     * {@link org.opensearch.analytics.spi.ExchangeSink} consumed via
     * {@code ShardExecutionTarget.ordinal()}).
     *
     * <p>Default implementation drops the ordinal and falls through to
     * {@link #feed(VectorSchemaRoot)}. Sinks that need to discriminate
     * batches by producer (e.g. Late Materialization, where the ordinal is
     * stamped onto each batch as a column) override this method.
     *
     * <p>Producers that have a meaningful per-task ordinal call this overload;
     * producers without one continue to call {@link #feed(VectorSchemaRoot)}.
     */
    default void feed(VectorSchemaRoot batch, int sourceOrdinal) {
        feed(batch);
    }

    /**
     * Whether the downstream consumer has finished and will read no more batches (e.g. a reduce
     * whose LimitExec satisfied its fetch). Producers may poll this after a {@link #feed} to stop
     * early. Default {@code false}; best-effort — a {@code true} is monotonic, a {@code false} may
     * race a concurrent completion.
     */
    default boolean isConsumerDone() {
        return false;
    }

    /**
     * Signal that no more batches will be fed. Releases resources.
     */
    void close();
}
