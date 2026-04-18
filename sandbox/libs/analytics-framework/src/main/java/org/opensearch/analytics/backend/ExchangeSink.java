/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Write-only interface for feeding Arrow batches into a stage exchange.
 * Producers (shard scan stages, local compute stages) call {@link #feed}
 * to push data; they never read from the sink.
 *
 * <p>Implementations must be thread-safe — multiple shard response
 * handlers may call {@link #feed} concurrently.
 *
 * @see ExchangeSource for the read-side counterpart
 */
public interface ExchangeSink {

    /**
     * Ingest an Arrow batch into this sink. The sink takes ownership of the
     * batch and is responsible for releasing it when no longer needed.
     */
    void feed(VectorSchemaRoot batch);

    /**
     * Signal that no more batches will be fed. Releases resources.
     */
    void close();
}
