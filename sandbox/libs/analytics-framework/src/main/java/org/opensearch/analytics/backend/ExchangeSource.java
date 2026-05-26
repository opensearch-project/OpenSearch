/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Read-only interface for consuming accumulated results from a stage exchange.
 * Consumers (parent stages, the walker's completion listener) read from this;
 * they never write to it.
 *
 * <p>Results are yielded as Arrow batches — the native columnar wire format.
 * Conversion to a row-oriented representation (if needed for presentation at
 * the edge) is the caller's responsibility.
 *
 * @see ExchangeSink for the write-side counterpart
 */
public interface ExchangeSource {

    /**
     * Return all accumulated batches in insertion order. For streaming/compute
     * backends, the returned iterable may block on {@code next()} while the
     * backend produces each batch; iteration terminates when the backend is done.
     */
    Iterable<VectorSchemaRoot> readResult();

    /**
     * Return the total number of accumulated rows across all batches.
     */
    long getRowCount();
}
