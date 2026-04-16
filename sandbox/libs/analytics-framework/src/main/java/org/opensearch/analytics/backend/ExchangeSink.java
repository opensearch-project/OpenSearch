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
 * Per-stage result accumulator that collects Arrow {@link VectorSchemaRoot} batches
 * from shard executions and provides access to the accumulated result set.
 */
public interface ExchangeSink {

    /**
     * Ingest an Arrow batch into this sink. The sink takes ownership of the
     * batch and is responsible for releasing it when no longer needed.
     */
    void feed(VectorSchemaRoot batch);

    /**
     * Signal that no more responses will be fed.
     */
    void close();

    /**
     * Return all accumulated rows in insertion order.
     */
    Iterable<Object[]> readResult();

    /**
     * Return the total number of accumulated rows.
     */
    long getRowCount();

    /**
     * Look up a cell value by column name and row index.
     *
     * @param column   the column name
     * @param rowIndex the zero-based row index
     * @return the cell value, or {@code null} if the column is unknown or the row index is out of range
     */
    Object getValueAt(String column, int rowIndex);
}
