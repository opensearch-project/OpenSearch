/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.ArrayList;
import java.util.List;

/**
 * Default exchange implementation that collects Arrow
 * {@link VectorSchemaRoot} batches via {@link ExchangeSink#feed} and
 * yields them back via {@link ExchangeSource#readResult}.
 *
 * <p>Implements both {@link ExchangeSink} (write side for producers) and
 * {@link ExchangeSource} (read side for consumers). The builder passes
 * the {@link ExchangeSink} view to child stages and the walker reads
 * results via the {@link ExchangeSource} view.
 *
 * <p><b>Thread safety:</b> {@link #feed} may be called concurrently from
 * multiple shard response handlers on the SEARCH thread pool. All mutating
 * and observing methods are synchronized on {@code this} to serialize
 * access to the backing lists and to atomicize the check-then-act
 * {@code fieldNames} initialization. This matches the pattern used by
 * {@code QueryPhaseResultConsumer} for coordinator-reduce in the core
 * search path.
 */
public class RowProducingSink implements ExchangeSink, ExchangeSource {

    private final List<VectorSchemaRoot> batches = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();

    @Override
    public synchronized void feed(VectorSchemaRoot batch) {
        if (fieldNames.isEmpty() && batch.getSchema().getFields().isEmpty() == false) {
            for (Field f : batch.getSchema().getFields()) {
                fieldNames.add(f.getName());
            }
        }
        batches.add(batch);
    }

    @Override
    public synchronized void close() {
        for (VectorSchemaRoot batch : batches) {
            batch.close();
        }
        batches.clear();
    }

    /**
     * Returns a snapshot of the batches fed so far. The returned iterable is a
     * defensive copy so the caller can iterate outside the sink's monitor.
     */
    @Override
    public synchronized Iterable<VectorSchemaRoot> readResult() {
        return new ArrayList<>(batches);
    }

    @Override
    public synchronized long getRowCount() {
        long total = 0;
        for (VectorSchemaRoot batch : batches) {
            total += batch.getRowCount();
        }
        return total;
    }

    /**
     * Look up a cell value by column name and row index.
     *
     * @param column   the column name
     * @param rowIndex the zero-based row index
     * @return the cell value, or {@code null} if the column is unknown or the row index is out of range
     */
    public synchronized Object getValueAt(String column, int rowIndex) {
        int colIdx = fieldNames.indexOf(column);
        if (colIdx < 0) return null;

        int offset = 0;
        for (VectorSchemaRoot batch : batches) {
            int batchRows = batch.getRowCount();
            if (rowIndex < offset + batchRows) {
                return ArrowValues.toJavaValue(batch.getVector(colIdx), rowIndex - offset);
            }
            offset += batchRows;
        }
        return null;
    }
}
