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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

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
 * <p>A configurable row count limit ({@link #maxRows}) acts as a guardrail
 * against unbounded result accumulation. When exceeded, {@link #feed}
 * throws {@link OpenSearchRejectedExecutionException} which propagates to the stage
 * execution and transitions it to FAILED.
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

    /**
     * Default maximum number of rows this sink will accept before rejecting
     * further batches. Analogous to {@code index.max_result_window} (10k)
     * in the core search path, but set higher for analytics workloads.
     *
     * <p>TODO: make configurable via cluster setting.
     */
    static final long DEFAULT_MAX_ROWS = 1_000_000L;

    private final List<VectorSchemaRoot> batches = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();
    private final long maxRows;
    private long totalRows;

    /** Creates a sink with the default row limit. */
    public RowProducingSink() {
        this(DEFAULT_MAX_ROWS);
    }

    /** Creates a sink with a custom row limit. Use {@code Long.MAX_VALUE} to disable. */
    public RowProducingSink(long maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public synchronized void feed(VectorSchemaRoot batch) {
        if (fieldNames.isEmpty() && batch.getSchema().getFields().isEmpty() == false) {
            for (Field f : batch.getSchema().getFields()) {
                fieldNames.add(f.getName());
            }
        }
        long incoming = batch.getRowCount();
        if (totalRows + incoming > maxRows) {
            batch.close();
            throw new OpenSearchRejectedExecutionException(
                "Analytics query result exceeded maximum row limit of "
                    + maxRows
                    + " rows. "
                    + "Consider adding filters or aggregations to reduce the result set."
            );
        }
        totalRows += incoming;
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
        return totalRows;
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
