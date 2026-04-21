/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.backend.ExchangeSource;

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
 */
public class RowProducingSink implements ExchangeSink, ExchangeSource {

    private final List<VectorSchemaRoot> batches = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();

    @Override
    public void feed(VectorSchemaRoot batch) {
        if (fieldNames.isEmpty() && batch.getSchema().getFields().isEmpty() == false) {
            for (org.apache.arrow.vector.types.pojo.Field f : batch.getSchema().getFields()) {
                fieldNames.add(f.getName());
            }
        }
        batches.add(batch);
    }

    @Override
    public void close() {
        for (VectorSchemaRoot batch : batches) {
            batch.close();
        }
        batches.clear();
    }

    @Override
    public Iterable<VectorSchemaRoot> readResult() {
        return batches;
    }

    @Override
    public long getRowCount() {
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
    public Object getValueAt(String column, int rowIndex) {
        int colIdx = fieldNames.indexOf(column);
        if (colIdx < 0) return null;

        int offset = 0;
        for (VectorSchemaRoot batch : batches) {
            int batchRows = batch.getRowCount();
            if (rowIndex < offset + batchRows) {
                return toJavaValue(batch.getVector(colIdx), rowIndex - offset);
            }
            offset += batchRows;
        }
        return null;
    }

    private static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return vector.getObject(index);
    }
}
