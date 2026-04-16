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
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.backend.ExchangeSink;

import java.util.ArrayList;
import java.util.List;

/**
 * Default {@link ExchangeSink} implementation that collects Arrow
 * {@link VectorSchemaRoot} batches. Converts back to {@code Object[]} rows
 * on {@link #readResult()} for caller compatibility.
 *
 * <p>The sink takes ownership of fed batches and releases them on
 * {@link #close()}.
 */
public class RowProducingSink implements ExchangeSink {

    private final List<VectorSchemaRoot> batches = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();

    @Override
    public void feed(VectorSchemaRoot batch) {
        if (fieldNames.isEmpty() && batch.getSchema().getFields().isEmpty() == false) {
            for (Field f : batch.getSchema().getFields()) {
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
    public Iterable<Object[]> readResult() {
        List<Object[]> rows = new ArrayList<>();
        for (VectorSchemaRoot batch : batches) {
            int colCount = batch.getFieldVectors().size();
            for (int r = 0; r < batch.getRowCount(); r++) {
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    row[c] = toJavaValue(batch.getVector(c), r);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    @Override
    public long getRowCount() {
        long total = 0;
        for (VectorSchemaRoot batch : batches) {
            total += batch.getRowCount();
        }
        return total;
    }

    @Override
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

    /**
     * Converts an Arrow vector value to a Java-native type. VarChar vectors
     * return {@code org.apache.arrow.vector.util.Text} which callers don't
     * expect — convert to {@code String}. Other types pass through as-is.
     */
    private static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return vector.getObject(index);
    }
}
