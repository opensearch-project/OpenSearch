/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Converts row-oriented {@link FragmentExecutionResponse} data to an Arrow
 * {@link VectorSchemaRoot}. This is MVP scaffolding — it will be deleted when
 * the wire format carries Arrow batches directly.
 *
 * <p>Supported types: Long, Integer, Double, Float, Boolean, String
 * (and CharSequence), byte[], and null.
 */
final class RowBatchToArrowConverter {

    private RowBatchToArrowConverter() {}

    /**
     * Convert a row-oriented response to an Arrow VectorSchemaRoot.
     *
     * @param response    the row-oriented shard response
     * @param targetSchema the Arrow schema the output must conform to
     * @param allocator   the buffer allocator for Arrow vectors
     * @return a new VectorSchemaRoot; caller owns and must close it
     */
    public static VectorSchemaRoot convert(FragmentExecutionResponse response, Schema targetSchema, BufferAllocator allocator) {
        VectorSchemaRoot vsr = VectorSchemaRoot.create(targetSchema, allocator);
        try {
            vsr.allocateNew();
            List<Object[]> rows = response.getRows();
            int rowCount = rows.size();

            for (int col = 0; col < targetSchema.getFields().size(); col++) {
                Field field = targetSchema.getFields().get(col);
                FieldVector vector = vsr.getVector(col);
                for (int r = 0; r < rowCount; r++) {
                    Object value = rows.get(r)[col];
                    setValue(vector, r, value, field);
                }
                vector.setValueCount(rowCount);
            }
            vsr.setRowCount(rowCount);
            return vsr;
        } catch (Exception e) {
            vsr.close();
            throw e;
        }
    }

    private static void setValue(FieldVector vector, int index, Object value, Field field) {
        if (value == null) {
            vector.setNull(index);
            return;
        }
        switch (vector.getMinorType()) {
            case BIGINT:
                if (value instanceof Number == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects BIGINT but got " + value.getClass().getName()
                    );
                }
                ((BigIntVector) vector).set(index, ((Number) value).longValue());
                break;
            case INT:
                if (value instanceof Number == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects INT but got " + value.getClass().getName()
                    );
                }
                ((IntVector) vector).set(index, ((Number) value).intValue());
                break;
            case FLOAT8:
                if (value instanceof Number == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects FLOAT8 but got " + value.getClass().getName()
                    );
                }
                ((Float8Vector) vector).set(index, ((Number) value).doubleValue());
                break;
            case FLOAT4:
                if (value instanceof Number == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects FLOAT4 but got " + value.getClass().getName()
                    );
                }
                ((Float4Vector) vector).set(index, ((Number) value).floatValue());
                break;
            case BIT:
                if (value instanceof Boolean == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects BIT (Boolean) but got " + value.getClass().getName()
                    );
                }
                ((BitVector) vector).set(index, ((Boolean) value) ? 1 : 0);
                break;
            case VARCHAR:
                if (value instanceof CharSequence == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects VARCHAR (CharSequence) but got " + value.getClass().getName()
                    );
                }
                ((VarCharVector) vector).setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
                break;
            case VARBINARY:
                if (value instanceof byte[] == false) {
                    throw new IllegalArgumentException(
                        "Column '" + field.getName() + "' expects VARBINARY (byte[]) but got " + value.getClass().getName()
                    );
                }
                ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
                break;
            default:
                throw new IllegalArgumentException("Unsupported vector type: " + vector.getMinorType());
        }
    }
}
