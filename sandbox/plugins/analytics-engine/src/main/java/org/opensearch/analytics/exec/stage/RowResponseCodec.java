/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link ResponseCodec} for the current row-oriented
 * {@link FragmentExecutionResponse} wire format. Converts {@code Object[]}
 * rows to Arrow {@link VectorSchemaRoot} via type inference.
 *
 * <p>This codec is the bridge that gets replaced when Arrow IPC transport
 * lands. A future {@code ArrowIpcResponseCodec} would import IPC buffers
 * directly — zero conversion.
 *
 * @opensearch.internal
 */
public final class RowResponseCodec implements ResponseCodec<FragmentExecutionResponse> {

    /** Singleton instance — stateless, thread-safe. */
    public static final RowResponseCodec INSTANCE = new RowResponseCodec();

    private RowResponseCodec() {}

    @Override
    public VectorSchemaRoot decode(FragmentExecutionResponse response, BufferAllocator allocator) {
        List<String> fieldNames = response.getFieldNames();
        List<Object[]> rows = response.getRows();

        if (allocator == null) {
            throw new IllegalArgumentException("BufferAllocator must not be null");
        }

        // Infer Arrow type per column from the first non-null value
        List<Field> fields = new ArrayList<>();
        for (int col = 0; col < fieldNames.size(); col++) {
            fields.add(inferField(fieldNames.get(col), rows, col));
        }
        Schema schema = new Schema(fields);

        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        try {
            vsr.allocateNew();
            int rowCount = rows.size();
            for (int col = 0; col < fieldNames.size(); col++) {
                FieldVector vector = vsr.getVector(col);
                for (int r = 0; r < rowCount; r++) {
                    Object value = rows.get(r)[col];
                    setVectorValue(vector, r, value);
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

    /**
     * Infers the Arrow {@link Field} for a column by scanning rows for the first
     * non-null value. Falls back to a nullable {@code Utf8} (VarChar) field if all
     * values are null or the Java type is unrecognized.
     *
     * <p>For {@link List} cells (produced by analytics-engine routes that emit
     * array-typed values — PPL {@code array(...)}, {@code array_slice}, …), this
     * returns a {@code List<inner>} field where the inner element type is inferred
     * from the first non-null element. Without this branch, list values fall
     * through to the {@code Utf8} fallback and {@link #setVectorValue} produces
     * {@code value.toString()} (e.g. {@code "[2,3,4]"} as a JSON-like string)
     * instead of a typed array.
     */
    static Field inferField(String name, List<Object[]> rows, int col) {
        for (Object[] row : rows) {
            Object value = row[col];
            if (value == null) continue;
            if (value instanceof List<?> list) {
                ArrowType elementType = inferElementArrowType(list, rows, col);
                Field elementField = new Field("$data$", FieldType.nullable(elementType), null);
                return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(elementField));
            }
            return new Field(name, FieldType.nullable(scalarArrowType(value)), null);
        }
        return new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
    }

    /**
     * Best-effort inference for a list element type. Looks at the first non-null
     * element of the given list, then falls back to scanning later rows of the
     * same column if this list is empty or all-null. Defaults to {@code Utf8}.
     */
    private static ArrowType inferElementArrowType(List<?> list, List<Object[]> rows, int col) {
        for (Object element : list) {
            if (element != null) return scalarArrowType(element);
        }
        for (Object[] row : rows) {
            Object value = row[col];
            if (value instanceof List<?> other) {
                for (Object element : other) {
                    if (element != null) return scalarArrowType(element);
                }
            }
        }
        return ArrowType.Utf8.INSTANCE;
    }

    /** Maps a Java scalar value to the corresponding Arrow scalar type. */
    private static ArrowType scalarArrowType(Object value) {
        if (value instanceof Long) return new ArrowType.Int(64, true);
        if (value instanceof Integer) return new ArrowType.Int(32, true);
        if (value instanceof Short) return new ArrowType.Int(16, true);
        if (value instanceof Byte) return new ArrowType.Int(8, true);
        if (value instanceof Double) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        if (value instanceof Float) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
        if (value instanceof Boolean) return ArrowType.Bool.INSTANCE;
        if (value instanceof CharSequence) return ArrowType.Utf8.INSTANCE;
        if (value instanceof byte[]) return ArrowType.Binary.INSTANCE;
        if (value instanceof Number) return new ArrowType.Int(64, true);
        return ArrowType.Utf8.INSTANCE;
    }

    /**
     * Sets a value on the appropriate Arrow vector type. Handles null by
     * calling {@code setNull}. For typed vectors, casts the Java value to
     * the expected type.
     */
    static void setVectorValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            vector.setNull(index);
            return;
        }
        if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, ((Number) value).intValue());
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, ((Boolean) value) ? 1 : 0);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof VarBinaryVector) {
            ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
        } else if (vector instanceof ListVector listVector) {
            writeListValue(listVector, index, (List<?>) value);
        } else {
            throw new IllegalArgumentException("Unsupported Arrow vector type: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Writes a Java {@link List} into an Arrow {@link ListVector} at the given row
     * index. Bypasses {@link UnionListWriter} entirely — writes directly to the
     * list's offset / validity buffers and to the inner data vector via the inner
     * vector's own typed setter. The writer-based API requires an
     * {@link org.apache.arrow.memory.ArrowBuf} per varchar element which Arrow
     * couples to a release lifecycle that's tricky to get right (early close →
     * use-after-free, no close → leak); the direct path avoids that altogether.
     *
     * <p>The inner data vector's type was decided in {@link #inferField} from the
     * first non-null list element, so the {@code instanceof} dispatch here simply
     * needs to match.
     */
    private static void writeListValue(ListVector listVector, int index, List<?> list) {
        FieldVector dataVector = listVector.getDataVector();
        int startOffset = listVector.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
        int writePos = startOffset;
        for (Object element : list) {
            // Grow the data vector if needed before writing. setSafe-style helpers handle this
            // automatically per type, but we still need to pre-position the cursor.
            if (element == null) {
                dataVector.setNull(writePos);
            } else if (dataVector instanceof BigIntVector v) {
                v.setSafe(writePos, ((Number) element).longValue());
            } else if (dataVector instanceof IntVector v) {
                v.setSafe(writePos, ((Number) element).intValue());
            } else if (dataVector instanceof SmallIntVector v) {
                v.setSafe(writePos, ((Number) element).shortValue());
            } else if (dataVector instanceof TinyIntVector v) {
                v.setSafe(writePos, ((Number) element).byteValue());
            } else if (dataVector instanceof Float8Vector v) {
                v.setSafe(writePos, ((Number) element).doubleValue());
            } else if (dataVector instanceof Float4Vector v) {
                v.setSafe(writePos, ((Number) element).floatValue());
            } else if (dataVector instanceof BitVector v) {
                v.setSafe(writePos, ((Boolean) element) ? 1 : 0);
            } else if (dataVector instanceof VarCharVector v) {
                v.setSafe(writePos, element.toString().getBytes(StandardCharsets.UTF_8));
            } else if (dataVector instanceof VarBinaryVector v) {
                v.setSafe(writePos, (byte[]) element);
            } else {
                throw new IllegalArgumentException("Unsupported list element vector type: " + dataVector.getClass().getSimpleName());
            }
            writePos++;
        }
        // Mark this row's list as non-null and update its end offset.
        listVector.setNotNull(index);
        listVector.getOffsetBuffer().setInt((long) (index + 1) * ListVector.OFFSET_WIDTH, writePos);
        // Keep the data vector's value count in sync so subsequent reads see the new tail.
        if (writePos > dataVector.getValueCount()) {
            dataVector.setValueCount(writePos);
        }
    }
}
