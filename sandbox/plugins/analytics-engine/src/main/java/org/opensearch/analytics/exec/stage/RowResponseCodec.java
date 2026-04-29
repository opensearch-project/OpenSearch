/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import java.util.Iterator;
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
            allocator = new RootAllocator();
        }

        // Infer Arrow type per column from the first non-null value. List columns
        // also need their element type inferred so the resulting ListVector has
        // a properly-typed data vector.
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
     * Infers the Arrow {@link Field} for a column. For list-typed cells, also
     * recurses into the elements to derive a child field — Arrow {@link ListVector}
     * requires a typed data vector, not just an opaque list type.
     */
    static Field inferField(String name, List<Object[]> rows, int col) {
        ArrowType arrowType = inferArrowType(rows, col);
        if (arrowType instanceof ArrowType.List) {
            ArrowType elementType = inferListElementType(rows, col);
            Field child = new Field("item", FieldType.nullable(elementType), null);
            return new Field(name, FieldType.nullable(arrowType), List.of(child));
        }
        return new Field(name, FieldType.nullable(arrowType), null);
    }

    /**
     * Infers the Arrow type for a column by scanning rows for the first
     * non-null value. Falls back to {@code Utf8} (VarChar) if all values
     * are null or the Java type is unrecognized.
     */
    static ArrowType inferArrowType(List<Object[]> rows, int col) {
        for (Object[] row : rows) {
            Object value = row[col];
            if (value == null) continue;
            if (value instanceof Long) return new ArrowType.Int(64, true);
            if (value instanceof Integer) return new ArrowType.Int(32, true);
            if (value instanceof Short) return new ArrowType.Int(16, true);
            if (value instanceof Byte) return new ArrowType.Int(8, true);
            if (value instanceof Double) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            if (value instanceof Float) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
            if (value instanceof Boolean) return ArrowType.Bool.INSTANCE;
            if (value instanceof CharSequence) return ArrowType.Utf8.INSTANCE;
            if (value instanceof byte[]) return ArrowType.Binary.INSTANCE;
            if (value instanceof List) return ArrowType.List.INSTANCE;
            if (value instanceof Number) return new ArrowType.Int(64, true);
            break;
        }
        return ArrowType.Utf8.INSTANCE;
    }

    /**
     * For a list-typed column, scans non-null cells until a non-empty list is
     * found and infers the element Arrow type from its first non-null element.
     * Falls back to {@code Utf8} when no element is observable.
     */
    private static ArrowType inferListElementType(List<Object[]> rows, int col) {
        for (Object[] row : rows) {
            Object value = row[col];
            if (!(value instanceof List<?> list)) continue;
            for (Object element : list) {
                if (element == null) continue;
                if (element instanceof Long) return new ArrowType.Int(64, true);
                if (element instanceof Integer) return new ArrowType.Int(32, true);
                if (element instanceof Short) return new ArrowType.Int(16, true);
                if (element instanceof Byte) return new ArrowType.Int(8, true);
                if (element instanceof Double) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
                if (element instanceof Float) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
                if (element instanceof Boolean) return ArrowType.Bool.INSTANCE;
                if (element instanceof CharSequence) return ArrowType.Utf8.INSTANCE;
                if (element instanceof org.apache.arrow.vector.util.Text) return ArrowType.Utf8.INSTANCE;
                if (element instanceof Number) return new ArrowType.Int(64, true);
            }
        }
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
            writeListCell(listVector, index, (List<?>) value);
        } else {
            throw new IllegalArgumentException("Unsupported Arrow vector type: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Writes a single Java {@link List} cell into the given {@link ListVector}
     * at {@code index}. Uses the {@link UnionListWriter} for offset management
     * so the list's start/end offsets stay consistent; element values are
     * dispatched per-type through the writer's typed setters, mirroring the
     * inference in {@link #inferListElementType}.
     */
    private static void writeListCell(ListVector listVector, int index, List<?> values) {
        UnionListWriter writer = listVector.getWriter();
        writer.setPosition(index);
        writer.startList();
        for (Object element : values) {
            writeListElement(writer, element);
        }
        writer.endList();
    }

    /**
     * Writes a single list element through the {@link UnionListWriter}'s typed
     * setters. Arrow's {@code Text} class doesn't implement {@link CharSequence}
     * but does implement {@link org.apache.arrow.vector.util.ReusableByteArray},
     * so a list cell sourced from {@link org.apache.arrow.vector.complex.ListVector#getObject}
     * carries {@code Text} elements rather than {@link String}. Match those
     * explicitly via the same {@code toString()} contract the rest of the
     * codec uses.
     */
    private static void writeListElement(UnionListWriter writer, Object element) {
        if (element == null) {
            writer.writeNull();
            return;
        }
        if (element instanceof Long l) {
            writer.bigInt().writeBigInt(l);
        } else if (element instanceof Integer i) {
            writer.integer().writeInt(i);
        } else if (element instanceof Short s) {
            writer.smallInt().writeSmallInt(s);
        } else if (element instanceof Byte b) {
            writer.tinyInt().writeTinyInt(b);
        } else if (element instanceof Double d) {
            writer.float8().writeFloat8(d);
        } else if (element instanceof Float f) {
            writer.float4().writeFloat4(f);
        } else if (element instanceof Boolean b) {
            writer.bit().writeBit(b ? 1 : 0);
        } else if (element instanceof CharSequence cs) {
            writer.varChar().writeVarChar(cs.toString());
        } else if (element instanceof org.apache.arrow.vector.util.Text t) {
            writer.varChar().writeVarChar(t.toString());
        } else if (element instanceof Number n) {
            writer.bigInt().writeBigInt(n.longValue());
        } else {
            throw new IllegalArgumentException(
                "Unsupported list element type: " + element.getClass().getSimpleName()
            );
        }
    }
}
