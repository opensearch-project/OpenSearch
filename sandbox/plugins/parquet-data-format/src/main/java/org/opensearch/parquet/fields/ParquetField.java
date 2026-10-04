/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class for Parquet field implementations that handle conversion
 * between OpenSearch field types and Apache Arrow vectors.
 */
public abstract class ParquetField {

    /**
     * Name of the child field inside a LIST column. Matches the parquet-rs convention
     * ({@code PARQUET_LIST_ELEMENT_NAME}) so the leaf path is {@code <field>.list.element}.
     */
    public static final String LIST_ELEMENT_NAME = "element";

    /** Creates a new ParquetField. */
    public ParquetField() {}

    /**
     * Writes the parsed field value into the appropriate vector in the managed VSR.
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    protected abstract void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue);

    /**
     * Writes a single parsed value at an explicit index in the given vector.
     * <p>
     * Scalar columns write at the row index, so {@link #addToGroup} can derive the position from
     * the VSR's row count. List columns write several values per row at positions in the child
     * vector that have nothing to do with the row number, so multi-value writes need this
     * index-explicit form instead.
     * <p>
     * Subclasses must override this to support being declared multi-valued; the default throws.
     * When overridden, {@link #addToGroup} should delegate to it so the scalar and list paths
     * share one value-coercion implementation.
     *
     * @param vector the target vector (the child data vector when writing into a list)
     * @param index the position to write at
     * @param parseValue the parsed value to write
     */
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        if (vector instanceof VarCharVector typed) {
            typed.setSafe(index, parseValue.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof VarBinaryVector typed) {
            if (parseValue instanceof InetAddress address) {
                BytesRef encoded = new BytesRef(InetAddressPoint.encode(address));
                typed.setSafe(index, encoded.bytes, encoded.offset, encoded.length);
            } else if (parseValue instanceof BytesRef bytes) {
                typed.setSafe(index, bytes.bytes, bytes.offset, bytes.length);
            } else {
                typed.setSafe(index, (byte[]) parseValue);
            }
        } else if (vector instanceof TinyIntVector typed) {
            typed.setSafe(index, ((Number) parseValue).byteValue());
        } else if (vector instanceof SmallIntVector typed) {
            typed.setSafe(index, ((Number) parseValue).shortValue());
        } else if (vector instanceof IntVector typed) {
            typed.setSafe(index, ((Number) parseValue).intValue());
        } else if (vector instanceof BigIntVector typed) {
            typed.setSafe(index, ((Number) parseValue).longValue());
        } else if (vector instanceof UInt8Vector typed) {
            typed.setSafe(index, ((Number) parseValue).longValue());
        } else if (vector instanceof Float2Vector typed) {
            typed.setSafeWithPossibleTruncate(index, ((Number) parseValue).floatValue());
        } else if (vector instanceof Float4Vector typed) {
            typed.setSafe(index, ((Number) parseValue).floatValue());
        } else if (vector instanceof Float8Vector typed) {
            typed.setSafe(index, ((Number) parseValue).doubleValue());
        } else if (vector instanceof BitVector typed) {
            typed.setSafe(index, (Boolean) parseValue ? 1 : 0);
        } else if (vector instanceof TimeStampMilliVector typed) {
            typed.setSafe(index, ((Number) parseValue).longValue());
        } else if (vector instanceof TimeStampNanoVector typed) {
            typed.setSafe(index, ((Number) parseValue).longValue());
        } else {
            throw new UnsupportedOperationException(
                "Arrow vector [" + vector.getClass().getSimpleName() + "] does not support scalar LIST elements"
            );
        }
    }

    /** Returns whether this scalar Arrow type can be represented as a LIST element. */
    public boolean supportsMultiValue() {
        ArrowType type = getArrowType();
        return type instanceof ArrowType.Utf8
            || type instanceof ArrowType.Binary
            || type instanceof ArrowType.Int
            || type instanceof ArrowType.FloatingPoint
            || type instanceof ArrowType.Bool
            || type instanceof ArrowType.Timestamp;
    }

    /**
     * Builds the Arrow field describing this column, including any child fields.
     * <p>
     * When {@code multiValue} is true the result is a {@code LIST<element>} whose child carries
     * this field's element type, so the same {@link ParquetField} describes both shapes.
     *
     * @param name the Arrow field name
     * @param multiValue whether to wrap the element type in a list
     * @return the Arrow field
     */
    public final Field toArrowField(String name, boolean multiValue) {
        if (multiValue == false) {
            return new Field(name, getFieldType(), null);
        }
        if (supportsMultiValue() == false) {
            throw new IllegalArgumentException(
                "Field ["
                    + name
                    + "] cannot be stored as multi-valued: type ["
                    + getClass().getSimpleName()
                    + "] does not support list storage"
            );
        }
        // The element is always nullable: a null inside an array (e.g. ["a", null]) is a legal
        // document even when the column itself is declared non-nullable.
        Field element = new Field(LIST_ELEMENT_NAME, FieldType.nullable(getArrowType()), null);
        return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    }

    /**
     * Creates and processes a field entry. Throws if vector not present in VSR.
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    public final void createField(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        assert fieldType != null : "MappedFieldType cannot be null";
        assert managedVSR != null : "ManagedVSR cannot be null";
        FieldVector vector = managedVSR.getVector(fieldType.name());
        if (vector instanceof ListVector listVector) {
            writeList(fieldType, managedVSR, listVector, parseValue);
            return;
        }
        addToGroup(fieldType, managedVSR, parseValue);
    }

    /**
     * Writes all values collected for one document into a list column at the current row.
     * <p>
     * A null {@code parseValue} is written as a null list, which is how an absent field is
     * represented. An empty list is written as a zero-length, non-null list, preserving the
     * distinction between {@code "tags": []} and no {@code tags} at all.
     */
    private void writeList(MappedFieldType fieldType, ManagedVSR managedVSR, ListVector listVector, Object parseValue) {
        int row = managedVSR.getRowCount();
        if (parseValue == null) {
            listVector.setNull(row);
            return;
        }
        List<?> values = parseValue instanceof List<?> list ? list : List.of(parseValue);
        int start = listVector.startNewValue(row);
        FieldVector dataVector = listVector.getDataVector();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                dataVector.setNull(start + i);
            } else {
                addToVector(dataVector, start + i, value);
            }
        }
        listVector.endValue(row, values.size());
    }

    /**
     * Returns the set of capabilities supported by this field type.
     * Subclasses may override to declare different capabilities.
     *
     * @return set of supported {@link FieldTypeCapabilities.Capability}
     */
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER);
    }

    /** Returns the Arrow type for this field. */
    public abstract ArrowType getArrowType();

    /** Returns the Arrow field type with nullability metadata. */
    public abstract FieldType getFieldType();
}
