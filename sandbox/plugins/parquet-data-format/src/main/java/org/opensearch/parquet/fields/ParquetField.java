/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;

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
     * Writes a single parsed value at an explicit index in the given vector — the canonical
     * element conversion for this type, shared by every write path that targets an arbitrary
     * position: LIST (multi-value) columns writing several values per row, and nested struct-leaf
     * writes ({@link NestedParquetField#writeLeafValue}) writing one value per element. Scalar
     * columns write at the row index, so {@link #addToGroup} can derive the position from the
     * VSR's row count; implementations should route {@code addToGroup} through this method so the
     * conversion exists exactly once.
     * <p>
     * Every scalar type must implement this; the default throws. Note {@link #supportsMultiValue}
     * separately gates whether a type may be DECLARED multi-valued — implementing this method is
     * necessary but not sufficient for list storage.
     *
     * @param vector the target vector (the child data vector when writing into a list or struct)
     * @param index the position to write at
     * @param parseValue the parsed non-null value to write
     */
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        throw new UnsupportedOperationException(
            "Field type ["
                + getClass().getSimpleName()
                + "] does not define a single-element encoder (addToVector), "
                + "required for multi-valued (list) storage and nested struct-leaf writes"
        );
    }

    /**
     * Returns whether this field may be declared multi-valued (stored as a Parquet LIST column).
     * A policy gate, deliberately narrower than "implements {@link #addToVector}": every scalar
     * type has an element encoder, but only types returning true here accept the
     * {@code multi_value} mapping parameter.
     *
     * @return true if multi-valued storage is supported
     */
    public boolean supportsMultiValue() {
        return false;
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
            writeList(managedVSR, listVector, parseValue);
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
    private void writeList(ManagedVSR managedVSR, ListVector listVector, Object parseValue) {
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

    /**
     * Builds the Arrow {@link Field} named {@code name} for this type. Default is a leaf with no
     * children, using {@link #getFieldType()} — correct for every scalar type. Overridden by types
     * whose Arrow representation has children (e.g. {@code flat_object}'s {@code MAP<Utf8,Utf8>}),
     * so schema-building code can call this uniformly instead of special-casing by type name.
     *
     * @param name the field's name — a full dotted path at the document root, or a leaf name
     *             relative to its parent struct when nested inside a {@code LIST<STRUCT>}
     */
    public Field buildField(String name) {
        return new Field(name, getFieldType(), null);
    }
}
