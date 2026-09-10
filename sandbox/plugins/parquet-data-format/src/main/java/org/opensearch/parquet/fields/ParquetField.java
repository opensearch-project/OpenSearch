/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Set;

/**
 * Abstract base class for Parquet field implementations that handle conversion
 * between OpenSearch field types and Apache Arrow vectors.
 */
public abstract class ParquetField {

    /** Creates a new ParquetField. */
    public ParquetField() {}

    /**
     * Writes the parsed field value into the appropriate vector in the managed VSR. Default looks
     * up the field's own top-level vector and row and delegates to {@link #writeValue}, so a normal
     * scalar type only needs to implement that one method. Overridden directly (bypassing
     * {@link #writeValue} entirely) by types that are never actually written this way — e.g.
     * {@code flat_object}, whose values arrive via a different signal.
     *
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    protected void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        writeValue(managedVSR.getVector(fieldType.name()), managedVSR.getRowCount(), parseValue);
    }

    /**
     * Writes {@code value} into {@code vector} at {@code index}. The single canonical conversion for
     * this type, reused by both the top-level path (via {@link #addToGroup}'s default above) and
     * nested-struct-leaf writing.
     *
     * @param vector the vector to write into — already the correct concrete type for this field
     * @param index the row (top-level) or struct-element (nested) index to write at
     * @param value the value to write; never null (callers skip the call entirely for a null value,
     *              leaving the slot null)
     */
    protected void writeValue(FieldVector vector, int index, Object value) {
        throw new UnsupportedOperationException("writeValue is not implemented for " + getClass().getSimpleName());
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
        addToGroup(fieldType, managedVSR, parseValue);
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
