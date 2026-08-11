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
     * Writes the parsed field value into the given vector at the given row.
     * <p>
     * The vector is resolved <b>once</b> by the caller and passed in; implementations must not re-resolve it
     * by field name — the per-field name lookup ({@code ManagedVSR.getVector} → {@code HashMap.get}) runs once
     * per field per document on the bulk-indexing hot path and was a visible CPU frame in ingest profiles when
     * every implementation repeated it.
     *
     * @param fieldType the mapped field type
     * @param vector the resolved field vector to write into
     * @param rowIndex the row index to write at
     * @param parseValue the parsed value to write
     */
    protected abstract void addToGroup(MappedFieldType fieldType, FieldVector vector, int rowIndex, Object parseValue);

    /**
     * Creates and processes a field entry against an already-resolved vector.
     * @param fieldType the mapped field type
     * @param vector the resolved field vector
     * @param rowIndex the row index to write at
     * @param parseValue the parsed value to write
     */
    public final void createField(MappedFieldType fieldType, FieldVector vector, int rowIndex, Object parseValue) {
        assert fieldType != null : "MappedFieldType cannot be null";
        assert vector != null : "FieldVector cannot be null";
        addToGroup(fieldType, vector, rowIndex, parseValue);
    }

    /**
     * Convenience entry point that resolves the vector by field name. Throws if vector not present in VSR.
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    public final void createField(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        assert fieldType != null : "MappedFieldType cannot be null";
        assert managedVSR != null : "ManagedVSR cannot be null";
        FieldVector vector = managedVSR.getVector(fieldType.name());
        if (vector == null) {
            throw new IllegalStateException("No vector for field [" + fieldType.name() + "] in VSR [" + managedVSR.getId() + "]");
        }
        addToGroup(fieldType, vector, managedVSR.getRowCount(), parseValue);
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
