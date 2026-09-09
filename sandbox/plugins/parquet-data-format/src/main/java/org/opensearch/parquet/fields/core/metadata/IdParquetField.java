/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.metadata;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Set;

/**
 * Parquet field for document _id metadata stored as binary using {@link VarBinaryVector}.
 */
public class IdParquetField extends ParquetField {

    /** Creates a new IdParquetField. */
    public IdParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarBinaryVector vector = (VarBinaryVector) managedVSR.getVector(mappedFieldType.name());
        vector.setSafe(managedVSR.getRowCount(), (byte[]) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Binary();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(
            FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.BLOOM_FILTER,
            FieldTypeCapabilities.Capability.STORED_FIELDS
        );
    }
}
