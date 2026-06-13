/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Set;

/**
 * Parquet field for boolean values using {@link BitVector}.
 */
public class BooleanParquetField extends ParquetField {

    /** Creates a new BooleanParquetField. */
    public BooleanParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        ((BitVector) managedVSR.getVector(mappedFieldType.name())).setSafe(managedVSR.getRowCount(), (Boolean) parseValue ? 1 : 0);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Bool();
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(
            FieldTypeCapabilities.Capability.BLOOM_FILTER,
            FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH // This can be supported directly via stats
        );
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
