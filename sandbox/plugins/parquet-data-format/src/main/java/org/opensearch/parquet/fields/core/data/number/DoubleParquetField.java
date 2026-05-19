/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for double-precision floating-point values using {@link Float8Vector}.
 */
public class DoubleParquetField extends ParquetField {

    /** Creates a new DoubleParquetField. */
    public DoubleParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        ((Float8Vector) managedVSR.getVector(mappedFieldType.name())).setSafe(managedVSR.getRowCount(), (Double) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
