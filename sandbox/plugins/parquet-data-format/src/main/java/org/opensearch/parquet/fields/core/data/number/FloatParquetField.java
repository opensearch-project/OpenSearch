/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for single-precision floating-point values using {@link Float4Vector}.
 */
public class FloatParquetField extends NumericParquetField {

    /** Creates a new FloatParquetField. */
    public FloatParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        addToVector(managedVSR.getVector(mappedFieldType.name()), managedVSR.getRowCount(), parseValue);
    }

    @Override
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        ((Float4Vector) vector).setSafe(index, (Float) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
