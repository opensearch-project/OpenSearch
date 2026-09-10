/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Parquet field for half-precision (16-bit) floating-point values using {@link Float2Vector}.
 */
public class HalfFloatParquetField extends NumericParquetField {

    /** Creates a new HalfFloatParquetField. */
    public HalfFloatParquetField() {}

    @Override
    protected void writeValue(FieldVector vector, int index, Object value) {
        ((Float2Vector) vector).setSafeWithPossibleTruncate(index, ((Number) value).floatValue());
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
