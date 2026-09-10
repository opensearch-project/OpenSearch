/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Parquet field for 64-bit unsigned long values using {@link UInt8Vector}.
 */
public class UnsignedLongParquetField extends NumericParquetField {

    /** Creates a new UnsignedLongParquetField. */
    public UnsignedLongParquetField() {}

    @Override
    protected void writeValue(FieldVector vector, int index, Object value) {
        ((UInt8Vector) vector).setSafe(index, ((Number) value).longValue());
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(64, false);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
