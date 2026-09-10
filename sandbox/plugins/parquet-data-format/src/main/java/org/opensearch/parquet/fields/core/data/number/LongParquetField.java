/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Parquet field for 64-bit signed long values using {@link BigIntVector}.
 */
public class LongParquetField extends NumericParquetField {

    private final boolean nullable;

    /** Creates a new LongParquetField. */
    public LongParquetField() {
        this(true);
    }

    public LongParquetField(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    protected void writeValue(FieldVector vector, int index, Object value) {
        ((BigIntVector) vector).setSafe(index, (Long) value);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(64, true);
    }

    @Override
    public FieldType getFieldType() {
        return nullable ? FieldType.nullable(getArrowType()) : FieldType.notNullable(getArrowType());
    }
}
