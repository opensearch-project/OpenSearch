/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field for 8-bit signed byte values using {@link TinyIntVector}.
 */
public class ByteParquetField extends NumericParquetField {

    /** Creates a new ByteParquetField. */
    public ByteParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, FieldVector vector, int rowIndex, Object parseValue) {
        ((TinyIntVector) vector).setSafe(rowIndex, ((Number) parseValue).byteValue());
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(8, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
