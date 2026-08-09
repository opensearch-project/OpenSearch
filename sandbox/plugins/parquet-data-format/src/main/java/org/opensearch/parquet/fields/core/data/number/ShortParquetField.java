/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field for 16-bit signed short values using {@link SmallIntVector}.
 */
public class ShortParquetField extends NumericParquetField {

    /** Creates a new ShortParquetField. */
    public ShortParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, FieldVector vector, int rowIndex, Object parseValue) {
        ((SmallIntVector) vector).setSafe(rowIndex, (Short) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(16, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
