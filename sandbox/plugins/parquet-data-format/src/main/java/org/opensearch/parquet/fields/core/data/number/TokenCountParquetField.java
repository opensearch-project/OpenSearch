/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Parquet field for token count values stored as 32-bit integers using {@link IntVector}.
 */
public class TokenCountParquetField extends NumericParquetField {

    /** Creates a new TokenCountParquetField. */
    public TokenCountParquetField() {}

    @Override
    protected void writeValue(FieldVector vector, int index, Object value) {
        ((IntVector) vector).setSafe(index, (Integer) value);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(32, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
