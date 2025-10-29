/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.core.data.number;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling single-precision floating-point data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch float fields and Apache Arrow
 * single-precision floating-point vectors for columnar storage in Parquet format. Float values are stored
 * using Apache Arrow's {@link Float4Vector}, which provides efficient 32-bit IEEE 754 single-precision
 * floating-point storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code float} number field mapping and
 * supports the full range of IEEE 754 single-precision floating-point values.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * FloatParquetField floatField = new FloatParquetField();
 * ArrowType arrowType = floatField.getArrowType(); // Returns single-precision floating-point type
 * FieldType fieldType = floatField.getFieldType(); // Returns non-nullable float field type
 * }</pre>
 *
 * @see ParquetField
 * @see Float4Vector
 * @see ArrowType.FloatingPoint
 * @since 1.0
 */
public class FloatParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        Float4Vector float4Vector = (Float4Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        float4Vector.setSafe(rowCount, (Float) parseValue);
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
