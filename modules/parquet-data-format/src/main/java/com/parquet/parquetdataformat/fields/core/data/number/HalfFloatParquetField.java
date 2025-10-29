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
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling half-precision (16-bit) floating-point data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch half_float fields and Apache Arrow
 * half-precision floating-point vectors for columnar storage in Parquet format. Half-float values are stored
 * using Apache Arrow's {@link Float2Vector}, which provides efficient 16-bit IEEE 754 half-precision
 * floating-point storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code half_float} number field mapping and
 * supports IEEE 754 half-precision floating-point values.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * HalfFloatParquetField halfFloatField = new HalfFloatParquetField();
 * ArrowType arrowType = halfFloatField.getArrowType(); // Returns half-precision floating-point type
 * FieldType fieldType = halfFloatField.getFieldType(); // Returns non-nullable half-float field type
 * }</pre>
 *
 * @see ParquetField
 * @see Float2Vector
 * @see ArrowType.FloatingPoint
 * @since 1.0
 */
public class HalfFloatParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        Float2Vector float2Vector = (Float2Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        float2Vector.setSafe(rowCount, (Short) parseValue);
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
