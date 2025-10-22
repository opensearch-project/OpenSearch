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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling double-precision floating-point data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch double fields and Apache Arrow
 * double-precision floating-point vectors for columnar storage in Parquet format. Double values are stored
 * using Apache Arrow's {@link Float8Vector}, which provides efficient 64-bit IEEE 754 double-precision
 * floating-point storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code double} number field mapping and
 * supports the full range of IEEE 754 double-precision floating-point values.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * DoubleParquetField doubleField = new DoubleParquetField();
 * ArrowType arrowType = doubleField.getArrowType(); // Returns double-precision floating-point type
 * FieldType fieldType = doubleField.getFieldType(); // Returns non-nullable double field type
 * }</pre>
 *
 * @see ParquetField
 * @see Float8Vector
 * @see ArrowType.FloatingPoint
 * @since 1.0
 */
public class DoubleParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        Float8Vector float8Vector = (Float8Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        float8Vector.setSafe(rowCount, (Double) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}
