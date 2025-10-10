/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.number;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling 64-bit unsigned long integer data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch unsigned_long fields and Apache Arrow
 * 64-bit unsigned integer vectors for columnar storage in Parquet format. Unsigned long values are stored
 * using Apache Arrow's {@link UInt8Vector}, which provides efficient fixed-width 64-bit unsigned integer storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code unsigned_long} number field mapping and
 * supports the full range of 64-bit unsigned integer values. The implementation includes proper
 * null handling, setting explicit null markers when null values are encountered.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * UnsignedLongParquetField unsignedLongField = new UnsignedLongParquetField();
 * ArrowType arrowType = unsignedLongField.getArrowType(); // Returns 64-bit unsigned integer type
 * FieldType fieldType = unsignedLongField.getFieldType(); // Returns non-nullable unsigned long field type
 * }</pre>
 *
 * @see ParquetField
 * @see UInt8Vector
 * @see ArrowType.Int
 * @since 1.0
 */
public class UnsignedLongParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        UInt8Vector uInt8Vector = (UInt8Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        if (parseValue == null) {
            uInt8Vector.setNull(rowCount);
        } else {
            long longValue = ((Number) parseValue).longValue();
            uInt8Vector.setSafe(rowCount, longValue);
        }
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(64, false);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}
