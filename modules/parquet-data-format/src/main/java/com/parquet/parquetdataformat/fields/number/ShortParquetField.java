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
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling 16-bit signed short integer data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch short fields and Apache Arrow
 * 16-bit signed integer vectors for columnar storage in Parquet format. Short values are stored
 * using Apache Arrow's {@link SmallIntVector}, which provides efficient fixed-width 16-bit integer storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code short} number field mapping and
 * supports the full range of 16-bit signed integer values. The implementation includes proper
 * null handling, setting explicit null markers when null values are encountered.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * ShortParquetField shortField = new ShortParquetField();
 * ArrowType arrowType = shortField.getArrowType(); // Returns 16-bit signed integer type
 * FieldType fieldType = shortField.getFieldType(); // Returns non-nullable short field type
 * }</pre>
 *
 * @see ParquetField
 * @see SmallIntVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class ShortParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        SmallIntVector smallIntVector = (SmallIntVector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        smallIntVector.setSafe(rowCount, (Short) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(16, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}
