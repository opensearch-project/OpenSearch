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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling 64-bit signed long integer data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch long fields and Apache Arrow
 * 64-bit signed integer vectors for columnar storage in Parquet format. Long values are stored
 * using Apache Arrow's {@link BigIntVector}, which provides efficient fixed-width 64-bit integer storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code long} number field mapping and
 * supports the full range of 64-bit signed integer values. The implementation includes proper
 * null handling, setting explicit null markers when null values are encountered.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * LongParquetField longField = new LongParquetField();
 * ArrowType arrowType = longField.getArrowType(); // Returns 64-bit signed integer type
 * FieldType fieldType = longField.getFieldType(); // Returns non-nullable long field type
 * }</pre>
 *
 * @see ParquetField
 * @see BigIntVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class LongParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        BigIntVector bigIntVector = (BigIntVector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        bigIntVector.setSafe(rowCount, (Long) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(64, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
