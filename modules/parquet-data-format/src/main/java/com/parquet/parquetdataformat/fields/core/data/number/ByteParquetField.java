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
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling 8-bit signed byte integer data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch byte fields and Apache Arrow
 * 8-bit signed integer vectors for columnar storage in Parquet format. Byte values are stored
 * using Apache Arrow's {@link TinyIntVector}, which provides efficient fixed-width 8-bit integer storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code byte} number field mapping and
 * supports the full range of 8-bit signed integer values.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * ByteParquetField byteField = new ByteParquetField();
 * ArrowType arrowType = byteField.getArrowType(); // Returns 8-bit signed integer type
 * FieldType fieldType = byteField.getFieldType(); // Returns non-nullable byte field type
 * }</pre>
 *
 * @see ParquetField
 * @see TinyIntVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class ByteParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        TinyIntVector tinyIntVector = (TinyIntVector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        tinyIntVector.setSafe(rowCount, (Byte) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(8, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}
