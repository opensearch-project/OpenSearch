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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling 32-bit signed integer data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch integer fields and Apache Arrow
 * 32-bit signed integer vectors for columnar storage in Parquet format. Integer values are stored
 * using Apache Arrow's {@link IntVector}, which provides efficient fixed-width integer storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code integer} number field mapping and
 * supports the full range of 32-bit signed integer values.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * IntegerParquetField intField = new IntegerParquetField();
 * ArrowType arrowType = intField.getArrowType(); // Returns 32-bit signed integer type
 * FieldType fieldType = intField.getFieldType(); // Returns non-nullable integer field type
 * }</pre>
 *
 * @see ParquetField
 * @see IntVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class IntegerParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        IntVector intVector = (IntVector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        intVector.setSafe(rowCount, (Integer) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(32, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}
