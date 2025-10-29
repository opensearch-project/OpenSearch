/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.core.data;

import com.parquet.parquetdataformat.fields.ArrowFieldRegistry;
import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling boolean data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch boolean fields and Apache Arrow
 * boolean vectors for columnar storage in Parquet format. Boolean values are stored using
 * Apache Arrow's {@link BitVector}, which provides efficient bit-level storage for boolean data.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code boolean} field mapping and is
 * automatically registered in the {@link ArrowFieldRegistry} for use during document processing.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * BooleanParquetField boolField = new BooleanParquetField();
 * ArrowType arrowType = boolField.getArrowType(); // Returns ArrowType.Bool
 * FieldType fieldType = boolField.getFieldType(); // Returns non-nullable boolean field type
 * }</pre>
 *
 * @see ParquetField
 * @see BitVector
 * @see ArrowType.Bool
 * @since 1.0
 */
public class BooleanParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        BitVector bitVector = (BitVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        bitVector.setSafe(rowIndex, (Boolean) parseValue ? 1 : 0);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Bool();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
